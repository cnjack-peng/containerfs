// Copyright 2018 The Containerfs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stream

import (
	"fmt"
	//"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	//"github.com/tiglabs/containerfs/util"
	//"github.com/tiglabs/containerfs/util/log"
	//"io"
	"math/rand"
	"sync"
	//"sync/atomic"
)

type ReadRequest struct {
	data       []byte
	offset     int
	size       int
	canRead    int
	err        error
	cond       *sync.Cond
	isResponse bool
}

type StreamReader struct {
	client   *ExtentClient
	extents  *ExtentCache
	inode    uint64
	readers  []*ExtentReader
	fileSize uint64
}

func (stream *StreamReader) String() (m string) {
	return fmt.Sprintf("inode(%v) fileSize(%v) extents(%v) ",
		stream.inode, stream.fileSize, stream.extents.List())
}

func NewStreamReader(client *ExtentClient, inode uint64) (*StreamReader, error) {
	stream := new(StreamReader)
	stream.client = client
	stream.extents = NewExtentCache()
	stream.inode = inode
	err := stream.GetExtents()
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (stream *StreamReader) GetExtents() error {
	err := stream.extents.Refresh(stream.inode, stream.client.getExtents)
	if err != nil {
		return err
	}
	stream.fileSize = stream.extents.Size()
	return nil
}

//TODO: use memory pool
func (stream *StreamReader) GetExtentReader(ek *proto.ExtentKey) (*ExtentReader, error) {
	partition, err := gDataWrapper.GetDataPartition(ek.PartitionId)
	if err != nil {
		return nil, err
	}
	reader := new(ExtentReader)
	reader.inode = stream.inode
	reader.key = *ek
	reader.dp = partition
	reader.readerIndex = uint32(rand.Intn(int(reader.dp.ReplicaNum)))
	return reader, nil
}

func (stream *StreamReader) read(data []byte, offset int, size int) (total int, err error) {
	var readBytes int
	err = stream.GetExtents()
	if err != nil {
		return
	}
	requests := stream.extents.PrepareRequest(offset, size, data)
	for _, req := range requests {
		if req.ExtentKey == nil {
			// Reading a hole, just fill zero
			for i, _ := range req.Data {
				req.Data[i] = 0
			}
			total += req.Size
		} else {
			reader, err := stream.GetExtentReader(req.ExtentKey)
			if err != nil {
				break
			}
			readBytes, err = reader.Read(req)
			total += readBytes
			if err != nil {
				break
			}
		}
	}
	return
}

//func (stream *StreamReader) initCheck(offset, size int) (canread int, err error) {
//	if size > util.ExtentSize {
//		return 0, io.EOF
//	}
//	if offset+size <= int(stream.fileSize) {
//		return size, nil
//	}
//	newStreamKey := proto.NewStreamKey(stream.inode)
//	_, newStreamKey.Extents, err = stream.client.getExtents(stream.inode)
//
//	if err == nil {
//		err = stream.updateLocalReader(newStreamKey)
//	}
//	if err != nil {
//		return 0, err
//	}
//
//	if offset > int(stream.fileSize) {
//		return 0, io.EOF
//	}
//	if offset+size > int(stream.fileSize) {
//		return int(stream.fileSize) - offset, io.EOF
//	}
//
//	return size, nil
//}

//func (stream *StreamReader) updateLocalReader(newStreamKey *proto.StreamKey) (err error) {
//	var (
//		newOffSet int
//		r         *ExtentReader
//	)
//	readers := make([]*ExtentReader, 0)
//	oldReaders := stream.readers
//	oldReaderCnt := len(stream.readers)
//	for index, key := range newStreamKey.Extents {
//		if index < oldReaderCnt-1 {
//			newOffSet += int(key.Size)
//			continue
//		} else if index == oldReaderCnt-1 {
//			oldReaders[index].updateKey(key)
//			newOffSet += int(key.Size)
//			continue
//		} else if index > oldReaderCnt-1 {
//			if r, err = NewExtentReader(stream.inode, newOffSet, key); err != nil {
//				return errors.Annotatef(err, "NewStreamReader inode(%v) key(%v) "+
//					"dp not found error", stream.inode, key)
//			}
//			readers = append(readers, r)
//			newOffSet += int(key.Size)
//			continue
//		}
//	}
//	stream.fileSize = newStreamKey.Size()
//	stream.extents = newStreamKey
//	stream.readers = append(stream.readers, readers...)
//
//	return nil
//}

//func (stream *StreamReader) read(data []byte, offset int, size int) (canRead int, err error) {
//	var keyCanRead int
//	keyCanRead, err = stream.initCheck(offset, size)
//	if keyCanRead <= 0 || (err != nil && err != io.EOF) {
//		return
//	}
//	readers, readerOffset, readerSize := stream.GetReader(offset, size)
//	for index := 0; index < len(readers); index++ {
//		r := readers[index]
//		err = r.read(data[canRead:canRead+readerSize[index]], readerOffset[index], readerSize[index], offset, size)
//		if err != nil {
//			err = errors.Annotatef(err, "UserRequest{inode(%v) FileSize(%v) "+
//				"Offset(%v) Size(%v)} readers{ (%v) Offset(%v) Size(%v) occous error}",
//				stream.inode, stream.fileSize, offset, size, r.toString(), readerOffset[index],
//				readerSize[index])
//			log.LogErrorf(err.Error())
//			return canRead, err
//		}
//		canRead += readerSize[index]
//	}
//	if canRead < size && err == nil {
//		return canRead, io.EOF
//	}
//
//	return
//}

//func (stream *StreamReader) predictExtent(offset, size int) (startIndex int) {
//	startIndex = offset >> 27
//	r := stream.readers[startIndex]
//	if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
//		return startIndex
//	}
//	index := startIndex - 1
//	if index >= 0 {
//		r = stream.readers[index]
//		if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
//			return index
//		}
//
//	}
//	index = startIndex - 2
//	if index >= 0 {
//		r = stream.readers[index]
//		if int(atomic.LoadUint64(&r.startInodeOffset)) <= offset && int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
//			return index
//		}
//	}
//
//	return 0
//}

//func (stream *StreamReader) GetReader(offset, size int) (readers []*ExtentReader, readersOffsets []int, readersSize []int) {
//	readers = make([]*ExtentReader, 0)
//	readersOffsets = make([]int, 0)
//	readersSize = make([]int, 0)
//	startIndex := stream.predictExtent(offset, size)
//	for _, r := range stream.readers[startIndex:] {
//		if size <= 0 {
//			break
//		}
//		if int(atomic.LoadUint64(&r.startInodeOffset)) > offset || int(atomic.LoadUint64(&r.endInodeOffset)) <= offset {
//			continue
//		}
//		var (
//			currReaderSize   int
//			currReaderOffset int
//		)
//		if int(atomic.LoadUint64(&r.endInodeOffset)) >= offset+size {
//			currReaderOffset = offset - int(atomic.LoadUint64(&r.startInodeOffset))
//			currReaderSize = size
//			offset += currReaderSize
//			size -= currReaderSize
//		} else {
//			currReaderOffset = offset - int(atomic.LoadUint64(&r.startInodeOffset))
//			currReaderSize = int(r.key.Size) - currReaderOffset
//			offset = int(atomic.LoadUint64(&r.endInodeOffset))
//			size = size - currReaderSize
//		}
//		if currReaderSize == 0 {
//			continue
//		}
//		readersSize = append(readersSize, currReaderSize)
//		readersOffsets = append(readersOffsets, currReaderOffset)
//		readers = append(readers, r)
//		if size <= 0 {
//			break
//		}
//	}
//
//	return
//}
