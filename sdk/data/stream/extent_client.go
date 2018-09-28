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
	"runtime"
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) (uint64, uint64, []proto.ExtentKey, error)

var (
	gDataWrapper     *wrapper.Wrapper
	writeRequestPool *sync.Pool
	flushRequestPool *sync.Pool
	closeRequestPool *sync.Pool
)

type ExtentClient struct {
	streamers       map[uint64]*Streamer
	streamerLock    sync.Mutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	gDataWrapper, err = wrapper.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, errors.Annotatef(err, "Init dp wrapper failed!")
	}
	client.streamers = make(map[uint64]*Streamer)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	closeRequestPool = &sync.Pool{New: func() interface{} {
		return &CloseRequest{}
	}}
	return
}

//TODO: delay stream writer init in GetStreamWriter
func (client *ExtentClient) OpenStream(inode uint64) error {
	client.streamerLock.Lock()
	stream, ok := client.streamers[inode]
	if !ok {
		stream = NewStreamer(client, inode)
	}

	stream.refcnt++

	if stream.writer != nil {
		client.streamerLock.Unlock()
		return nil
	}
	stream.writer = NewStreamWriter(stream, inode)
	client.streamers[inode] = stream
	client.streamerLock.Unlock()

	extents := NewExtentCache()
	err := extents.Refresh(inode, client.getExtents)

	client.streamerLock.Lock()
	if err != nil {
		stream.refcnt--
		if stream.refcnt == 0 {
			delete(client.streamers, inode)
		}
	} else {
		stream.extents = extents
	}
	client.streamerLock.Unlock()

	if err != nil {
		return err
	}
	return nil
}

func (client *ExtentClient) CloseStream(inode uint64) error {
	client.streamerLock.Lock()
	stream, ok := client.streamers[inode]
	if !ok {
		client.streamerLock.Unlock()
		return nil
	}
	writer := stream.writer
	client.streamerLock.Unlock()

	if writer != nil {
		err := writer.Close()
		if err != nil {
			return err
		}
	}

	client.streamerLock.Lock()
	stream.refcnt--
	if stream.refcnt <= 0 {
		delete(client.streamers, inode)
	}
	client.streamerLock.Unlock()
	return nil
}

func (client *ExtentClient) RefreshExtentsCache(inode uint64) error {
	stream := client.getStreamer(inode)
	if stream == nil {
		return nil
	}
	return stream.GetExtents()
}

func (client *ExtentClient) GetFileSize(inode uint64) (size int, gen uint64) {
	stream := client.getStreamer(inode)
	if stream == nil {
		return
	}
	return stream.extents.Size()
}

func (client *ExtentClient) Write(inode uint64, offset int, data []byte) (write int, err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		return 0, fmt.Errorf("Prefix(%v) cannot init write stream", prefix)
	}

	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.kernelOffset = offset
	request.size = len(data)
	request.done = make(chan struct{}, 1)
	stream.requestCh <- request
	<-request.done
	err = request.err
	write = request.canWrite
	if err != nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
		ump.Alarm(gDataWrapper.UmpWarningKey(), err.Error())
	}
	writeRequestPool.Put(request)
	return
}

func (client *ExtentClient) Flush(inode uint64) error {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		return nil
	}
	return stream.Flush()
}

func (client *ExtentClient) Read(inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}

	stream := client.getStreamer(inode)
	if stream == nil {
		return
	}

	writer := client.getStreamWriter(inode)
	if writer != nil {
		if err = writer.Flush(); err != nil {
			return
		}
	}

	read, err = stream.read(data, offset, size)
	return
}

func (client *ExtentClient) getStreamer(inode uint64) *Streamer {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	stream, ok := client.streamers[inode]
	if !ok {
		return nil
	}
	return stream
}

func (client *ExtentClient) getStreamWriter(inode uint64) *StreamWriter {
	client.streamerLock.Lock()
	defer client.streamerLock.Unlock()
	stream, ok := client.streamers[inode]
	if !ok {
		return nil
	}
	return stream.writer
}
