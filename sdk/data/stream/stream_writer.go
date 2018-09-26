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
	"hash/crc32"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	MaxSelectDataPartionForWrite = 32
	MaxStreamInitRetry           = 3
	HasClosed                    = -1
)

type WriteRequest struct {
	data         []byte
	size         int
	canWrite     int
	err          error
	kernelOffset int
	cutSize      int
	done         chan struct{}
}

type FlushRequest struct {
	err  error
	done chan struct{}
}

type CloseRequest struct {
	err  error
	done chan struct{}
}

type StreamWriter struct {
	currentWriter           *ExtentWriter //current ExtentWriter
	stream                  *Streamer
	errCount                int    //error count
	currentPartitionId      uint32 //current PartitionId
	currentExtentId         uint64 //current FileId
	Inode                   uint64 //inode
	excludePartition        []uint32
	appendExtentKey         AppendExtentKeyFunc
	requestCh               chan interface{}
	exitCh                  chan bool
	hasUpdateKey            map[string]int
	hasWriteSize            uint64
	hasClosed               int32
	hasUpdateToMetaNodeSize uint64
}

func NewStreamWriter(stream *Streamer, inode uint64) *StreamWriter {
	sw := new(StreamWriter)
	sw.stream = stream
	sw.Inode = inode
	sw.requestCh = make(chan interface{}, 1000)
	sw.exitCh = make(chan bool, 10)
	sw.excludePartition = make([]uint32, 0)
	sw.hasUpdateKey = make(map[string]int, 0)
	go sw.server()
	return sw
}

func (sw *StreamWriter) String() (m string) {
	currentWriterMsg := ""
	if sw.currentWriter != nil {
		currentWriterMsg = sw.currentWriter.String()
	}
	return fmt.Sprintf("inode(%v) currentDataPartion(%v) currentExtentId(%v)"+" errCount(%v)", sw.Inode, sw.currentPartitionId, currentWriterMsg, sw.errCount)
}

func (sw *StreamWriter) Flush() error {
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	sw.requestCh <- request
	<-request.done
	err := request.err
	flushRequestPool.Put(request)
	return err
}

func (sw *StreamWriter) Close() error {
	err := sw.Flush()
	if err != nil {
		return err
	}
	request := closeRequestPool.Get().(*CloseRequest)
	request.done = make(chan struct{}, 1)
	sw.requestCh <- request
	<-request.done
	closeRequestPool.Put(request)
	return nil
}

func (sw *StreamWriter) toStringWithWriter(writer *ExtentWriter) (m string) {
	return fmt.Sprintf("inode(%v) currentDataPartion(%v) currentExtentId(%v)"+
		" errCount(%v)", sw.Inode, sw.currentPartitionId, writer, sw.errCount)
}

func (sw *StreamWriter) needFlush(fileOffset uint64) bool {
	return sw.currentWriter != nil &&
		(sw.currentWriter.fileOffset+uint64(sw.currentWriter.offset) != fileOffset ||
			sw.currentWriter.isFullExtent())
}

//writer init,alloc a extent ,select dp and extent
func (sw *StreamWriter) init(fileOffset uint64) (err error) {
	if sw.needFlush(fileOffset) {
		if err = sw.flushCurrExtentWriter(true); err != nil {
			return errors.Annotatef(err, "WriteInit")
		}
	}

	if sw.currentWriter != nil {
		return
	}
	var writer *ExtentWriter
	writer, err = sw.allocateNewExtentWriter(fileOffset)
	if err != nil {
		err = errors.Annotatef(err, "WriteInit AllocNewExtentFailed")
		return err
	}

	sw.setCurrentWriter(writer)
	return
}

func (sw *StreamWriter) server() {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		select {
		case request := <-sw.requestCh:
			sw.handleRequest(request)
		case <-sw.exitCh:
			sw.flushCurrExtentWriter(true)
			return
		case <-t.C:
			atomic.StoreUint64(&sw.hasUpdateToMetaNodeSize, uint64(sw.updateToMetaNodeSize()))
			filesize, gen := sw.stream.extents.Size()
			log.LogDebugf("inode(%v) update to metanode filesize To(%v) user has Write to (%v) gen(%v)", sw.Inode, sw.getHasUpdateToMetaNodeSize(), filesize, gen)
			if sw.getCurrentWriter() == nil {
				continue
			}
			sw.flushCurrExtentWriter(false)
		}
	}
}

func (sw *StreamWriter) handleRequest(request interface{}) {
	switch request := request.(type) {
	case *WriteRequest:
		request.canWrite, request.err = sw.write(request.data, request.kernelOffset, request.size)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = sw.flushCurrExtentWriter(false)
		request.done <- struct{}{}
	case *CloseRequest:
		request.err = sw.flushCurrExtentWriter(true)
		if request.err == nil {
			request.err = sw.close()
		}
		request.done <- struct{}{}
		sw.exit()
	default:
	}
}

func (sw *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
	log.LogDebugf("StreamWriter write: ino(%v) offset(%v) size(%v)", sw.Inode, offset, size)

	requests := sw.stream.extents.PrepareRequest(offset, size, data)
	log.LogDebugf("StreamWriter write: prepared requests(%v)", requests)
	for _, req := range requests {
		var writeSize int
		if req.ExtentKey != nil {
			writeSize, err = sw.doRewrite(req)
		} else {
			writeSize, err = sw.doWrite(req.Data, req.FileOffset, req.Size)
		}
		if err != nil {
			log.LogErrorf("StreamWriter write: err(%v)", err)
			break
		}
		total += writeSize
	}
	if filesize, _ := sw.stream.extents.Size(); offset+total > filesize {
		sw.stream.extents.SetSize(uint64(offset + total))
	}
	log.LogDebugf("StreamWriter write: done total(%v) err(%v)", total, err)
	return
}

func (sw *StreamWriter) doRewrite(req *ExtentRequest) (total int, err error) {
	var dp *wrapper.DataPartition
	offset := req.FileOffset
	size := req.Size
	ekOffset := int(req.ExtentKey.FileOffset)

	if sw.currentWriter != nil {
		currPacket := sw.currentWriter.currentPacket
		kernelOffset := currPacket.kernelOffset
		packSize := int(currPacket.Size)
		if kernelOffset < offset && offset < kernelOffset+packSize {
			fileEnd := util.Min(kernelOffset+packSize, offset+size)
			copy(currPacket.Data[offset-kernelOffset:fileEnd-kernelOffset], req.Data[:fileEnd-offset])
			total += (fileEnd - offset)
			log.LogDebugf("doRewrite: rewrite current writer from (%v) to (%v)", offset, fileEnd)
		}
	}

	if total >= size {
		return
	}

	if dp, err = gDataWrapper.GetDataPartition(req.ExtentKey.PartitionId); err != nil {
		errors.Annotatef(err, "doRewrite: failed to get datapartition, ek(%v)", req.ExtentKey)
		return
	}

	sc := NewStreamConn(dp)

	for total < size {
		reqPacket := NewWritePacket(dp, req.ExtentKey.ExtentId, offset-ekOffset+total, offset, true)
		packSize := util.Min(size-total, util.BlockSize)
		copy(reqPacket.Data[:packSize], req.Data[total:total+packSize])
		reqPacket.Size = uint32(packSize)
		reqPacket.Crc = crc32.ChecksumIEEE(reqPacket.Data[:packSize])

		replyPacket := new(Packet)
		err = sc.Send(reqPacket, func(conn *net.TCPConn) (error, bool) {
			e := replyPacket.ReadFromConn(conn, proto.ReadDeadlineTime)
			if e != nil {
				return errors.Annotatef(e, "Stream Writer doRewrite: failed to read from connect"), false
			}

			if replyPacket.ResultCode == proto.OpAgain {
				return nil, true
			}

			if replyPacket.ResultCode == proto.OpNotLeaderErr {
				e = NotLeaderError
			}
			return e, false
		})

		if err != nil || replyPacket.ResultCode != proto.OpOk {
			err = errors.Annotatef(err, "doRewrite failed: req(%v) reqPacket(%v) replyPacket(%v)", req, reqPacket, replyPacket)
			break
		}

		if !reqPacket.IsEqualWriteReply(replyPacket) || reqPacket.Crc != replyPacket.Crc {
			err = errors.New(fmt.Sprintf("doRewrite: is not the corresponding reply, req(%v) reqPacket(%v) replyPacket(%v)", req, reqPacket, replyPacket))
			break
		}

		total += packSize
	}

	return
}

func (sw *StreamWriter) doWrite(data []byte, offset, size int) (total int, err error) {
	var (
		write int
	)
	defer func() {
		if err == nil {
			total = size
			return
		}
		err = errors.Annotatef(err, "UserRequest{inode(%v) write "+
			"KernelOffset(%v) KernelSize(%v) hasWrite(%v)}  StreamWriter{ (%v) occous error}",
			sw.Inode, offset, size, total, sw)
		log.LogError(err.Error())
		log.LogError(errors.ErrorStack(err))
	}()

	var initRetry int = 0
	for total < size {
		if err = sw.init(uint64(offset + total)); err != nil {
			if initRetry++; initRetry > MaxStreamInitRetry {
				return total, err
			}
			continue
		}
		write, err = sw.currentWriter.write(data[total:size], offset, size-total)
		if err == nil {
			write = size - total
			total += write
			sw.currentWriter.offset += write
			continue
		}
		if strings.Contains(err.Error(), FullExtentErr.Error()) {
			continue
		}
		if err = sw.recoverExtent(); err != nil {
			return
		} else {
			write = size - total //recover success ,then write is allLength
		}
		total += write
		sw.currentWriter.offset += write
	}

	if sw.currentWriter != nil {
		ek := sw.currentWriter.toKey()
		ek.Size += uint32(total)
		sw.stream.extents.Append(&ek, false)
	}

	return total, err
}

func (sw *StreamWriter) close() (err error) {
	if sw.currentWriter != nil {
		err = sw.currentWriter.close()
	}
	return
}

func (sw *StreamWriter) flushCurrExtentWriter(close bool) (err error) {
	var status error
	defer func() {
		if err == nil || status == syscall.ENOENT {
			sw.errCount = 0
			err = nil
			return
		}
		sw.errCount++
		if sw.errCount < MaxSelectDataPartionForWrite {
			if err = sw.recoverExtent(); err == nil {
				err = sw.flushCurrExtentWriter(false)
			}
		}
	}()
	writer := sw.getCurrentWriter()
	if writer == nil {
		err = nil
		return nil
	}
	if err = writer.flush(); err != nil {
		err = errors.Annotatef(err, "writer(%v) Flush Failed", writer)
		return err
	}
	if err = sw.updateToMetaNode(); err != nil {
		err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
		return err
	}
	if close || writer.isFullExtent() {
		writer.close()
		writer.getConnect().Close()
		if err = sw.updateToMetaNode(); err != nil {
			err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
			return err
		}
		sw.setCurrentWriter(nil)
	}

	return err
}

func (sw *StreamWriter) updateToMetaNodeSize() (sumSize int) {
	return int(sw.hasUpdateToMetaNodeSize)
}

func (sw *StreamWriter) setCurrentWriter(writer *ExtentWriter) {
	sw.currentWriter = writer
}

func (sw *StreamWriter) getCurrentWriter() *ExtentWriter {
	return sw.currentWriter
}

func (sw *StreamWriter) updateToMetaNode() (err error) {
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if sw.currentWriter == nil {
			return
		}
		ek := sw.currentWriter.toKey() //first get currentExtent Key
		if ek.Size == 0 {
			return
		}

		updateKey := ek.GetExtentKey()
		lastUpdateExtentKeySize, ok := sw.hasUpdateKey[updateKey]
		if ok && lastUpdateExtentKeySize == int(ek.Size) {
			return nil
		}
		lastUpdateSize := 0
		if ok {
			lastUpdateSize = lastUpdateExtentKeySize
		}
		if lastUpdateSize == int(ek.Size) {
			return nil
		}
		ekey := ek
		sw.stream.extents.Append(&ekey, true)
		err = sw.stream.client.appendExtentKey(sw.Inode, ek) //put it to metanode
		if err == syscall.ENOENT {
			sw.exit()
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update extent(%v) to MetaNode Failed", ek.Size)
			log.LogErrorf("StreamWriter(%v) err(%v)", sw, err)
			continue
		}
		sw.addHasUpdateToMetaNodeSize(int(ek.Size) - lastUpdateSize)
		sw.hasUpdateKey[updateKey] = int(ek.Size)
		return
	}

	return err
}

func (sw *StreamWriter) writeRecoverPackets(writer *ExtentWriter, retryPackets []*Packet) (err error) {
	for _, p := range retryPackets {
		log.LogInfof("recover packet (%v) kernelOffset(%v) to extent(%v)",
			p.GetUniqueLogId(), p.kernelOffset, writer)
		_, err = writer.write(p.Data, p.kernelOffset, int(p.Size))
		if err != nil {
			err = errors.Annotatef(err, "pkg(%v) RecoverExtent write failed", p.GetUniqueLogId())
			log.LogErrorf("StreamWriter(%v) err(%v)", sw.toStringWithWriter(writer), err.Error())
			sw.excludePartition = append(sw.excludePartition, writer.dp.PartitionID)
			return err
		}
	}
	return
}

func (sw *StreamWriter) recoverExtent() (err error) {
	sw.excludePartition = append(sw.excludePartition, sw.currentWriter.dp.PartitionID) //exclude current PartionId
	sw.currentWriter.notifyExit()
	retryPackets := sw.currentWriter.getNeedRetrySendPackets() //get need retry recover packets
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if err = sw.updateToMetaNode(); err == nil {
			break
		}
	}
	var writer *ExtentWriter
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		err = nil
		if writer, err = sw.allocateNewExtentWriter(uint64(retryPackets[0].kernelOffset)); err != nil { //allocate new extent
			err = errors.Annotatef(err, "RecoverExtent Failed")
			log.LogErrorf("writer(%v) err(%v)", writer, err)
			continue
		}
		if err = sw.writeRecoverPackets(writer, retryPackets); err == nil {
			sw.excludePartition = make([]uint32, 0)
			sw.setCurrentWriter(writer)
			sw.updateToMetaNode()
			return err
		} else {
			writer.forbirdUpdateToMetanode()
			writer.notifyExit()
		}
	}

	return err

}

func (sw *StreamWriter) allocateNewExtentWriter(fileOffset uint64) (writer *ExtentWriter, err error) {
	var (
		dp       *wrapper.DataPartition
		extentId uint64
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if dp, err = gDataWrapper.GetWriteDataPartition(sw.excludePartition); err != nil {
			log.LogWarn(fmt.Sprintf("StreamWriter(%v) ActionAllocNewExtentWriter "+
				"failed on getWriteDataPartion,error(%v) execludeDataPartion(%v)", sw, err, sw.excludePartition))
			continue
		}
		if extentId, err = sw.createExtent(dp); err != nil {
			log.LogWarn(fmt.Sprintf("StreamWriter(%v)ActionAllocNewExtentWriter "+
				"create Extent,error(%v) execludeDataPartion(%v)", sw, err, sw.excludePartition))
			continue
		}
		if writer, err = NewExtentWriter(sw.Inode, dp, extentId, fileOffset); err != nil {
			log.LogWarn(fmt.Sprintf("StreamWriter(%v) ActionAllocNewExtentWriter "+
				"NewExtentWriter(%v),error(%v) execludeDataPartion(%v)", sw, extentId, err, sw.excludePartition))
			continue
		}
		break
	}
	if extentId <= 0 {
		log.LogErrorf(errors.Annotatef(err, "allocateNewExtentWriter").Error())
		return nil, errors.Annotatef(err, "allocateNewExtentWriter")
	}
	sw.currentPartitionId = dp.PartitionID
	sw.currentExtentId = extentId
	err = nil

	return writer, nil
}

func (sw *StreamWriter) createExtent(dp *wrapper.DataPartition) (extentId uint64, err error) {
	var (
		connect *net.TCPConn
	)
	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
	if err != nil {
		err = errors.Annotatef(err, " get connect from datapartionHosts(%v)", dp.Hosts[0])
		return 0, err
	}
	connect, _ = conn.(*net.TCPConn)
	connect.SetKeepAlive(true)
	connect.SetNoDelay(true)
	defer connect.Close()
	p := NewCreateExtentPacket(dp, sw.Inode)
	if err = p.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, "send CreateExtent(%v) to datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime*2); err != nil {
		err = errors.Annotatef(err, "receive CreateExtent(%v) failed datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.Annotatef(err, "receive CreateExtent(%v) failed datapartionHosts(%v) ", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	extentId = p.FileID
	if p.FileID <= 0 {
		err = errors.Annotatef(err, "illegal extentId(%v) from (%v) response",
			extentId, dp.Hosts[0])
		return

	}

	return extentId, nil
}

func (sw *StreamWriter) exit() {
	select {
	case sw.exitCh <- true:
	default:
	}
}

func (sw *StreamWriter) addHasUpdateToMetaNodeSize(writed int) {
	atomic.AddUint64(&sw.hasUpdateToMetaNodeSize, uint64(writed))
}

func (sw *StreamWriter) getHasUpdateToMetaNodeSize() uint64 {
	return atomic.LoadUint64(&sw.hasUpdateToMetaNodeSize)
}
