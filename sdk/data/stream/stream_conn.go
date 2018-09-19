package stream

import (
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/pool"
)

const (
	StreamSendMaxRetry      = 100
	StreamSendSleepInterval = 100 * time.Millisecond
)

type StreamConn struct {
	partition uint32
	currAddr  string
	hosts     []string
}

var (
	StreamConnPool = pool.NewConnPool()
)

func NewStreamConn(dp *wrapper.DataPartition) *StreamConn {
	return &StreamConn{
		partition: dp.PartitionID,
		currAddr:  dp.LeaderAddr,
		hosts:     dp.Hosts,
	}
}

func (sc *StreamConn) String() string {
	return fmt.Sprintf("Partition(%v) Leader(%v) Hosts(%v)", sc.partition, sc.currAddr, sc.hosts)
}

func (sc *StreamConn) Send(req *Packet) (resp *Packet, err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		resp, err = sc.sendToPartition(req)
		if err == nil {
			return
		}
		log.LogWarnf("Send: retry in %v", StreamSendSleepInterval)
		time.Sleep(StreamSendSleepInterval)
	}
	return nil, errors.New(fmt.Sprintf("Send: retried %v times and still failed, sc(%v)", StreamSendMaxRetry, sc))
}

func (sc *StreamConn) sendToPartition(req *Packet) (resp *Packet, err error) {
	conn, err := StreamConnPool.Get(sc.currAddr)
	if err == nil {
		resp, err = sc.sendToConn(conn, req)
		if err == nil {
			StreamConnPool.Put(conn, false)
			return
		}
		StreamConnPool.Put(conn, true)
	}

	for _, addr := range sc.hosts {
		conn, err = StreamConnPool.Get(addr)
		if err != nil {
			continue
		}
		resp, err = sc.sendToConn(conn, req)
		if err == nil {
			sc.currAddr = addr
			StreamConnPool.Put(conn, false)
			return
		}
		StreamConnPool.Put(conn, true)
	}
	return nil, errors.New(fmt.Sprintf("sendToPatition Failed: sc(%v)", sc))
}

func (sc *StreamConn) sendToConn(conn *net.TCPConn, req *Packet) (resp *Packet, err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = req.writeTo(conn)
		if err != nil {
			err = errors.Annotatef(err, "sendToConn: failed to write to connect sc(%v)", sc)
			break
		}

		resp = new(Packet)
		err = resp.ReadFromConn(conn, proto.ReadDeadlineTime)
		if err != nil {
			err = errors.Annotatef(err, "sendToConn: failed to read from connect sc(%v)", sc)
			break
		}

		if resp.ResultCode == proto.OpNotLeaderErr {
			err = errors.New(fmt.Sprintf("sendToConn: Not leader sc(%v)", sc))
			break
		} else if resp.ResultCode != proto.OpAgain {
			break
		}

		time.Sleep(StreamSendSleepInterval)
	}

	if err != nil {
		log.LogWarn(err)
		return nil, err
	}
	return resp, nil
}
