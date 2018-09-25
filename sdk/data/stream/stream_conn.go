package stream

import (
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/pool"
)

const (
	StreamSendMaxRetry      = 100
	StreamSendSleepInterval = 100 * time.Millisecond
)

type GetReplyFunc func(conn *net.TCPConn) (err error, again bool)

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
	return fmt.Sprintf("Partition(%v) CurrentAddr(%v) Hosts(%v)", sc.partition, sc.currAddr, sc.hosts)
}

func (sc *StreamConn) Send(req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = sc.sendToPartition(req, getReply)
		if err == nil {
			return
		}
		log.LogWarnf("Send: err(%v)", err)
	}
	return errors.New(fmt.Sprintf("Send: retried %v times and still failed, sc(%v)", StreamSendMaxRetry, sc))
}

func (sc *StreamConn) sendToPartition(req *Packet, getReply GetReplyFunc) (err error) {
	conn, err := StreamConnPool.Get(sc.currAddr)
	if err == nil {
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.Put(conn, false)
			return
		}
		log.LogWarnf("sendToPartition: curr addr failed, sc(%v) req(%v) err(%v)", sc, req, err)
		StreamConnPool.Put(conn, true)
	}

	for _, addr := range sc.hosts {
		conn, err = StreamConnPool.Get(addr)
		if err != nil {
			log.LogWarnf("sendToPartition: failed to get connection to (%v) sc(%v) req(%v) err(%v)", addr, sc, req, err)
			continue
		}
		sc.currAddr = addr
		err = sc.sendToConn(conn, req, getReply)
		if err == nil {
			StreamConnPool.Put(conn, false)
			return
		}
		StreamConnPool.Put(conn, true)
	}
	return errors.New(fmt.Sprintf("sendToPatition Failed: sc(%v)", sc))
}

func (sc *StreamConn) sendToConn(conn *net.TCPConn, req *Packet, getReply GetReplyFunc) (err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = req.WriteToConn(conn)
		if err != nil {
			err = errors.Annotatef(err, "sendToConn: failed to write to connect sc(%v)", sc)
			break
		}

		err, again := getReply(conn)
		if !again {
			if err != nil {
				err = errors.Annotatef(err, "sc(%v)", sc)
			}
			break
		}

		time.Sleep(StreamSendSleepInterval)
	}

	if err != nil {
		log.LogWarn(err)
	}

	log.LogDebugf("sendToPartition: send to sc(%v) successsful, req(%v)", sc, req)
	return
}
