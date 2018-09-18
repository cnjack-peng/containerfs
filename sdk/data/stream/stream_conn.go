package stream

import (
	"fmt"
	"net"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/util/log"
)

const (
	StreamSendMaxRetry      = 100
	StreamSendSleepInterval = 100 * time.Millisecond
)

type StreamConn struct {
	partition uint32
	currAddr  string
	hosts     []string
	conn      *net.TCPConn
}

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

func (sc *StreamConn) GetConn(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		log.LogWarnf("sendToPartition: dial to (%v) err(%v)", addr, err)
		return err
	}
	sc.currAddr = addr
	sc.conn = conn.(*net.TCPConn)
	sc.conn.SetKeepAlive(true)
	sc.conn.SetNoDelay(true)
	return nil
}

func (sc *StreamConn) PutConn() {
	conn := sc.conn
	if conn != nil {
		sc.conn = nil
		sc.currAddr = ""
		conn.Close()
	}
}

func (sc *StreamConn) sendToPartition(req *Packet) (resp *Packet, err error) {
	if sc.conn != nil {
		resp, err = sc.sendToConn(req)
		if err == nil {
			return
		}
	}
	sc.PutConn()
	for _, addr := range sc.hosts {
		err = sc.GetConn(addr)
		if err != nil {
			continue
		}
		resp, err = sc.sendToConn(req)
		if err == nil {
			return
		}
		sc.PutConn()
	}
	return nil, errors.New(fmt.Sprintf("sendToPatition Failed: sc(%v)", sc))
}

func (sc *StreamConn) sendToConn(req *Packet) (resp *Packet, err error) {
	for i := 0; i < StreamSendMaxRetry; i++ {
		err = req.writeTo(sc.conn)
		if err != nil {
			err = errors.Annotatef(err, "sendToConn: failed to write to connect sc(%v)", sc)
			break
		}

		resp = new(Packet)
		err = resp.ReadFromConn(sc.conn, proto.ReadDeadlineTime)
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
