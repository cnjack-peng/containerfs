// Copyright 2018 The ChuBao Authors.
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

package datanode

import (
	"encoding/json"
	"sync/atomic"

	"encoding/binary"
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"io"
)

func (dp *dataPartition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	defer func() {
		dp.uploadApplyID(index)
		if err != nil {
			resp = proto.OpExistErr
			dp.ChangeStatus(proto.Unavaliable)
			if dp.applyErrMinId == 0 {
				dp.applyErrMinId = index  //record min apply id
			}
		} else {
			resp = proto.OpOk
		}
	}()
	msg := &RndWrtCmdItem{}
	if err = msg.rndWrtCmdUnmarshal(command); err != nil {
		return
	}

	opItem := &rndWrtOpItem{}
	switch msg.Op {
	case opRandomWrite:
		if opItem, err = rndWrtDataUnmarshal(msg.V); err != nil {
			return
		}
		err = dp.randomWriteStore(opItem)
	default:
		err = fmt.Errorf(fmt.Sprintf("Wrong random operate %v", msg.Op))
		return
	}
	return
}

func (dp *dataPartition) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func() {
		if err == nil {
			dp.uploadApplyID(index)
		}
	}()
	req := &proto.DataPartitionOfflineRequest{}
	if err = json.Unmarshal(confChange.Context, req); err != nil {
		return
	}
	// Change memory state
	var (
		updated bool
	)
	switch confChange.Type {
	case raftproto.ConfAddNode:
		updated, err = dp.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		updated, err = dp.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		updated, err = dp.confUpdateNode(req, index)
	}
	if err != nil {
		return
	}
	if updated {
		if err = dp.StoreMeta(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] err[%v].", err)
			return
		}
	}
	return
}

//iterator be reserved for future
func (dp *dataPartition) Snapshot() (raftproto.Snapshot, error) {
	applyID := dp.applyId
	snapIterator := NewItemIterator(applyID)
	return snapIterator, nil
}

func (dp *dataPartition) ApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator) (err error) {
	var (
		data       []byte
		index      int
		appIndexID uint64
	)
	defer func() {
		if err == io.EOF {
			dp.applyId = appIndexID
			err = nil
			log.LogDebugf("[ApplySnapshot] successful applyId[%v].", dp.applyId)
			return
		}
		log.LogErrorf("[ApplySnapshot]: %s", err.Error())
	}()
	for {
		data, err = iterator.Next()
		if err != nil {
			return
		}
		if index == 0 {
			appIndexID = binary.BigEndian.Uint64(data)
			index++
			continue
		}
	}
}

func (dp *dataPartition) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
}

func (dp *dataPartition) HandleLeaderChange(leader uint64) {
	ump.Alarm(UmpModuleName, fmt.Sprintf("LeaderChange: partition=%d, "+
		"newLeader=%d", dp.config.PartitionId, leader))

	if dp.config.NodeId == leader {
		dp.isRaftLeader = true
	}

	//TODO: push leader to master? or check wal index for truncate?
}

func (dp *dataPartition) Put(key, val interface{}) (resp interface{}, err error) {
	item := &RndWrtCmdItem{
		Op: key.(uint32),
		K:  nil,
		V:  nil,
	}
	if val != nil {
		item.V = val.([]byte)
	}
	cmd, err := item.rndWrtCmdMarshalJson()
	if err != nil {
		return
	}

	//submit raftStore
	resp, err = dp.raftPartition.Submit(cmd)
	return
}

func (dp *dataPartition) Get(key interface{}) (interface{}, error) {
	return nil, nil
}

func (dp *dataPartition) Del(key interface{}) (interface{}, error) {
	return nil, nil
}

func (dp *dataPartition) uploadApplyID(applyId uint64) {
	atomic.StoreUint64(&dp.applyId, applyId)
	dp.storeApplyIndex(applyId)
}
