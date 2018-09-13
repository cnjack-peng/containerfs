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
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/raftstore"
	"github.com/tiglabs/containerfs/util/config"
	"github.com/tiglabs/containerfs/util/log"
	raftproto "github.com/tiglabs/raft/proto"
	"os"
	"strconv"
	"strings"
	"time"
	"path"
	"fmt"
	"io/ioutil"
)

type dataPartitionCfg struct {
	VolName       string              `json:"vol_name"`
	PartitionType string              `json:"partition_type"`
	PartitionId   uint32              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	RandomWrite   bool                `json:"random_write"`
	NodeId        uint64              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
}

func (dp *dataPartition) getRaftPort() (heartbeat, replicate int, err error) {
	raftConfig := dp.config.RaftStore.RaftConfig()
	heartbeatAddrParts := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicateAddrParts := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrParts) != 2 {
		err = errors.New("illegal heartbeat address")
		return
	}
	if len(replicateAddrParts) != 2 {
		err = errors.New("illegal replicate address")
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrParts[1])
	if err != nil {
		return
	}
	replicate, err = strconv.Atoi(replicateAddrParts[1])
	if err != nil {
		return
	}
	return
}

func (dp *dataPartition) StartRaft() (err error) {
	var (
		heartbeatPort int
		replicatePort int
		peers         []raftstore.PeerAddress
	)

	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
		return
	}
	for _, peer := range dp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftproto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicatePort: replicatePort,
		}
		peers = append(peers, rp)
	}
	log.LogDebugf("start partition id=%d raft peers: %s path: %s",
		dp.partitionId, peers, dp.path)
	pc := &raftstore.PartitionConfig{
		ID:      uint64(dp.partitionId),
		Applied: dp.applyId,
		Peers:   peers,
		SM:      dp,
		WalPath: dp.path,
	}

	dp.raftPartition, err = dp.config.RaftStore.CreatePartition(pc)

	return
}

func (dp *dataPartition) stopRaft() {
	if dp.raftPartition != nil {
		dp.raftPartition.Stop()
	}
	return
}

func (dp *dataPartition) confAddNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicatePort int
	)
	if heartbeatPort, replicatePort, err = dp.getRaftPort(); err != nil {
		return
	}

	findAddPeer := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	updated = !findAddPeer
	if !updated {
		return
	}
	dp.config.Peers = append(dp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicatePort)
	return
}

func (dp *dataPartition) confRemoveNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	peerIndex := -1
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		return
	}
	if req.RemovePeer.ID == dp.config.NodeId {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if dp.raftPartition.AppliedIndex() < index {
					continue
				}
				if dp.raftPartition != nil {
					dp.raftPartition.Delete()
				}
				dp.Stop()
				os.RemoveAll(dp.Path())
				log.LogDebugf("[confRemoveNode]: remove self end.")
				return
			}
		}(index)
		updated = false
		log.LogDebugf("[confRemoveNode]: begin remove self.")
		return
	}
	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	log.LogDebugf("[confRemoveNode]: remove peer.")
	return
}

func (dp *dataPartition) confUpdateNode(req *proto.DataPartitionOfflineRequest, index uint64) (updated bool, err error) {
	log.LogDebugf("[confUpdateNode]: not support.")
	return
}

func (dp *dataPartition) storeApplyIndex(applyIndex uint64) (err error) {
	filename := path.Join(dp.Path(), TempApplyIndexFile)
	fp, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.
		O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		fp.Sync()
		fp.Close()
		os.Remove(filename)
	}()
	if _, err = fp.WriteString(fmt.Sprintf("%d", applyIndex)); err != nil {
		return
	}
	err = os.Rename(filename, path.Join(dp.Path(), ApplyIndexFile))
	return
}

func (dp *dataPartition) loadApplyIndex() (err error) {
	filename := path.Join(dp.Path(), ApplyIndexFile)
	if _, err = os.Stat(filename); err != nil {
		err = nil
		return
	}
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if err == os.ErrNotExist {
			err = nil
			return
		}
		err = errors.Errorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.Errorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &dp.applyId); err != nil {
		err = errors.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

//random write need start raft server
func (s *DataNode) parseRaftConfig(cfg *config.Config) (err error) {
	s.raftDir = cfg.GetString(ConfigKeyRaftDir)
	if s.raftDir == "" {
		s.raftDir = DefaultRaftDir
	}
	s.raftHeartbeat = cfg.GetString(ConfigKeyRaftHeartbeat)
	s.raftReplicate = cfg.GetString(ConfigKeyRaftReplicate)

	log.LogDebugf("[parseRaftConfig] load raftDir[%v].", s.raftDir)
	log.LogDebugf("[parseRaftConfig] load raftHearbeat[%v].", s.raftHeartbeat)
	log.LogDebugf("[parseRaftConfig] load raftReplicate[%v].", s.raftReplicate)
	return
}

func (s *DataNode) startRaftServer(cfg *config.Config) (err error) {
	log.LogInfo("Start: startRaftServer")

	s.parseRaftConfig(cfg)

	if _, err = os.Stat(s.raftDir); err != nil {
		if err = os.MkdirAll(s.raftDir, 0755); err != nil {
			err = errors.Errorf("create raft server dir: %s", err.Error())
			log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
			return
		}
	}

	heartbeatPort, _ := strconv.Atoi(s.raftHeartbeat)
	replicatePort, _ := strconv.Atoi(s.raftReplicate)

	raftConf := &raftstore.Config{
		NodeID:        s.nodeId,
		WalPath:       s.raftDir,
		IpAddr:        s.localIp,
		HeartbeatPort: heartbeatPort,
		ReplicatePort: replicatePort,
	}
	s.raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		err = errors.Errorf("new raftStore: %s", err.Error())
		log.LogErrorf("action[startRaftServer] cannot start raft server err[%v]", err)
	}

	return
}

func (s *DataNode) stopRaftServer() {
	if s.raftStore != nil {
		s.raftStore.Stop()
	}
}

