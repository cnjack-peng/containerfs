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

package master

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type Topology struct {
	setIndex   uint64
	dataNodes  sync.Map
	metaNodes  sync.Map
	nodeSetMap map[uint64]*NodeSet
	nsLock     sync.RWMutex
}

func NewTopology() (t *Topology) {
	t = new(Topology)
	t.nodeSetMap = make(map[uint64]*NodeSet)
	return
}

type TopoDataNode struct {
	*DataNode
	setId uint64
}

func newTopoDataNode(dataNode *DataNode, setId uint64) *TopoDataNode {
	return &TopoDataNode{
		DataNode: dataNode,
		setId:    setId,
	}
}

func (t *Topology) replaceDataNode(dataNode *DataNode) {
	if oldRack, err := t.getRack(dataNode); err == nil {
		oldRack.PutDataNode(dataNode)
	}
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if ok {
		node := topoNode.(*TopoDataNode)
		node.RackName = dataNode.RackName
		t.putDataNodeToCache(node)
	}
}

func (t *Topology) PutDataNode(dataNode *DataNode) (err error) {

	if _, ok := t.dataNodes.Load(dataNode.Addr); ok {
		return
	}
	var ns *NodeSet
	if ns, err = t.getNodeSet(dataNode.NodeSetId); err != nil {
		return
	}
	ns.putDataNode(dataNode)
	node := newTopoDataNode(dataNode, ns.Id)
	t.putDataNodeToCache(node)
	return
}

func (t *Topology) putDataNodeToCache(dataNode *TopoDataNode) {
	t.dataNodes.Store(dataNode.Id, dataNode)
}

func (t *Topology) getRack(dataNode *DataNode) (rack *Rack, err error) {
	topoNode, ok := t.dataNodes.Load(dataNode.Addr)
	if !ok {
		return nil, errors.Annotatef(DataNodeNotFound, "%v not found", dataNode.Addr)
	}
	node := topoNode.(*TopoDataNode)
	ns, err := t.getNodeSet(node.setId)
	if err != nil {
		return
	}
	return ns.getRack(node.RackName)
}

func (t *Topology) getAvailNodeSetForDataNode() (ns *NodeSet) {
	t.nsLock.RLock()
	defer t.nsLock.RUnlock()
	for _, ns = range t.nodeSetMap {
		if ns.dataNodeLen < ns.Capacity {
			return
		}
	}
	return
}

func (t *Topology) getAvailNodeSetForMetaNode() (ns *NodeSet) {
	t.nsLock.RLock()
	defer t.nsLock.RUnlock()
	for _, ns = range t.nodeSetMap {
		if ns.metaNodeLen < ns.Capacity {
			return
		}
	}
	return
}

func (t *Topology) createNodeSet(c *Cluster) (ns *NodeSet, err error) {
	id, err := c.idAlloc.allocateMetaNodeID()
	if err != nil {
		return
	}
	ns = newNodeSet(id, DefaultNodeSetCapacity)
	if err = c.syncAddNodeSet(ns); err != nil {
		return
	}
	t.putNodeSet(ns)
	return
}

func (t *Topology) putNodeSet(ns *NodeSet) {
	t.nsLock.Lock()
	defer t.nsLock.Unlock()
	t.nodeSetMap[ns.Id] = ns
}

func (t *Topology) getNodeSet(setId uint64) (ns *NodeSet, err error) {
	t.nsLock.RLock()
	defer t.nsLock.RUnlock()
	ns, ok := t.nodeSetMap[setId]
	if !ok {
		return nil, errors.Errorf("set %v not found", setId)
	}
	return
}

func (t *Topology) allocNodeSet(replicaNum uint8) (ns *NodeSet, err error) {
	for i := 0; i < len(t.nodeSetMap); i++ {
		if t.setIndex >= uint64(len(t.nodeSetMap)) {
			t.setIndex = 0
		}
		ns = t.nodeSetMap[t.setIndex]
		if ns.canWrite(replicaNum) {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[allocNodeSet],err:%v", NoNodeSetForCreateDataPartition))
	return nil, NoNodeSetForCreateDataPartition
}

type NodeSet struct {
	Id          uint64
	Capacity    int
	rackIndex   int
	rackMap     map[string]*Rack
	racks       []string
	rackLock    sync.RWMutex
	dataNodeLen int
	metaNodeLen int
	nsLock      sync.RWMutex
}

func newNodeSet(id uint64, cap int) *NodeSet {
	ns := &NodeSet{
		Id:       id,
		Capacity: cap,
	}
	ns.rackMap = make(map[string]*Rack)
	ns.racks = make([]string, 0)
	return ns
}

func (ns *NodeSet) canWrite(replicaNum uint8) bool {
	for _, rack := range ns.rackMap {
		if rack.canWrite(replicaNum) {
			return true
		}
	}
	return false
}

func (ns *NodeSet) isSingleRack() bool {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	return len(ns.rackMap) == 1
}

func (ns *NodeSet) getRack(name string) (rack *Rack, err error) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	rack, ok := ns.rackMap[name]
	if !ok {
		return nil, errors.Annotatef(RackNotFound, "%v not found", name)
	}
	return
}

func (ns *NodeSet) putRack(rack *Rack) {
	ns.rackLock.Lock()
	defer ns.rackLock.Unlock()
	ns.rackMap[rack.name] = rack
	if ok := ns.isExist(rack.name); !ok {
		ns.racks = append(ns.racks, rack.name)
	}
}

func (ns *NodeSet) isExist(rackName string) (ok bool) {
	for _, name := range ns.racks {
		if name == rackName {
			ok = true
			return
		}
	}
	return
}

func (ns *NodeSet) removeRack(name string) {
	ns.rackLock.Lock()
	defer ns.rackLock.Unlock()
	delete(ns.rackMap, name)
}

func (ns *NodeSet) putDataNode(dataNode *DataNode) {
	rack, err := ns.getRack(dataNode.RackName)
	if err != nil {
		rack = NewRack(dataNode.RackName)
		ns.putRack(rack)
	}
	rack.PutDataNode(dataNode)
	ns.nsLock.Lock()
	ns.dataNodeLen++
	ns.nsLock.Unlock()
}

func (ns *NodeSet) getAllRacks() (racks []*Rack) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	racks = make([]*Rack, 0)
	for _, rack := range ns.rackMap {
		racks = append(racks, rack)
	}
	return
}

func (ns *NodeSet) getRackNameByIndex(index int) (rName string) {
	ns.rackLock.RLock()
	defer ns.rackLock.RUnlock()
	rName = ns.racks[index]
	return
}

func (ns *NodeSet) allocRacks(replicaNum int, excludeRack []string) (racks []*Rack, err error) {
	racks = make([]*Rack, 0)
	if excludeRack == nil {
		excludeRack = make([]string, 0)
	}
	racks = ns.getAllRacks()
	if ns.isSingleRack() {
		return racks, nil
	}

	for i := 0; i < len(racks); i++ {
		if ns.rackIndex >= len(racks) {
			ns.rackIndex = 0
		}
		rName := ns.getRackNameByIndex(ns.rackIndex)
		if contains(excludeRack, rName) {
			continue
		}
		ns.rackIndex++
		var rack *Rack
		if rack, err = ns.getRack(ns.racks[ns.rackIndex]); err != nil {
			continue
		}
		if rack.canWrite(1) {
			racks = append(racks, rack)
		}
		if len(racks) >= int(replicaNum) {
			break
		}
	}
	if len(racks) == 0 {
		log.LogError(fmt.Sprintf("action[allocRacks],err:%v", NoRackForCreateDataPartition))
		return nil, NoRackForCreateDataPartition
	}
	if len(racks) > int(replicaNum) {
		racks = racks[:int(replicaNum)]
	}
	err = nil
	return
}

type Rack struct {
	name      string
	dataNodes sync.Map
	metaNodes sync.Map
	sync.RWMutex
}

func NewRack(name string) (rack *Rack) {
	return &Rack{name: name}
}

func (rack *Rack) PutDataNode(dataNode *DataNode) {
	rack.dataNodes.Store(dataNode.Addr, dataNode)
}

func (rack *Rack) GetDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := rack.dataNodes.Load(addr)
	if !ok {
		return nil, errors.Annotatef(DataNodeNotFound, "%v not found", addr)
	}
	dataNode = value.(*DataNode)
	return
}
func (rack *Rack) RemoveDataNode(addr string) {
	rack.dataNodes.Delete(addr)
}

func (rack *Rack) canWrite(replicaNum uint8) (can bool) {
	rack.RLock()
	defer rack.RUnlock()
	var leastAlive uint8
	rack.dataNodes.Range(func(addr, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.isActive == true && dataNode.IsWriteAble() == true {
			leastAlive++
		}
		if leastAlive >= replicaNum {
			can = true
			return false
		}
		return true
	})
	return
}

func (rack *Rack) GetDataNodeMaxTotal() (maxTotal uint64) {
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if dataNode.Total > maxTotal {
			maxTotal = dataNode.Total
		}
		return true
	})
	return
}

func (rack *Rack) getAvailDataNodeHosts(excludeHosts []string, replicaNum int) (newHosts []string, peers []proto.Peer, err error) {
	orderHosts := make([]string, 0)
	newHosts = make([]string, 0)
	peers = make([]proto.Peer, 0)
	if replicaNum == 0 {
		return
	}

	maxTotal := rack.GetDataNodeMaxTotal()
	nodeTabs, availCarryCount := rack.GetAvailCarryDataNodeTab(maxTotal, excludeHosts, replicaNum)
	if len(nodeTabs) < replicaNum {
		err = NoHaveAnyDataNodeToWrite
		err = fmt.Errorf(GetAvailDataNodeHostsErr+" err:%v ,ActiveNodeCount:%v  MatchNodeCount:%v  ",
			NoHaveAnyDataNodeToWrite, rack.DataNodeCount(), len(nodeTabs))
		return
	}

	nodeTabs.SetNodeTabCarry(availCarryCount, replicaNum)
	sort.Sort(nodeTabs)

	for i := 0; i < replicaNum; i++ {
		node := nodeTabs[i].Ptr.(*DataNode)
		node.SelectNodeForWrite()
		orderHosts = append(orderHosts, node.Addr)
		peer := proto.Peer{ID: node.Id, Addr: node.Addr}
		peers = append(peers, peer)
	}

	if newHosts, err = rack.DisOrderArray(orderHosts); err != nil {
		err = fmt.Errorf(GetAvailDataNodeHostsErr+"err:%v  orderHosts is nil", err.Error())
		return
	}
	return
}

func (rack *Rack) GetAvailCarryDataNodeTab(maxTotal uint64, excludeHosts []string, replicaNum int) (nodeTabs NodeTabArrSorterByCarry, availCount int) {
	nodeTabs = make(NodeTabArrSorterByCarry, 0)
	rack.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		if contains(excludeHosts, dataNode.Addr) == true {
			log.LogDebugf("contains return")
			return true
		}
		if dataNode.IsWriteAble() == false {
			log.LogDebugf("isWritable return")
			return true
		}
		if dataNode.IsAvailCarryNode() == true {
			availCount++
		}
		nt := new(NodeTab)
		nt.Carry = dataNode.Carry
		if dataNode.Used < 0 {
			nt.Weight = 1.0
		} else {
			nt.Weight = float64(dataNode.RemainWeightsForCreateVol) / float64(maxTotal)
		}
		nt.Ptr = dataNode
		nodeTabs = append(nodeTabs, nt)

		return true
	})

	return
}

func (rack *Rack) DataNodeCount() (len int) {

	rack.dataNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (rack *Rack) DisOrderArray(oldHosts []string) (newHosts []string, err error) {
	var (
		newCurrPos int
	)

	if oldHosts == nil || len(oldHosts) == 0 {
		log.LogError(fmt.Sprintf("action[DisOrderArray],err:%v", DisOrderArrayErr))
		err = DisOrderArrayErr
		return
	}

	lenOldHosts := len(oldHosts)
	newHosts = make([]string, lenOldHosts)
	if lenOldHosts == 1 {
		copy(newHosts, oldHosts)
		return
	}

	for randCount := 0; randCount < lenOldHosts; randCount++ {
		remainCount := lenOldHosts - randCount
		rand.Seed(time.Now().UnixNano())
		oCurrPos := rand.Intn(remainCount)
		newHosts[newCurrPos] = oldHosts[oCurrPos]
		newCurrPos++
		oldHosts[oCurrPos] = oldHosts[remainCount-1]
	}

	return
}
