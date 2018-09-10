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

package metanode

import (
	"bytes"
	"encoding/binary"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/btree"
	"io"
)

type ResponseInode struct {
	Status uint8
	Msg    *Inode
}

func NewResponseInode() *ResponseInode {
	return &ResponseInode{
		Msg: NewInode(0, 0),
	}
}

// CreateInode create inode to inode tree.
func (mp *metaPartition) createInode(ino *Inode) (status uint8) {
	status = proto.OpOk
	if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
		status = proto.OpExistErr
	}
	return
}

func (mp *metaPartition) createLinkInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.Type == proto.ModeDir {
		resp.Status = proto.OpArgMismatchErr
		return
	}
	if i.MarkDelete == 1 {
		resp.Status = proto.OpNotExistErr
		return
	}
	i.NLink++
	resp.Msg = i
	return
}

// GetInode query inode from InodeTree with specified inode info;
func (mp *metaPartition) getInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		resp.Status = proto.OpNotExistErr
		return
	}
	i := item.(*Inode)
	if i.MarkDelete == 1 {
		resp.Status = proto.OpNotExistErr
		return
	}
	resp.Msg = i
	return
}

func (mp *metaPartition) hasInode(ino *Inode) (ok bool) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		ok = false
		return
	}
	i := item.(*Inode)
	if i.MarkDelete == 1 {
		ok = false
		return
	}
	ok = true
	return
}

func (mp *metaPartition) internalHasInode(ino *Inode) bool {
	return mp.inodeTree.Has(ino)
}

func (mp *metaPartition) getInodeTree() *BTree {
	return mp.inodeTree.GetTree()
}

func (mp *metaPartition) RangeInode(f func(i btree.Item) bool) {
	mp.inodeTree.Ascend(f)
}

// DeleteInode delete specified inode item from inode tree.
func (mp *metaPartition) deleteInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	isDelete := false
	mp.inodeTree.Find(ino, func(i BtreeItem) {
		isFind = true
		inode := i.(*Inode)
		resp.Msg = inode
		if inode.Type == proto.ModeRegular {
			inode.NLink--
			return
		}
		// should delete inode
		isDelete = true
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}
	if isDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) internalDelete(val []byte) (err error) {
	if len(val) == 0 {
		return
	}
	buf := bytes.NewBuffer(val)
	ino := NewInode(0, 0)
	for {
		err = binary.Read(buf, binary.BigEndian, &ino.Inode)
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		mp.internalDeleteInode(ino)
	}
}

func (mp *metaPartition) internalDeleteInode(ino *Inode) {
	mp.inodeTree.Delete(ino)
	return
}

func (mp *metaPartition) appendExtents(ino *Inode) (status uint8) {
	exts := ino.Extents
	status = proto.OpOk
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	ino = item.(*Inode)
	if ino.MarkDelete == 1 {
		status = proto.OpNotExistErr
		return
	}
	modifyTime := ino.ModifyTime
	var delItems []BtreeItem
	exts.Range(func(item BtreeItem) bool {
		delItems = ino.AppendExtents(item)
		return true
	})
	ino.ModifyTime = modifyTime
	ino.Generation++
	if len(delItems) > 0 {
		for _, item := range delItems {
			mp.extDelCh <- item
		}
	}
	return
}

func (mp *metaPartition) extentsTruncate(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	var delExtents []BtreeItem
	mp.inodeTree.Find(ino, func(item BtreeItem) {
		isFind = true
		i := item.(*Inode)
		if i.Type == proto.ModeDir {
			resp.Status = proto.OpArgMismatchErr
			return
		}
		if i.MarkDelete == 1 {
			resp.Status = proto.OpNotExistErr
			return
		}
		i.Size = ino.Size
		i.ModifyTime = ino.ModifyTime
		i.Generation++
		i.Extents.Extents.AscendGreaterOrEqual(&proto.
			ExtentKey{FileOffset: ino.Size},
			func(item BtreeItem) bool {
				delExtents = append(delExtents, item)
				return true
			})
		// delete
		for _, ext := range delExtents {
			i.Extents.Delete(ext)
			mp.extDelCh <- ext
		}
		// check max
		extItem := i.Extents.Max()
		ext := extItem.(*proto.ExtentKey)
		if n := ino.Size - ext.FileOffset; ext.Size > uint32(n) {
			ext.Size = uint32(n)
		}
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}

	return
}

func (mp *metaPartition) evictInode(ino *Inode) (resp *ResponseInode) {
	resp = NewResponseInode()
	resp.Status = proto.OpOk
	isFind := false
	isDelete := false
	mp.inodeTree.Find(ino, func(item BtreeItem) {
		isFind = true
		i := item.(*Inode)
		if i.Type == proto.ModeDir {
			if i.NLink < 2 {
				isDelete = true
			}
			return
		}

		if i.MarkDelete == 1 {
			return
		}
		if i.NLink < 1 {
			i.MarkDelete = 1
			// push to free list
			mp.freeList.Push(i)
		}
	})
	if !isFind {
		resp.Status = proto.OpNotExistErr
		return
	}
	if isDelete {
		mp.inodeTree.Delete(ino)
	}
	return
}

func (mp *metaPartition) checkAndInsertFreeList(ino *Inode) {
	if ino.Type == proto.ModeDir {
		return
	}
	if ino.MarkDelete == 1 {
		mp.freeList.Push(ino)
	}
}
