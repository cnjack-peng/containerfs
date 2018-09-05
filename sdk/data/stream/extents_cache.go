package stream

import (
	"sync"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/btree"
)

type ExtentRequest struct {
	FileOffset int
	Size       int
	Data       []byte
	ExtentKey  *proto.ExtentKey
}

func NewExtentRequest(offset, size int, data []byte, ek *proto.ExtentKey) *ExtentRequest {
	return &ExtentRequest{
		FileOffset: offset,
		Size:       size,
		Data:       data,
		ExtentKey:  ek,
	}
}

type ExtentCache struct {
	sync.RWMutex
	gen  uint64
	size uint64
	root *btree.BTree
}

func NewExtentCache() *ExtentCache {
	return &ExtentCache{root: btree.New(32)}
}

func (cache *ExtentCache) Refresh(inode uint64, getExtents GetExtentsFunc) error {
	gen, size, extents, err := getExtents(inode)
	if err != nil {
		return err
	}
	cache.update(gen, size, extents)
	return nil
}

func (cache *ExtentCache) update(gen, size uint64, eks []proto.ExtentKey) {
	cache.Lock()
	defer cache.Unlock()

	if cache.gen != 0 && cache.gen >= gen {
		return
	}

	cache.gen = gen
	cache.root = btree.New(32)
	for _, ek := range eks {
		cache.root.ReplaceOrInsert(&ek)
	}
}

func (cache *ExtentCache) Append(ek *proto.ExtentKey) {
	ekEnd := ek.FileOffset + uint64(ek.Size)
	lower := &proto.ExtentKey{FileOffset: ek.FileOffset}
	upper := &proto.ExtentKey{FileOffset: ekEnd}
	discard := make([]*proto.ExtentKey, 0)

	cache.Lock()
	defer cache.Unlock()

	cache.root.AscendRange(lower, upper, func(i btree.Item) bool {
		found := i.(*proto.ExtentKey)
		discard = append(discard, found)
		return true
	})

	for _, key := range discard {
		cache.root.Delete(key)
	}

	cache.root.ReplaceOrInsert(ek)
	cache.gen++
	if ekEnd > cache.size {
		cache.size = ekEnd
	}
}

func (cache *ExtentCache) Max() *proto.ExtentKey {
	cache.RLock()
	defer cache.RUnlock()
	ek := cache.root.Max().(*proto.ExtentKey)
	return ek
}

func (cache *ExtentCache) Size() uint64 {
	cache.RLock()
	defer cache.RUnlock()
	return cache.size
}

func (cache *ExtentCache) List() []*proto.ExtentKey {
	cache.RLock()
	root := cache.root.Clone()
	cache.RUnlock()

	extents := make([]*proto.ExtentKey, 0, root.Len())
	root.Ascend(func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		extents = append(extents, ek)
		return true
	})
	return extents
}

func (cache *ExtentCache) Get(offset uint64) (ret *proto.ExtentKey) {
	pivot := &proto.ExtentKey{FileOffset: offset + 1}
	cache.RLock()
	defer cache.RUnlock()

	cache.root.AscendLessThan(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		if offset >= ek.FileOffset && offset < ek.FileOffset+uint64(ek.Size) {
			ret = ek
		}
		return false
	})
	return ret
}

func (cache *ExtentCache) PrepareRequest(offset, size int, data []byte) []*ExtentRequest {
	requests := make([]*ExtentRequest, 0)
	pivot := &proto.ExtentKey{FileOffset: uint64(offset)}
	start := offset
	end := offset + size

	cache.RLock()
	defer cache.RUnlock()

	cache.root.AscendLessThan(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		ekStart := int(ek.FileOffset)
		ekEnd := int(ek.FileOffset) + int(ek.Size)

		if start < ekStart {
			if end <= ekStart {
				// add hole (start, end)
				req := NewExtentRequest(start, end, data[start-offset:end-offset], nil)
				requests = append(requests, req)
				return false
			} else if end < ekEnd {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)
				// add non-hole (ekStart, end)
				req = NewExtentRequest(ekStart, end, data[ekStart-offset:end-offset], ek)
				requests = append(requests, req)
				return false
			} else {
				return true
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				req := NewExtentRequest(start, end, data[start-offset:end-offset], ek)
				requests = append(requests, req)
				return false
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				req := NewExtentRequest(start, ekEnd, data[start-offset:ekEnd-offset], ek)
				requests = append(requests, req)
				return true
			}
		} else {
			if end <= ekEnd {
				//add hole (start, end)
				req := NewExtentRequest(start, end, data[start-offset:end-offset], nil)
				requests = append(requests, req)
				return false
			} else {
				return true
			}
		}
	})

	return requests
}
