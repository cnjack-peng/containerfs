package stream

import (
	"fmt"
	"sync"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/btree"
	"github.com/tiglabs/containerfs/util/log"
)

type ExtentRequest struct {
	FileOffset int
	Size       int
	Data       []byte
	ExtentKey  *proto.ExtentKey
}

func (er *ExtentRequest) String() string {
	return fmt.Sprintf("FileOffset(%v) Size(%v) ExtentKey(%v)", er.FileOffset, er.Size, er.ExtentKey)
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
	//log.LogDebugf("Local ExtentCache before update: gen(%v) size(%v) extents(%v)", cache.gen, cache.size, cache.List())
	cache.update(gen, size, extents)
	//log.LogDebugf("Local ExtentCache after update: gen(%v) size(%v) extents(%v)", cache.gen, cache.size, cache.List())
	return nil
}

func (cache *ExtentCache) update(gen, size uint64, eks []proto.ExtentKey) {
	cache.Lock()
	defer cache.Unlock()

	log.LogDebugf("ExtentCache update: cache.gen(%v) cache.size(%v) gen(%v) size(%v)", cache.gen, cache.size, gen, size)

	if cache.gen != 0 && cache.gen >= gen {
		log.LogDebugf("ExtentCache update: no need to update, remote extents(%v)", eks)
		return
	}

	cache.gen = gen
	cache.size = size
	cache.root.Clear(false)
	for _, ek := range eks {
		//log.LogDebugf("update: ek(%v)", ek)
		extent := ek
		cache.root.ReplaceOrInsert(&extent)
	}
}

func (cache *ExtentCache) Append(ek *proto.ExtentKey, sync bool) {
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
	if sync {
		cache.gen++
	}
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

func (cache *ExtentCache) Size() (size int, gen uint64) {
	cache.RLock()
	defer cache.RUnlock()
	return int(cache.size), cache.gen
}

func (cache *ExtentCache) SetSize(size uint64) {
	cache.Lock()
	defer cache.Unlock()
	cache.size = size
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
	upper := &proto.ExtentKey{FileOffset: uint64(offset + size)}
	start := offset
	end := offset + size

	cache.RLock()
	defer cache.RUnlock()

	lower := &proto.ExtentKey{}
	cache.root.DescendLessOrEqual(pivot, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		lower.FileOffset = ek.FileOffset
		return false
	})

	cache.root.AscendRange(lower, upper, func(i btree.Item) bool {
		ek := i.(*proto.ExtentKey)
		ekStart := int(ek.FileOffset)
		ekEnd := int(ek.FileOffset) + int(ek.Size)

		log.LogDebugf("PrepareRequest: start(%v) end(%v) ekStart(%v) ekEnd(%v)", start, end, ekStart, ekEnd)

		if start < ekStart {
			if end <= ekStart {
				return false
			} else if end < ekEnd {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)
				// add non-hole (ekStart, end)
				req = NewExtentRequest(ekStart, end-ekStart, data[ekStart-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add hole (start, ekStart)
				req := NewExtentRequest(start, ekStart-start, data[start-offset:ekStart-offset], nil)
				requests = append(requests, req)

				// add non-hole (ekStart, ekEnd)
				req = NewExtentRequest(ekStart, ekEnd-ekStart, data[ekStart-offset:ekEnd-offset], ek)
				requests = append(requests, req)

				start = ekEnd
				return true
			}
		} else if start < ekEnd {
			if end <= ekEnd {
				// add non-hole (start, end)
				req := NewExtentRequest(start, end-start, data[start-offset:end-offset], ek)
				requests = append(requests, req)
				start = end
				return false
			} else {
				// add non-hole (start, ekEnd), start = ekEnd
				req := NewExtentRequest(start, ekEnd-start, data[start-offset:ekEnd-offset], ek)
				requests = append(requests, req)
				start = ekEnd
				return true
			}
		} else {
			return true
		}
	})

	log.LogDebugf("PrepareRequest: start(%v) end(%v)", start, end)
	if start < end {
		// add hole (start, end)
		req := NewExtentRequest(start, end-start, data[start-offset:end-offset], nil)
		requests = append(requests, req)
	}

	return requests
}
