package datanode

import (
	_ "net/http/pprof"

	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
)

type RndWrtCmdItem struct {
	Op uint32 `json:"op"`
	K  []byte `json:"k"`
	V  []byte `json:"v"`
}

type rndWrtOpItem struct {
	extentId uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
}

// Marshal random write value to binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+------+
//  | Item | extentId | offset | size | crc | data |
//  +------+----+------+------+------+------+------+
//  | byte |     8    |    8   |  8   |  4  | size |
//  +------+----+------+------+------+------+------+
func rndWrtDataMarshal(extentId uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size))
	if err = binary.Write(buff, binary.BigEndian, extentId); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, crc); err != nil {
		return
	}
	if _, err = buff.Write(data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func rndWrtDataUnmarshal(raw []byte) (result *rndWrtOpItem, err error) {
	var opItem rndWrtOpItem

	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentId); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}

	result = &opItem
	return
}

func (rndWrtItem *RndWrtCmdItem) rndWrtCmdMarshalJson() (cmd []byte, err error) {
	return json.Marshal(rndWrtItem)
}

func (rndWrtItem *RndWrtCmdItem) rndWrtCmdUnmarshal(cmd []byte) (err error) {
	return json.Unmarshal(cmd, rndWrtItem)
}

func (dp *dataPartition) RndWrtSubmit(pkg *Packet) (err error) {
	val, err := rndWrtDataMarshal(pkg.FileID, pkg.Offset, int64(pkg.Size), pkg.Data, pkg.Crc)
	if err != nil {
		return
	}
	resp, err := dp.Put(opRandomWrite, val)
	if err != nil {
		return
	}

	pkg.ResultCode = resp.(uint8)
	log.LogDebugf("[rndWrtSubmit] raft sync: response status = %v.", pkg.GetResultMesg())
	return
}

func (dp *dataPartition) rndWrtStore(opItem *rndWrtOpItem) (status uint8) {
	status = proto.OpOk
	err := dp.GetExtentStore().Write(opItem.extentId, opItem.offset, opItem.size, opItem.data, opItem.crc)
	if err != nil {
		log.LogError("[rndWrtStore] write err", err)
		status = proto.OpExistErr
	}
	return
}

type ItemIterator struct {
	applyID uint64
	cur     int
	total   int
}

func NewItemIterator(applyID uint64) *ItemIterator {
	si := new(ItemIterator)
	si.applyID = applyID
	return si
}

func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

func (si *ItemIterator) Close() {
	si.cur = si.total + 1
	return
}

func (si *ItemIterator) Next() (data []byte, err error) {
	if si.cur > si.total {
		err = io.EOF
		data = nil
		return
	}
	// First Send ApplyIndex
	if si.cur == 0 {
		appIdBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(appIdBuf, si.applyID)
		data = appIdBuf[:]
		si.cur++
		return
	}

	return
}
