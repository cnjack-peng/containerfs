package metanode

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/util/log"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

const (
	prefixDelExtent     = "EXTENT_DEL"
	maxDeleteExtentSize = 10 * MB
)

func (mp *metaPartition) startDeleteExtents() {
	fileList := list.New()
	// start Append Delete Extents to File Worker
	go mp.appendDelExtentsToFile(fileList)
	// start ticket delete file worker
	go mp.deleteExtentsFile(fileList)
}

func (mp *metaPartition) appendDelExtentsToFile(fileList *list.List) {
	var (
		fileName     string
		fileSize     int64
		idx          int64
		fp           *os.File
		err          error
		cursorHeader [8]byte
	)
	binary.BigEndian.PutUint64(cursorHeader[:], 8)
LOOP:
	finfos, err := ioutil.ReadDir(mp.config.RootDir)
	if err != nil {
		panic(err)
	}
	for _, info := range finfos {
		if strings.HasPrefix(info.Name(), prefixDelExtent) {
			fileList.PushBack(info.Name())
			fileSize = info.Size()
		}
	}
	lastItem := fileList.Back()
	if lastItem == nil {
		fileName = fmt.Sprintf("%s_%d", prefixDelExtent, idx)
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
		fp.Write(cursorHeader[:])
	} else {
		fileName = lastItem.Value.(string)
		fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(err)
		}
	}

	defer fp.Close()
	var buf []byte
	for {
		select {
		case <-mp.stopC:
			return
		case <-mp.extReset:
			fp.Close()
			// reset fileList
			fileList.Init()
			goto LOOP
		case item := <-mp.extDelCh:
			ek := item.(*proto.ExtentKey)
			buf, err = ek.MarshalBinary()
			if err != nil {
				log.LogWarnf("[appendDelExtentsToFile] extentKey marshal: %s"+
					"", err.Error())
				mp.extDelCh <- ek
				continue
			}
			if fileSize >= maxDeleteExtentSize {
				// close old File
				fp.Close()
				idx += 1
				fileName = fmt.Sprintf("%s_%d", prefixDelExtent, idx)
				fp, err = os.OpenFile(path.Join(mp.config.RootDir, fileName),
					os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
				if _, err = fp.Write(cursorHeader[:]); err != nil {
					panic(err)
				}
				fileSize = 8
			}
			// write file
			if _, err = fp.Write(buf); err != nil {
				panic(err)
			}
			fileSize += int64(len(buf))
		}
	}

}

func (mp *metaPartition) deleteExtentsFile(fileList *list.List) {
	var (
		element  *list.Element
		fileName string
		file     string
		fileInfo os.FileInfo
		err      error
	)
	for {
		time.Sleep(10 * time.Minute)
		select {
		case <-mp.stopC:
			return
		default:
		}
	LOOP:
		element = fileList.Front()
		if element == nil {
			continue
		}
		fileName = element.Value.(string)
		file = path.Join(mp.config.RootDir, fileName)
		if fileInfo, err = os.Stat(file); err != nil {
			fileList.Remove(element)
			goto LOOP
		}
		if _, ok := mp.IsLeader(); !ok {
			log.LogDebugf("[deleteExtentsFile] not raft leader, please ignore")
			continue
		}
		buf := make([]byte, MB)
		fp, err := os.OpenFile(file, os.O_RDWR, 0644)
		if err != nil {
			panic(err)
		}

		if _, err = fp.ReadAt(buf[:8], 0); err != nil {
			log.LogWarnf("[deleteExtentsFile] read cursor least 8bytes, " +
				"retry later")
			fp.Close()
			continue
		}
		cursor := binary.BigEndian.Uint64(buf)
		if size := uint64(fileInfo.Size()) - cursor; size < MB {
			if size <= 0 {
				size = uint64(proto.ExtentLength)
			} else if size > 0 && size < uint64(proto.ExtentLength) {
				errStr := fmt.Sprintf("[deleteExtentsFile] %s file corrupted!", fileName)
				log.LogErrorf(errStr)
				panic(errStr)
			}
			buf = buf[:size]
		}
		n, err := fp.ReadAt(buf, int64(cursor))
		fp.Close()
		if err != nil {
			if err == io.EOF {
				err = nil
				if fileList.Len() > 1 {
					status := mp.raftPartition.Status()
					if status.State == "StateLeader" && !status.
						RestoringSnapshot {
						if _, err = mp.Put(opFSMInternalDelExtentFile,
							[]byte(fileName)); err != nil {
							log.LogErrorf(
								"[deleteExtentsFile] delete old file: %s, "+
									"status: %s", fileName, err.Error())
						}
						log.LogDebugf("[deleteExtentsFile] delete old file"+
							": %s, status: %v", fileName, err == nil)
						goto LOOP
					}
					log.LogDebugf("[deleteExtentsFile] delete old file"+
						" status: %s", status.State)
				} else {
					log.LogDebugf("[deleteExtentsFile] %s extents delete ok",
						fileName)
				}
				continue
			}
			panic(err)
		}
		buff := bytes.NewBuffer(buf)
		cursor += uint64(n)
		for {
			if buff.Len() == 0 {
				break
			}
			if buff.Len() < proto.ExtentLength {
				cursor -= uint64(buff.Len())
			}
			ek := &proto.ExtentKey{}
			if err = ek.UnmarshalBinary(buff); err != nil {
				panic(err)
			}
			// delete dataPartition
			if err = mp.executeDeleteDataPartition(ek); err != nil {
				mp.extDelCh <- ek
				log.LogWarnf("[deleteExtentsFile] %s", err.Error())
			}
		}
		buff.Reset()
		buff.WriteString(fmt.Sprintf("%s %d", fileName, cursor))
		if _, err = mp.Put(opFSMInternalDelExtentCursor, buff.Bytes()); err != nil {
			log.LogWarnf("[deleteExtentsFile] %s", err.Error())
		}
		log.LogDebugf("[deleteExtentsFile] file=%s, cursor=%d", fileName, cursor)
		goto LOOP
	}
}
