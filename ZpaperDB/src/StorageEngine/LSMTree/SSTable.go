package storage

import (
	"bytes"
	"encoding/binary"
	"ini"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	noCompression     = 0x00
	snappyCompression = 0x01
	//check whether is SSTable file.
	magicNum = 0xdb4775248b80fb57
	//it means that filter will create an instance while data size bigger than 2^12.
	filterBase = 12
)

// SSTable include data block,meta block\meta index block,index block\footer,
// the footer including meta index handle,index handle and padding,magic number.
// data block

type TableBuilder struct {
	data        *[]pairs
	ssTableFile *os.File
}

type block struct {
	keyValueSet []pairs
	blockType   byte
	checksum    uint32
}

type metaBlock struct {
	filterData []filter
	offset     []uint
	filterSize uint
	filterBase byte
	blockType  byte
	checkSum   uint32
}

type pairs struct {
	keyLen   uint
	valueLen uint
	key      []byte
	value    interface{}
}

type footer struct {
	metaIndexHandle BlockHandler
	indexHandle     BlockHandler
	padding         []byte
	magicNum        [8]byte
}

type BlockHandler struct {
	offset uint
	size   uint
}

type compaction struct {
	curLevel   int
	maxFileNum int
	inputFile  [][]*os.File
}

func (lsm *LSMTree) MakeCompaction() {
	var left, right uint8
	cfg, _ := ini.Load("lsm.ini")
	maxNum, _ := cfg.Section("SSTable").Key("maxFileOfOneLevel").Int()
	cp := new(compaction)
	cp.maxFileNum = maxNum
	cp.inputFile = make([][]*os.File, 20)
	for i := 0; i < 20; i++ {
		right = lsm.levelNum[i]
		cp.inputFile[i] = make([]*os.File, 10)
		cp.inputFile[i] = lsm.ssTableFile[left:right]
		left = right
	}
	go func() {
		for {
			if lsm.table.fulled == true {
				lsm.mu.Lock()
				lsm.levelNum[0]++
				tb := new(TableBuilder)
				tb.data = lsm.table.str.export()
				lsm.table.str = lsm.initSkipList()
				lsm.table.fulled = false
				lsm.mu.Unlock()
				tb.ssTableFile, _ = os.Create("./data/level0/ssTable" + strconv.Itoa(lsm.ssTableNum))
				cp.inputFile[0] = append(cp.inputFile[0], tb.ssTableFile)
				tb.minorCompress()
				time.Sleep(10 * time.Second)
			}
		}
	}()
	go func() {
		for {
			for index, value := range lsm.levelNum {
				if value >= uint8(maxNum) {
					cp.curLevel = index
					cp.majorCompress()
				}
			}
			time.Sleep(1 * time.Minute)
		}
	}()
}

func (tb *TableBuilder) minorCompress() {

}

func (cp *compaction) majorCompress() {

}

func (b *BlockHandler) set(offset uint, size uint) {
	b.offset = offset
	b.size = size
}

func (b *BlockHandler) get() (uint, uint) {
	return b.offset, b.size
}

func (tb *TableBuilder) buildDataBlock(pair []pairs) ([]*block, []uint, uint) {
	var i, k, l = 0, 0, 1
	var size, totalSize uint
	offset := make([]uint, 1024)
	offset[0] = 8
	blockSet := make([]*block, 1024)
	for {
		blockSet[k] = new(block)
		size += pair[i].keyLen + pair[i].valueLen + 8
		totalSize += size
		if size > 4091 { // 4096-5
			blockSet[k].keyValueSet = pair[:i-1]
			offset[l] = offset[l-1] + (size - pair[i].keyLen - pair[i].valueLen - 13)
			l++
			i = 0
			k++
		}
		i++
		if i > len(pair) {
			break
		}
	}
	return blockSet, offset, totalSize
}

func (tb *TableBuilder) buildMetaBlock(dataBlockSize uint, dataBlock []*block) ([]*metaBlock, []uint, uint) {
	var k, l, o int
	var size, filterNum, totalSize int
	blockOffset := make([]uint, 200)
	filterSet := make([]filter, 200)
	blockSet := make([]*metaBlock, 200)
	for i := 0; i < len(dataBlock); i++ {
		insertions := len(dataBlock[i].keyValueSet)
		newFilter, _ := MakeBloomFilter(0.01, int64(insertions))
		filterNum++
		for j := 0; j < insertions; j++ {
			newFilter.Add(dataBlock[i].keyValueSet[j].key)
		}
		tmpFilter := new(filter)
		tmpFilter.keyNum = uint(newFilter.ElementNum)
		tmpFilter.bitMap = newFilter.ByteArray
		tmpFilter.hashNum = newFilter.HashNum
		tmpFilter.bitMapLen = uint(newFilter.BitArrayLen)
		filterSet[k] = *tmpFilter
		k++
		size += len(newFilter.ByteArray)
		totalSize += size
		if size > 4086-filterNum*4 {
			blockSet[l].filterData = filterSet[:k-1]
			filterSet[0] = filterSet[k]
			blockSet[l].filterSize = uint(size - len(filterSet[k].bitMap) - 12)
			blockSet[l].blockType = noCompression
			blockSet[l].filterBase = filterBase
			blockOffset[o] = dataBlockSize + blockSet[l].filterSize + uint(4*filterNum) + 6
			o++
			blockSet[l].offset = make([]uint, filterNum)
			var offset uint = 0
			for u := 0; u < filterNum; u++ {
				blockSet[l].offset[u] = offset
				offset += uint(len(filterSet[k].bitMap)) + 12
			}
			size, filterNum, k = 0, 0, 1
		}
	}
	return blockSet, blockOffset, uint(totalSize)
}

func (tb *TableBuilder) buildMetaIndexBlock(metaBlock []*metaBlock, offset []uint) *block {
	var key []byte
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, "zpaperdb.filter")
	if err != nil {
		log.Fatal(err)
	}
	_, err1 := buf.Read(key)
	if err1 != nil {
		log.Fatal(err1)
	}
	metaIndexBlock := new(block)
	metaIndexBlock.keyValueSet = make([]pairs, len(metaBlock))
	for i := 0; i < len(metaBlock); i++ {
		metaIndexBlock.keyValueSet[i].key = key
		metaIndexBlock.keyValueSet[i].keyLen = 15
		tmpHandle := new(BlockHandler)
		tmpHandle.set(offset[i], offset[i+1]-offset[i])
		metaIndexBlock.keyValueSet[i].value = *tmpHandle
		metaIndexBlock.keyValueSet[i].valueLen = 8
	}
	return metaIndexBlock
}

func (tb *TableBuilder) buildIndexBlock(dataBlock []*block, offset []uint) *block {
	indexBlock := new(block)
	indexBlock.keyValueSet = make([]pairs, len(dataBlock))
	for i := 0; i < len(dataBlock); i++ {
		indexBlock.keyValueSet[i].key = dataBlock[i].keyValueSet[i].key
		indexBlock.keyValueSet[i].keyLen = dataBlock[i].keyValueSet[i].keyLen
		tmpHandle := new(BlockHandler)
		tmpHandle.set(offset[i], offset[i+1]-offset[i])
		indexBlock.keyValueSet[i].valueLen = 8
		indexBlock.keyValueSet[i].value = *tmpHandle
	}
	return indexBlock
}

func (tb *TableBuilder) buildFooter(metaIndex, index BlockHandler) *footer {
	tmpFooter := new(footer)
	tmpFooter.indexHandle = index
	tmpFooter.metaIndexHandle = metaIndex
	tmpFooter.padding = make([]byte, 24)
	tmpFooter.magicNum = magicNum
	return tmpFooter
}
