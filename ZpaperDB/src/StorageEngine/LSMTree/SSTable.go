package storage

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"ini"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

// SSTable include data block,meta block\meta index block,index block\footer,
// the footer including meta index handle,index handle and padding,magic number.
// data block

type TableBuilder struct {
	rw          *sync.RWMutex
	data        *[]pairs
	ssTableFile *os.File
	snappy      byte
	fpp         float32
	filterBase  byte
}

type block struct {
	keyValueSet  []pairs
	restartPoint []int32
	restartNum   uint32
	blockType    byte
	checksum     uint32
}

type indexBlock struct {
	keyValueSet  []indexPairs
	restartPoint []int32
	restartNum   uint32
	blockType    byte
	checksum     uint32
}

type metaBlock struct {
	filterData []filter
	offset     []uint32
	filterSize uint32
	filterBase byte
	blockType  byte
	checkSum   uint32
}

type pairs struct {
	keyLen   uint32
	valueLen uint32
	key      []byte
	value    []byte
}

type indexPairs struct {
	keyLen   uint32
	valueLen uint32
	key      []byte
	value    BlockHandler
}

func (p *pairs) set(key []byte, value []byte) {
	p.key = key
	p.keyLen = uint32(len(key))
	p.value = value
	p.valueLen = uint32(len(value))
}

type footer struct {
	metaIndexHandle BlockHandler
	indexHandle     BlockHandler
	padding         []byte
	magicNum        []byte
}

type BlockHandler struct {
	offset uint32
	size   uint32
}

type SSTable struct {
	dataBlockSet   []block
	metaBlockSet   []metaBlock
	metaIndexBlock indexBlock
	indexBlock     indexBlock
	footer         footer
}

type compaction struct {
	curLevel   int
	maxFileNum int
	inputFile  [][]*os.File
}

func (lsm *LSMTree) BeginCompaction() {
	var snappy byte
	var left, right uint8
	cfg, _ := ini.Load("lsm.ini")
	maxNum, _ := cfg.Section("SSTable").Key("maxFileOfOneLevel").Int()
	tmpSnappy, _ := cfg.Section("SSTable").Key("snappyCompression").Bool()
	if tmpSnappy == true {
		snappy = 0x1
	} else {
		snappy = 0x0
	}
	fpp, _ := cfg.Section("SSTable").Key("filterFpp").Float64()
	cp := new(compaction)
	cp.maxFileNum = maxNum
	cp.inputFile = make([][]*os.File, 20)
	for i := 0; i < 20; i++ {
		right = lsm.levelNum[i]
		cp.inputFile[i] = make([]*os.File, 10)
		cp.inputFile[i] = lsm.ssTableFile[left:right]
		left = right
	}
	go lsm.checkMemTableSize()
	go func() {
		for {
			<-lsm.table.fulled
			lsm.mu.Lock()
			lsm.levelNum[0]++
			tb := new(TableBuilder)
			tb.data = lsm.table.str.export()
			tb.snappy = snappy
			tb.fpp = float32(fpp)
			tb.filterBase = 12
			lsm.table.str = lsm.initSkipList()
			lsm.table.size = 0
			lsm.mu.Unlock()
			tb.ssTableFile, _ = os.Create("./data/level0/ssTable" + strconv.Itoa(lsm.ssTableNum))
			cp.inputFile[0] = append(cp.inputFile[0], tb.ssTableFile)
			err := tb.minorCompress()
			if err != nil {
				log.Fatalln(err)
			}
			time.Sleep(10 * time.Second)
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

func (tb *TableBuilder) minorCompress() error {
	var i, j, k int32
	data := *tb.data
	offset := *tb.segmentKV()
	dataBlockSet := make([]*block, 0, 1024)
	for offset[i] != 0 {
		dataBlockSet = append(dataBlockSet, tb.buildDataBlock(data[j:offset[i]+1]))
		j = offset[i] + 1
		k++
	}
	dataBlockNum := len(dataBlockSet)
	dataBlockSize := uint32(dataBlockNum * 4096)
	metaBlockSet := tb.buildMetaBlock(dataBlockSet)
	metaBlockNum := len(metaBlockSet)
	metaBlockSize := uint32(metaBlockNum * 4096)
	metaIndexBlock := tb.buildMetaIndexBlock(metaBlockSet, uint32(len(dataBlockSet)*4096))
	dataIndexBlock := tb.buildIndexBlock(dataBlockSet)
	footerBlock := tb.buildFooter(BlockHandler{
		offset: dataBlockSize + metaBlockSize,
		size:   4096,
	}, BlockHandler{
		offset: dataBlockSize + metaBlockSize + 4096,
		size:   4096,
	})
	var num int
	ssTable := new(SSTable)
	ssTable.dataBlockSet = make([]block, dataBlockNum)
	ssTable.metaBlockSet = make([]metaBlock, metaBlockNum)
	for num = 0; num < dataBlockNum; num++ {
		ssTable.dataBlockSet[num] = *dataBlockSet[num]
	}
	for num = 0; num < metaBlockNum; num++ {
		ssTable.metaBlockSet[num] = *metaBlockSet[num]
	}
	ssTable.metaIndexBlock = *metaIndexBlock
	ssTable.indexBlock = *dataIndexBlock
	ssTable.footer = *footerBlock
	err := binary.Write(tb.ssTableFile, binary.LittleEndian, ssTable)
	if err != nil {
		return err
	}
	return nil
}

func (tb *TableBuilder) segmentKV() *[]int32 {
	var j, sign int
	var size uint32 = 9 //
	indexSet := make([]int32, 256, 1024)
	data := *tb.data
	pairNum := len(*tb.data)
	for i := 0; i < pairNum; i++ {
		size += data[i].keyLen + data[i].valueLen + 8
		sign++
		if sign%16 == 0 {
			size += 4
		}
		if size > 4096 {
			i--
			indexSet[j] = int32(i)
			j++
			size = 9 //reset size
			sign = 0 //reset sign
		}
		if i == pairNum-1 {
			indexSet[j] = int32(i)
		}
	}
	return &indexSet
}

func (cp *compaction) majorCompress() {

}

func (b *BlockHandler) set(offset uint32, size uint32) {
	b.offset = offset
	b.size = size
}

func (b *BlockHandler) get() (uint32, uint32) {
	return b.offset, b.size
}

func (tb *TableBuilder) buildDataBlock(pair []pairs) *block {
	var j, k = 0, 1
	var offset uint32
	pairNum := len(pair)
	newBlock := new(block)
	newBlock.keyValueSet = make([]pairs, pairNum)
	newBlock.restartPoint = make([]int32, pairNum/16+1)
	newBlock.restartNum = uint32(pairNum/16 + 1)
	newBlock.blockType = tb.snappy
	for i := 0; i < pairNum; i++ {
		newBlock.keyValueSet[i] = pair[i]
		offset += pair[i].keyLen + pair[i].valueLen + 8
		j++
		if j == 16 {
			newBlock.restartPoint[k] = int32(offset)
			k++
			j = 0
		}
	}
	return newBlock
}

func (tb *TableBuilder) buildMetaBlock(dataBlock []*block) []*metaBlock {
	var k int
	var filterSize, curBlockSize uint32 = 0, 10
	filterSet := make([]filter, 0, 200)
	offsetSet := make([]uint32, 0, 200)
	offsetSet = append(offsetSet, 0)
	blockSet := make([]*metaBlock, 0, 200)
	for i := 0; i < len(dataBlock)-1; i++ {
		pairNum := int64(len(dataBlock[i].keyValueSet))
		newFilter, _ := MakeBloomFilter(0.01, pairNum)
		for j := 0; j < int(pairNum); j++ {
			newFilter.Add(dataBlock[i].keyValueSet[j].key)
		}
		filterSet = append(filterSet, filter{
			keyNum:    uint32(newFilter.ElementNum),
			bitMap:    newFilter.ByteArray,
			bitMapLen: uint32(newFilter.BitArrayLen),
			hashNum:   int32(newFilter.HashNum),
		})
		filterSize += filterSet[k].getSize()
		curBlockSize = filterSize + 4
		k++
		offsetSet = append(offsetSet, filterSize)
		if curBlockSize > 4096 {
			newMetaBlock := new(metaBlock)
			newMetaBlock.filterData = filterSet[:k-1]
			newMetaBlock.offset = offsetSet[:k-1]
			newMetaBlock.filterSize = filterSize - filterSet[k-1].getSize() - filterSet[k-2].getSize()
			newMetaBlock.filterBase = 0x12
			blockSet = append(blockSet, newMetaBlock)
			i--
			filterSize = 0
			curBlockSize = 10
			offsetSet = offsetSet[:1]
			filterSet = filterSet[0:0]
			k = 0
		}
	}
	blockSet = append(blockSet, &metaBlock{
		filterData: filterSet[:],
		offset:     offsetSet[:],
		filterSize: filterSize,
		filterBase: 0x12,
		blockType:  0,
		checkSum:   0,
	})
	return blockSet
}

func (tb *TableBuilder) buildMetaIndexBlock(metaBlock []*metaBlock, startOffset uint32) *indexBlock {
	var j, k = 0, 1
	key := binaryData("BloomFilter.zpaperdb")
	metaIndexBlock := new(indexBlock)
	metaIndexBlock.keyValueSet = make([]indexPairs, len(metaBlock))
	metaIndexBlock.restartPoint = make([]int32, len(metaBlock)/16+1)
	for i := 0; i < len(metaBlock); i++ {
		metaIndexBlock.keyValueSet[i].key = key
		metaIndexBlock.keyValueSet[i].keyLen = 20
		tmpHandle := new(BlockHandler)
		tmpHandle.set(startOffset+uint32(i*4096), 4096)
		j++
		if j == 16 {
			metaIndexBlock.restartPoint[k] = int32(i * 4096)
			k++
			j = 0
		}
		metaIndexBlock.keyValueSet[i].value = *tmpHandle
		metaIndexBlock.keyValueSet[i].valueLen = 8
	}
	return metaIndexBlock
}

func (tb *TableBuilder) buildIndexBlock(dataBlock []*block) *indexBlock {
	var j, k = 0, 1
	tmpIndexBlock := new(indexBlock)
	tmpIndexBlock.keyValueSet = make([]indexPairs, len(dataBlock))
	for i := 0; i < len(dataBlock); i++ {
		tmpIndexBlock.keyValueSet[i].key = dataBlock[i].keyValueSet[0].key
		tmpIndexBlock.keyValueSet[i].keyLen = dataBlock[i].keyValueSet[0].keyLen
		tmpHandle := new(BlockHandler)
		tmpHandle.set(uint32(i*4096), 4096)
		j++
		if j == 16 {
			tmpIndexBlock.restartPoint[k] = int32(i * 4096)
			k++
			j = 0
		}
		tmpIndexBlock.keyValueSet[i].valueLen = 8
		tmpIndexBlock.keyValueSet[i].value = *tmpHandle
	}
	return tmpIndexBlock
}

func (tb *TableBuilder) buildFooter(metaIndex, index BlockHandler) *footer {
	tmpFooter := new(footer)
	tmpFooter.indexHandle = index
	tmpFooter.metaIndexHandle = metaIndex
	tmpFooter.padding = make([]byte, 24)
	tmpFooter.magicNum = binaryData("db4775248b80fb57")
	return tmpFooter
}

func binaryData(data interface{}) []byte {
	buf := new(bytes.Buffer)
	sign, typeValue := checkKVType(data)
	if sign == false {
		return nil
	} else {
		if typeValue == 0x1 {
			data = []byte(reflect.ValueOf(data).String())
		}
	}
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func checkKVType(data interface{}) (bool, byte) {
	switch reflect.TypeOf(data).Kind() {
	case reflect.String:
		return true, 0x1
	case reflect.Uint8:
		return true, 0x2
	case reflect.Uint16:
		return true, 0x3
	case reflect.Uint32:
		return true, 0x4
	case reflect.Uint64:
		return true, 0x5
	case reflect.Int8:
		return true, 0x6
	case reflect.Int16:
		return true, 0x7
	case reflect.Int32:
		return true, 0x8
	case reflect.Int64:
		return true, 0x9
	case reflect.Float32:
		return true, 0x10
	case reflect.Float64:
		return true, 0x11
	default:
		return false, 0x0
	}
}

func getCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
