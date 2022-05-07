package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestSegmentKV(t *testing.T) {
	var i int32
	tmpPairs := make([]pairs, 4000)
	for i = 0; i < 4000; i++ {
		tmp := binaryData(i)
		tmpPairs[i].set(tmp, tmp)
	}
	tb := new(TableBuilder)
	tb.data = &tmpPairs
	indexSet := *tb.segmentKV()
	pairSize := tmpPairs[0].keyLen + tmpPairs[0].valueLen + 8
	if pairSize*uint32(indexSet[0]+1) > 4016 {
		t.Error("Split data flow size error,want 4016")
	}
	if indexSet[16] != 0 {
		t.Error("Split data flow number error,want 16")
	}
	fmt.Println(indexSet)
}

func TestBuildDataBlock(t *testing.T) {
	var i int32
	tmpPairs := make([]pairs, 251)
	for i = 0; i < 251; i++ {
		tmp := binaryData(i)
		tmpPairs[i].set(tmp, tmp)
	}
	tb := new(TableBuilder)
	tmpBlock := tb.buildDataBlock(tmpPairs)
	if len(tmpBlock.restartPoint) != 16 {
		t.Error("Data block restart point number error,want 16")
	}
	fmt.Println(tmpBlock)
}

func TestBuildMetaBlock(t *testing.T) {
	var i, j, l int32
	tmpPairs := make([]pairs, 4000)
	for i = 0; i < 4000; i++ {
		tmp := binaryData(i)
		tmpPairs[i].set(tmp, tmp)
	}
	tb := new(TableBuilder)
	tb.data = &tmpPairs
	offset := *tb.segmentKV()
	dataBlockSet := make([]*block, 0, 1024)
	for offset[l] != 0 {
		dataBlockSet = append(dataBlockSet, tb.buildDataBlock(tmpPairs[j:offset[l]+1]))
		j = offset[l] + 1
		l++
	}
	dataBlockSet = append(dataBlockSet, tb.buildDataBlock(tmpPairs[j:]))
	metaBlockSet := tb.buildMetaBlock(dataBlockSet)
	if len(metaBlockSet) != 2 {
		t.Error("Meta block num error,want 2.")
	}
	for k := 0; k < len(metaBlockSet); k++ {
		if int(metaBlockSet[k].filterSize)+4*len(metaBlockSet[k].offset)+10 > 4096 {
			t.Error("Meta block size error,want < 4096")
		}
	}
}

func TestBuildSSTable(t *testing.T) {
	var i int32
	tmpPairs := make([]pairs, 10000)
	for i = 0; i < 10000; i++ {
		tmp := binaryData(i)
		tmpPairs[i].set(tmp, tmp)
	}
	tb := new(TableBuilder)
	tb.data = &tmpPairs
	tb.filterBase = 12
	tb.snappy = 0x0
	tb.fpp = 0.01
	tb.ssTableFile, _ = os.Create("./data/test")
	err := tb.minorCompress()
	if err != nil {
		fmt.Println(err)
	}
}

func TestMessy(t *testing.T) {
	fmt.Println(binaryData(10000))
}
