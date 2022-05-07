package storage

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestHashFunc(t *testing.T) {
	expe := []uint64{93, 1440, 780, 1825, 1722, 827}
	bf, _ := MakeBloomFilter(0.01, 200)
	byteSet := []byte{1, 2, 3}
	hashValueSet := bf.mappingByHash(byteSet)
	for i := range expe {
		if hashValueSet[i] != expe[i] {
			t.Error("Hash value error,hash function changed.")
		}
	}
}

func TestBloomFilter_Add(t *testing.T) {
	bf, _ := MakeBloomFilter(0.01, 200)
	for i := 0; i < 100; i++ {
		bf.Add(binaryData(strconv.Itoa(i)))
	}
	if bf.BitArrayLen != 1917 {
		t.Error("Bloom filter bit map length error,want 1917.")
	}
}

func TestBloomFilter_Query(t *testing.T) {
	var index, trueNum, falseNum int
	bf, _ := MakeBloomFilter(0.01, 200)
	for i := 0; i < 100; i++ {
		bf.Add(binaryData(strconv.Itoa(i)))
	}
	resultSet := make([]bool, 51)
	for j := 0; j < 200; j += 4 {
		resultSet[index] = bf.Query(binaryData(strconv.Itoa(j)))
		index++
	}
	for _, value := range resultSet {
		if value == true {
			trueNum++
		} else {
			falseNum++
		}
	}
	if trueNum != 25 || falseNum != 26 {
		t.Error("Bloom filter query error too much.")
	}
}

func BenchmarkBloomFilter(b *testing.B) {
	testBF, _ := MakeBloomFilter(0.01, int64(b.N))
	for i := 0; i < b.N; i++ {
		testBF.Add(binaryData(rand.Int()))
	}
}

func TestPlugMessy(t *testing.T) {
	byteSet := []byte{1, 2, 3, 4, 5}
	bf, _ := MakeBloomFilter(0.01, 200)
	for i := 0; i < 100; i++ {
		bf.Add(binaryData(i))
	}
	fmt.Println(bf.mappingByHash(byteSet))
}
