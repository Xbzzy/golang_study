package storage

// include 1.sort interface of some data type.
// 2.bloom filter to improve read performance.
// 3.some typical hash func.
// 4.stratified SSTable compressor to keep lower storage costs.
import (
	"bytes"
	"errors"
	"math"
	"sync"
)

const bitNum = 32 << (^uint(0) >> 63)

type keySet [][]byte

type HashFunctions struct{}

func (k keySet) Len() int {
	return len(k)
}

func (k keySet) Less(i, j int) bool {
	if bytes.Compare(k[i], k[j]) == -1 {
		return true
	} else {
		return false
	}
}

func (k keySet) Swap(i, j int) {
	tmpArray := k[i]
	k[i] = k[j]
	k[j] = tmpArray
}

type bloomFilter struct {
	mu          sync.Mutex
	elementNum  int64
	bitArray    []byte
	bitArrayLen int64
	hashNum     int64
	hashFunc    *HashFunctions
}

func (h *HashFunctions) RSHash(key []byte) uint64 {
	var a uint64 = 63689
	var b uint64 = 378551
	var hash uint64
	for i := 0; i < len(key); i++ {
		hash = hash*a + uint64(key[i])
		a *= b
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (h *HashFunctions) BKDRHash(key []byte) uint64 {
	var hash uint64
	var seed uint64 = 131313
	for i := 0; i < len(key); i++ {
		hash = hash*seed + uint64(key[i])
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (h *HashFunctions) DJBHash(key []byte) uint64 {
	var hash uint64 = 5381
	for i := 0; i < len(key); i++ {
		hash = (hash << 5) + hash + uint64(key[i])
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (h *HashFunctions) JSHash(key []byte) uint64 {
	var hash uint64 = 1315423911
	for i := 0; i < len(key); i++ {
		hash ^= (hash << 5) + uint64(key[i]) + (hash >> 2)
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (h *HashFunctions) SDBMHash(key []byte) uint64 {
	var hash uint64
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (bf *bloomFilter) optimalHashFuncNum(expectedInsertions int64) int64 {
	left := float64(bf.bitArrayLen / expectedInsertions)
	right := math.Ln2
	return int64(math.Max(1, left*right))
}

func (bf *bloomFilter) optimalBitArrayLen(fpp float64, expectedInsertions int64) int64 {
	negN := -expectedInsertions
	up := float64(negN) * math.Log(fpp)
	down := math.Ln2 * math.Ln2
	return int64(up / down)
}

func MakeBloomFilter(fpp float64, expectedInsertions int64) (*bloomFilter, error) {
	switch {
	case fpp < 0.0:
		return nil, errors.New("false positive probability must be > 0.0")
	case fpp > 1.0:
		return nil, errors.New("false positive probability must be < 1.0")
	case expectedInsertions <= 0:
		return nil, errors.New("excepted insertions must be >= 0")
	}
	bf := new(bloomFilter)
	bf.bitArrayLen = bf.optimalBitArrayLen(fpp, expectedInsertions)
	bf.bitArray = make([]uint8, bf.bitArrayLen)
	bf.hashNum = bf.optimalHashFuncNum(expectedInsertions)
	return bf, nil
}

func (bf *bloomFilter) add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.elementNum++

}

func (bf *bloomFilter) query(key []byte) bool {
	return true
}
