package storage

// include 1.sort interface of some data type.
// 2.bloom filter to improve read performance.
// 3.some typical hash func.
// 4.stratified SSTable compressor to keep lower storage costs.
// 5.skip list define to keep the choosability of memory table.
import (
	"bytes"
	"errors"
	"math"
	"sync"
)

const bitNum = 32 << (^uint(0) >> 63)

type keySet [][]byte

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
	ElementNum  int64
	ByteArray   []byte
	BitArrayLen int64
	HashNum     int
	HashFunc    []func([]byte) uint64
}

func RSHash(key []byte) uint64 {
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

func BKDRHash(key []byte) uint64 {
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

func DJBHash(key []byte) uint64 {
	var hash uint64 = 5381
	for i := 0; i < len(key); i++ {
		hash = (hash << 5) + hash + uint64(key[i])
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func JSHash(key []byte) uint64 {
	var hash uint64 = 1315423911
	for i := 0; i < len(key); i++ {
		hash ^= (hash << 5) + uint64(key[i]) + (hash >> 2)
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func SDBMHash(key []byte) uint64 {
	var hash uint64
	for i := 0; i < len(key); i++ {
		hash = hash*65599 + uint64(key[i])
		// hash = uint64(key[i])+(hash<<6)+(hash<<16)-hash
	}
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func AdlerHash(key []byte) uint64 {
	var s1 uint64 = 1
	var s2 uint64
	for i := 0; i < len(key); i++ {
		s1 += uint64(key[i])
		s2 += s1
	}
	hash := s2*65536 + s1
	if bitNum == 32 {
		return hash & 0x7FFFFFFF
	}
	return hash & 0x7FFFFFFFFFFFFFFF
}

func (bf *bloomFilter) optimalHashFuncNum(expectedInsertions int64) int {
	left := float64(bf.BitArrayLen / expectedInsertions)
	right := math.Ln2
	return int(math.Max(1, left*right))
}

func (bf *bloomFilter) optimalBitArrayLen(fpp float64, expectedInsertions int64) uint64 {
	negN := -expectedInsertions
	up := float64(negN) * math.Log(fpp)
	down := math.Ln2 * math.Ln2
	return uint64(up / down)
}

func initHashFunc(hashFunc []func([]byte) uint64) {
	hashFunc[0] = RSHash
	hashFunc[1] = BKDRHash
	hashFunc[2] = DJBHash
	hashFunc[3] = JSHash
	hashFunc[4] = SDBMHash
	hashFunc[5] = AdlerHash
	return
}

func (bf *bloomFilter) mappingByHash(key []byte) []uint64 {
	result := make([]uint64, bf.HashNum)
	for i := 0; i < bf.HashNum; i++ {
		tmpResult := bf.HashFunc[i](key)
		result[i] = tmpResult % uint64(bf.BitArrayLen)
	}
	return result
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
	bf.BitArrayLen = int64(bf.optimalBitArrayLen(fpp, expectedInsertions))
	bf.ByteArray = make([]byte, (bf.BitArrayLen>>3)+1)
	bf.HashNum = bf.optimalHashFuncNum(expectedInsertions)
	bf.HashFunc = make([]func([]byte) uint64, 6)
	initHashFunc(bf.HashFunc)
	return bf, nil
}

func (bf *bloomFilter) changeBit(site uint64) {
	if site == 0 {
		return
	}
	byteSite := site >> 3
	bitSite := site & 0x07
	if bitSite == 0 {
		byteSite -= 1
		bitSite = 8
	}
	bf.ByteArray[byteSite] |= 128 >> (bitSite - 1)
	return
}

func (bf *bloomFilter) checkBitSite(site uint64) bool {
	byteSite := site >> 3
	bitSite := site & 0x07
	if bitSite == 0 {
		byteSite -= 1
		bitSite = 8
	}
	if bf.ByteArray[byteSite]&(128>>(bitSite-1)) == 128>>(bitSite-1) {
		return true
	}
	return false
}

func (bf *bloomFilter) Add(key []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.ElementNum++
	if bf.HashNum > 6 {
		bf.HashNum = 6
	}
	site := bf.mappingByHash(key)
	for i := 0; i < len(site); i++ {
		bf.changeBit(site[i])
	}
	return
}

func (bf *bloomFilter) Query(key []byte) bool {
	site := bf.mappingByHash(key)
	for i := 0; i < len(site); i++ {
		if bf.checkBitSite(site[i]) == true {
			continue
		} else {
			return false
		}
	}
	return true
}
