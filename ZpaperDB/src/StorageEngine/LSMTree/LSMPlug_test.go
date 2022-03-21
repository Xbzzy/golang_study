package storage

import "testing"

func TestAdd(t *testing.T) {
	bf, _ := MakeBloomFilter(0.1, 100)
	bf.add(nil)
}
