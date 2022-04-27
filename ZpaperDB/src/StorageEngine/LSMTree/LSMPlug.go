package storage

// including :
// 1.sort interface of some data type.
// 2.Iterator interface.
// 2.bloom filter to improve read performance.
// 3.some typical hash func.
// 4.disk block cache to improve the efficiency of read.
// 5.RBTree.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	bitNum = 32 << (^uint(0) >> 63)
)

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

type IContainer interface {
	Iterator() IIterator
}

type IIterator interface {
	Valid() bool
	SeedToFirst()
	SeekToLast()
	Seek(target []byte) bool
	Next()
	Prev()
	key() []byte
	value() []byte
}

type Iterator struct {
	curIndex  int
	container *Container
}

func (i *Iterator) Valid() bool {
	if i.curIndex == len(i.container.container)-1 {
		return false
	}
	return true
}
func (i *Iterator) SeedToFirst() {
	i.curIndex = 0
}
func (i *Iterator) SeekToLast() {
	i.curIndex = len(i.container.container) - 1
}
func (i *Iterator) Seek(target []byte) bool {
	for bytes.Compare(i.container.container[i.curIndex].entry.key, target) != 0 {
		i.curIndex++
		if i.curIndex == len(i.container.container) {
			return false
		}
	}
	return true
}
func (i *Iterator) Next() {
	if i.curIndex < len(i.container.container)-1 {
		i.curIndex++
	}
}
func (i *Iterator) Prev() {
	i.curIndex--
}
func (i *Iterator) key() []byte {
	return i.container.container[i.curIndex].entry.key
}
func (i *Iterator) value() []byte {
	return i.container.container[i.curIndex].entry.value
}

type bloomFilter struct {
	mu          sync.Mutex
	ElementNum  int64
	ByteArray   []byte
	BitArrayLen int64
	HashNum     int
	HashFunc    []func([]byte) uint64
}

type filter struct {
	keyNum    uint
	bitMap    []byte
	bitMapLen uint
	hashNum   int
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
	hashFunc = []func([]byte) uint64{
		RSHash,
		BKDRHash,
		DJBHash,
		JSHash,
		SDBMHash,
		AdlerHash}
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

type RBTree struct {
	mu           *sync.Mutex
	root         *RBTreeNode
	pairs        []entry
	full         bool
	dataSize     int
	ssTableIndex int
}

type RBTreeNode struct {
	key    []byte
	value  []byte
	color  bool //true is red,false is black.
	left   *RBTreeNode
	right  *RBTreeNode
	parent *RBTreeNode
}

func (lsm *LSMTree) initRBTree() *RBTree {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	tree := new(RBTree)
	tree.full = false
	lsm.ssTableNum++
	tree.ssTableIndex = lsm.ssTableNum
	lsm.table.str = tree
	return tree
}

func (rb *RBTree) findInsertSite(key []byte, node *RBTreeNode) (*RBTreeNode, *RBTreeNode) {
	if bytes.Compare(key, node.key) == 1 {
		if node.right == nil {
			return node, node.right
		}
		rb.findInsertSite(key, node.right)
	} else {
		if node.left == nil {
			return node, node.left
		}
		rb.findInsertSite(key, node.left)
	}
	return nil, nil
}

func (rb *RBTree) reAdjustColor(cur *RBTreeNode) {
	var uncle *RBTreeNode
	parent := cur.parent
	pParent := cur.parent.parent
	if cur == cur.parent.left {
		uncle = cur.parent.right
	} else {
		uncle = cur.parent.left
	}
	if pParent == rb.root {
		parent.color = black
		uncle.color = black
		return
	}
	parent.color = black
	uncle.color = black
	pParent.color = red
	rb.reAdjustColor(pParent)
}

func (rb *RBTree) rightSignSpin(cur *RBTreeNode) {
	var tmpRoot *RBTreeNode
	parent := cur
	pParent := cur.parent
	if pParent == rb.root {
		parent.right = pParent
		pParent.color = red
		pParent.left = nil
		pParent.parent = parent
		rb.root = parent
		rb.root.color = black
		return
	} else {
		tmpRoot = pParent.parent
		if tmpRoot.left == pParent {
			parent.right = pParent
			pParent.color = red
			pParent.left = nil
			pParent.parent = parent
			tmpRoot.left = parent
			tmpRoot.left.color = black
			return
		} else {
			parent.right = pParent
			pParent.color = red
			pParent.left = nil
			pParent.parent = parent
			tmpRoot.right = parent
			tmpRoot.right.color = black
			return
		}
	}
}

func (rb *RBTree) leftSignSpin(cur *RBTreeNode) {
	var tmpRoot *RBTreeNode
	parent := cur
	pParent := cur.parent
	if pParent == rb.root {
		parent.left = rb.root
		pParent.color = red
		pParent.right = nil
		pParent.parent = parent
		rb.root = parent
		rb.root.color = black
		return
	} else {
		tmpRoot = pParent.parent
		if tmpRoot.left == pParent {
			parent.left = pParent
			pParent.color = red
			pParent.right = nil
			pParent.parent = parent
			tmpRoot.left = parent
			tmpRoot.left.color = black
			return
		} else {
			parent.left = pParent
			pParent.color = red
			pParent.right = nil
			pParent.parent = parent
			tmpRoot.right = parent
			tmpRoot.right.color = black
			return
		}
	}
}

func (rbn *RBTreeNode) insertData(key []byte, value []byte, parent *RBTreeNode) {
	rbn.key = key
	rbn.value = value
	rbn.parent = parent
	rbn.color = red
	return
}

func (rb *RBTree) addDataSize(key []byte, value []byte) {
	tmpDataPair := new(entry)
	tmpDataPair.key = key
	tmpDataPair.value = value
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, tmpDataPair)
	if err != nil {
		rb.addDataSize(key, value)
		return
	}
	rb.dataSize += buf.Len()
	if rb.dataSize > tableMaxSize {
		rb.full = true
	}
	return
}

func (rb *RBTree) insert(key []byte, value []byte) {
	var lsm *LSMTree
	var parent *RBTreeNode
	var uncle *RBTreeNode
	rb.addDataSize(key, value)
	err := lsm.writeRestoreLog(key, value, "insert")
	if err != nil {
		return
	}
	if rb.root == nil {
		rb.root = new(RBTreeNode)
		rb.root.key = key
		rb.root.value = value
		rb.root.color = black
		return
	}
	insertSite := new(RBTreeNode)
	parent, insertSite = rb.findInsertSite(key, rb.root)
	if parent.color == black {
		insertSite.insertData(key, value, parent)
		return
	}
	insertSite.insertData(key, value, parent)
	cur := insertSite
	pParent := parent.parent
	if parent.left == cur {
		uncle = parent.right
	} else {
		uncle = parent.left
	}
	if uncle != nil && uncle.color == red {
		rb.reAdjustColor(cur)
		return
	}
	if cur == parent.left && parent == pParent.left {
		rb.rightSignSpin(cur.parent)
	} else if cur == parent.right && parent == pParent.right {
		rb.leftSignSpin(cur.parent)
	} else if cur == parent.right && parent == pParent.left {
		rb.leftSignSpin(cur)
		cur.color = red
		rb.rightSignSpin(cur)
	} else if cur == parent.left && parent == pParent.right {
		rb.rightSignSpin(cur)
		cur.color = red
		rb.leftSignSpin(cur)
	}
	return
}

func (rbn *RBTreeNode) deleteLeafAdjust(parent *RBTreeNode) {
	if rbn.color == black {
		if rbn.left == nil && rbn.right == nil {
			if parent.color == red {
				parent.left = nil
				parent.color = black
				rbn.color = red
			} else {
				parent.left = nil
				rbn.color = red
				// in order to maintain number of black, have to adjust.
			}
		} else if rbn.left.color == red {
			parent.left = nil
			newParent := rbn.left
			if parent == parent.parent.left {
				parent.parent.left = newParent
			} else {
				parent.parent.right = newParent
			}
			newParent.parent = parent.parent
			newParent.color = parent.color
			newParent.left = parent
			newParent.right = rbn
			parent.color = black
			parent.parent = newParent
			rbn.parent = newParent
			rbn.left = nil
		} else if rbn.right.color == red {
			parent.left = nil
			newParent := rbn
			if parent == parent.parent.left {
				parent.parent.left = newParent
			} else {
				parent.parent.right = newParent
			}
			newParent.parent = parent.parent
			newParent.color = parent.color
			newParent.left = parent
			newParent.right.color = black
			parent.color = black
			parent.right = rbn.left
			parent.parent = newParent
			newParent.right.parent = parent
		}
	} else {
		parent.left = nil
		if parent == parent.parent.left {
			parent.parent.left = rbn
		} else {
			parent.parent.right = rbn
		}
		parent.right = rbn.left
		rbn.left.parent = parent
		rbn.left = parent
		rbn.parent = parent.parent
		parent.parent = rbn
		parent.color = red
		rbn.color = black
	}
}

func (rb *RBTree) delete(key []byte) bool {
	result := rb.root.findNode(key)
	if result.rb == nil {
		return false
	}
	parent := result.rb.parent
	if result.rb.left == nil && result.rb.right == nil { // deleted node is a leaf node.
		if result.rb.color == red {
			if result.rb == parent.left {
				parent.left = nil
			} else {
				parent.right = nil
			}
		} else {
			if result.rb == parent.left {
				bro := parent.right
				bro.deleteLeafAdjust(parent)
			} else {
				bro := parent.left
				bro.deleteLeafAdjust(parent)
			}
		}
	}
	if result.rb.left == nil || result.rb.right != nil {
		child := result.rb.right
		if result.rb == result.rb.parent.left {
			result.rb.parent.left = child
		} else {
			result.rb.parent.right = child
		}
		child.parent = result.rb.parent
		child.color = black
	} else if result.rb.left != nil || result.rb.right == nil {
		child := result.rb.left
		if result.rb == result.rb.parent.left {
			result.rb.parent.left = child
		} else {
			result.rb.parent.right = child
		}
		child.parent = result.rb.parent
		child.color = black
	}
	if result.rb.left != nil && result.rb.right != nil {
		tmpKey := result.rb.right.key
		result.rb.right.key = result.rb.key
		result.rb.key = tmpKey
		result.rb.value = result.rb.right.value
		rb.delete(key)
	}
	return true
}

func (rb *RBTree) changeValue(key []byte, newValue []byte) bool {
	result := rb.root.findNode(key)
	if result.rb == nil {
		return false
	} else {
		result.rb.value = newValue
		return true
	}
}

func (rb *RBTree) find(key []byte) *findResult {
	return rb.root.findNode(key)
}

func (rbn *RBTreeNode) findNode(key []byte) *findResult {
	if bytes.Compare(key, rbn.key) == 0 {
		return &findResult{rbn, nil}
	} else if bytes.Compare(key, rbn.key) == 1 {
		if rbn.right == nil {
			return nil
		}
		rbn.right.findNode(key)
	} else {
		if rbn.left == nil {
			return nil
		}
		rbn.left.findNode(key)
	}
	return nil
}

func (rb *RBTree) inorder(cur *RBTreeNode, num *int) {
	if cur == nil {
		return
	}
	rb.inorder(cur.left, num)
	rb.pairs[*num].key = cur.key
	rb.pairs[*num].value = cur.value
	*num++
	rb.inorder(cur.right, num)
}

func (lsm *LSMTree) setMemoryHash(key []byte, offSet int64, ssTableNum int) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	tmpMap := lsm.memoryHash[ssTableNum]
	(*tmpMap)[&key] = offSet
	return
}

func (lsm *LSMTree) encodingToBuf(instance *RBTree) (*bytes.Buffer, error) {
	var num *int
	var curSize int
	root := instance.root
	instance.inorder(root, num)
	buf := new(bytes.Buffer)
	for i := 0; i < len(instance.pairs); i++ {
		err := binary.Write(buf, binary.BigEndian, instance.pairs[i])
		if err != nil {
			return nil, err
		}
		curSize += buf.Len()
		if curSize > 4096 {
			lsm.setMemoryHash(instance.pairs[i+1].key, int64(curSize), instance.ssTableIndex)
		}
	}
	return buf, nil
}

func (lsm *LSMTree) writeToDisk(buf *bytes.Buffer) error {
	var tmpLen int
	var err, err1, err2 error
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	lsm.ssTableFile[lsm.ssTableNum], err = os.Create("SSTable" + strconv.Itoa(lsm.ssTableNum))
	if err != nil {
		return err
	}
	for i := 0; i < 4194304; i += tmpLen {
		tmpData := new([]byte)
		_, err1 = buf.Read(*tmpData)
		if err1 != nil {
			return err1
		}
		tmpLen, err2 = lsm.ssTableFile[lsm.ssTableNum].Write(*tmpData)
		if err2 != nil {
			return err2
		}
	}
	lsm.ssTableNum++
	return nil
}

/*
func (lsm *LSMTree) syncToDisk() {
	var wg sync.WaitGroup
	for {
		if lsm.table.fulled == true {
			wg.Add(1)
			go func() {
				buf, err := lsm.encodingToBuf(lsm.table.str)
				if err != nil {
					buf, err = lsm.encodingToBuf(lsm.table.str)
				}
				err1 := lsm.writeToDisk(buf)
				if err1 != nil {
					err1 = lsm.writeToDisk(buf)
				}
				wg.Done()
				lsm.memTable[i] = nil
			}()
		}
		wg.Wait()
		randTime := 200 + rand.Intn(randTimeSection)
		time.Sleep(time.Duration(randTime) * time.Millisecond)
	}
}
*/

func (lsm *LSMTree) writeRestoreLog(key []byte, value []byte, preFix string) error {
	tmpLogEntry := new(logEntry)
	tmpLogEntry.data.key = key
	tmpLogEntry.data.value = value
	tmpLogEntry.nowTime = time.Now()
	tmpLogEntry.logPrefix = preFix
	data, err := json.Marshal(tmpLogEntry)
	if err != nil {
		return err
	}
	_, err1 := lsm.writeAheadLog.Write(data)
	if err1 != nil {
		return err1
	}
	return nil
}

// return the start-end offset section of the key in memory hash and the segment number.
func (lsm *LSMTree) searchInSparseHash(key []byte) ([]int64, []int64, []int) {
	var start, end []int64
	var segment []int
	num := 0
	for i := lsm.ssTableNum; i > 0; i-- {
		var keys keySet
		keyNum := 0
		for hashKey := range *lsm.memoryHash[i] {
			keys[keyNum] = *hashKey
			keyNum++
		}
		sort.Sort(keys)
		index := sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], key) == 1
		})
		if index == 0 {
			start[num] = 0
			end[num] = (*lsm.memoryHash[i])[&keys[index]]
		} else if index == len(keys) {
			start[num] = (*lsm.memoryHash[i])[&keys[index-1]]
			end[num] = -1
		} else {
			start[num] = (*lsm.memoryHash[i])[&keys[index-1]]
			end[num] = (*lsm.memoryHash[i])[&keys[index]]
		}
		segment[num] = i
		num++
	}
	return start, end, segment
}

func (rb *RBTree) export() *[]pairs {
	data := make([]pairs, 10000)
	return &data
}
