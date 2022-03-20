package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// RBTree act as a LSMTree memoryTree basic workflow:
// While writing in, put the data in memory self-balancing tree(Red-Black Tree),
// this memoryTree called memoryTable,when memoryTable greater than a certain
// threshold size(usually a few megabytes),write it in disk as a SSTable file.
// Because memoryTree has been maintained key-value sorted by key, so write disk
// will be efficient.New SSTable file has been the newest part of the database system.
// While SSTable writing in disk,write request will insert into new RBtree instance.
// In order to deal with write request, first try to search key in memory table,
// then is the newest section of the disk file, the following is more old disk file.
// And so on, until find the target data(or nil).Background process will perform
// merge and compress periodically,for perform multi-sectionDiskFile.And discard
// old data that has been overwritten or deleted.
const (
	red                = true
	black              = false
	tableMaxSize       = 4194304 // memory table max size = 4MB
	fpp                = 0.05    // for the time false positive probability being as 0.05
	exceptedInsertions = 10000   // for the time excepted insertions being as 10000
)

// RBTree nature :
// Every node's color is red or black.
// Root node's color is black.
// If a node is red, then its children nodes is black.
// For every node, the path from current node to
// its descendant leaf nodes, all include an equal number black node.
// All of above condition is in order to ensure that
// the longest path no more than double of the shortest path.

type LSMTree struct {
	mu         *sync.Mutex
	memoryTree []*RBTree
	treeNum    int
	memoryHash []*map[*[]byte]int64
	dataFile   []*os.File
	ssTableNum int
	restoreLog *os.File
	bf         *bloomFilter
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

type entry struct {
	key       []byte
	value     []byte
	tombstone bool
}

type logEntry struct {
	logPrefix  string
	nowTime    time.Time
	curTreeNum int
	data       *entry
}

type WriteArgs struct {
	Key   []byte
	Value []byte
}

type WriteReply struct {
	Succeed bool
	err     error
}

type ReadArgs struct {
	Key []byte
}

type ReadReply struct {
	Value []byte
	Found bool
	err   error
}

func (lsm *LSMTree) initLSMTree() (*LSMTree, error) {
	var err error
	tree := new(LSMTree)
	tree.memoryTree = make([]*RBTree, 4)
	tree.dataFile = make([]*os.File, 10)
	tree.memoryHash = make([]*map[*[]byte]int64, 100)
	for i := 0; i < 100; i++ {
		tmpMap := make(map[*[]byte]int64)
		tree.memoryHash[i] = &tmpMap
	}
	tree.bf, err = MakeBloomFilter(fpp, exceptedInsertions)
	if err != nil {
		return tree, err
	}
	return tree, nil
}

func (lsm *LSMTree) initRBTree() *RBTree {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	tree := new(RBTree)
	tree.full = false
	lsm.ssTableNum++
	tree.ssTableIndex = lsm.ssTableNum
	lsm.memoryTree[lsm.treeNum] = tree
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

func (rb *RBTree) insert(key []byte, value []byte) error {
	var lsm *LSMTree
	var uncle *RBTreeNode
	rb.addDataSize(key, value)
	err := lsm.writeRestoreLog(key, value, "insert")
	if err != nil {
		return err
	}
	go lsm.bf.Add(key)
	if rb.root == nil {
		rb.root = new(RBTreeNode)
		rb.root.key = key
		rb.root.value = value
		rb.root.color = black
		return nil
	}
	parent, insertSite := rb.findInsertSite(key, rb.root)
	if parent.color == black {
		insertSite.insertData(key, value, parent)
		return nil
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
		return nil
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
	return nil
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

func (rb *RBTree) deleteNode(key []byte) bool {
	deleted, sign := rb.root.search(key)
	if sign == false {
		return false
	}
	parent := deleted.parent
	if deleted.left == nil && deleted.right == nil { // deleted node is a leaf node.
		if deleted.color == red {
			if deleted == parent.left {
				parent.left = nil
			} else {
				parent.right = nil
			}
		} else {
			if deleted == parent.left {
				bro := parent.right
				bro.deleteLeafAdjust(parent)
			} else {
				bro := parent.left
				bro.deleteLeafAdjust(parent)
			}
		}
	}
	if deleted.left == nil || deleted.right != nil {
		child := deleted.right
		if deleted == deleted.parent.left {
			deleted.parent.left = child
		} else {
			deleted.parent.right = child
		}
		child.parent = deleted.parent
		child.color = black
	} else if deleted.left != nil || deleted.right == nil {
		child := deleted.left
		if deleted == deleted.parent.left {
			deleted.parent.left = child
		} else {
			deleted.parent.right = child
		}
		child.parent = deleted.parent
		child.color = black
	}
	if deleted.left != nil && deleted.right != nil {
		tmpKey := deleted.right.key
		deleted.right.key = deleted.key
		deleted.key = tmpKey
		deleted.value = deleted.right.value
		rb.deleteNode(key)
	}
	return true
}

func (rb *RBTree) changeValue(key []byte, newValue []byte) bool {
	changeNode, sign := rb.root.search(key)
	if sign == false {
		return sign
	} else {
		changeNode.value = newValue
		return true
	}
}

func (rbn *RBTreeNode) search(key []byte) (*RBTreeNode, bool) {
	if bytes.Compare(key, rbn.key) == 0 {
		return rbn, true
	} else if bytes.Compare(key, rbn.key) == 1 {
		if rbn.right == nil {
			return nil, false
		}
		rbn.right.search(key)
	} else {
		if rbn.left == nil {
			return nil, false
		}
		rbn.left.search(key)
	}
	return nil, false
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
	lsm.dataFile[lsm.ssTableNum], err = os.Create("SSTable" + strconv.Itoa(lsm.ssTableNum))
	if err != nil {
		return err
	}
	for i := 0; i < 4194304; i += tmpLen {
		tmpData := new([]byte)
		_, err1 = buf.Read(*tmpData)
		if err1 != nil {
			return err1
		}
		tmpLen, err2 = lsm.dataFile[lsm.ssTableNum].Write(*tmpData)
		if err2 != nil {
			return err2
		}
	}
	lsm.ssTableNum++
	return nil
}

func (lsm *LSMTree) syncToDisk() {
	var wg sync.WaitGroup
	for {
		for i := 0; i < lsm.treeNum-1; i++ {
			if lsm.memoryTree[i].full == true {
				wg.Add(1)
				go func() {
					buf, err := lsm.encodingToBuf(lsm.memoryTree[i])
					if err != nil {
						buf, err = lsm.encodingToBuf(lsm.memoryTree[i])
					}
					err1 := lsm.writeToDisk(buf)
					if err1 != nil {
						err1 = lsm.writeToDisk(buf)
					}
					wg.Done()
					lsm.memoryTree[i] = nil
				}()
			}
			wg.Wait()
			randTime := 200 + rand.Intn(300)
			time.Sleep(time.Duration(randTime) * time.Millisecond)
		}
	}
}

func (lsm *LSMTree) writeRestoreLog(key []byte, value []byte, preFix string) error {
	tmpLogEntry := new(logEntry)
	tmpLogEntry.data.key = key
	tmpLogEntry.data.value = value
	tmpLogEntry.nowTime = time.Now()
	tmpLogEntry.curTreeNum = lsm.treeNum
	tmpLogEntry.logPrefix = preFix
	data, err := json.Marshal(tmpLogEntry)
	if err != nil {
		return err
	}
	_, err1 := lsm.restoreLog.Write(data)
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

func MakeLSMTree() (*LSMTree, error) {
	var lsmTree *LSMTree
	var err error
	lsmTree, err = lsmTree.initLSMTree()
	if err != nil {
		return nil, err
	}
	lsmTree.treeNum = 0
	lsmTree.memoryTree[lsmTree.treeNum] = lsmTree.initRBTree()
	go lsmTree.syncToDisk()
	return lsmTree, nil
}

func (lsm *LSMTree) Put(args *WriteArgs, reply *WriteReply) error {
	memoryTree := lsm.memoryTree[lsm.treeNum]
	memoryTree.mu.Lock()
	defer memoryTree.mu.Unlock()
	if memoryTree.full != true {
		err := memoryTree.insert(args.Key, args.Value)
		if err != nil {
			reply.Succeed = false
			reply.err = err
			return err
		}
		reply.Succeed = true
		reply.err = nil
	} else {
		lsm.mu.Lock()
		lsm.treeNum++
		lsm.memoryTree[lsm.treeNum] = lsm.initRBTree()
		lsm.mu.Unlock()
		err1 := lsm.Put(args, reply)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (lsm *LSMTree) Get(args *ReadArgs, reply *ReadReply) error {
	memoryTree := lsm.memoryTree[lsm.treeNum]
	if lsm.bf.Query(args.Key) == false {
		reply.Found = false
		return nil
	}
	tmpNode, found := memoryTree.root.search(args.Key)
	if found == true {
		reply.Value = tmpNode.value
		reply.Found = true
		reply.err = nil
	} else {
		start, end, segNum := lsm.searchInSparseHash(args.Key)
		for i := 0; i < len(start); i++ {
			if end[i] == -1 {
				section := io.NewSectionReader(lsm.dataFile[segNum[i]], start[i], 1024)
				end[i] = section.Size()
			}
			binaryData := make([]byte, end[i]-start[i])
			_, err := lsm.dataFile[segNum[i]].ReadAt(binaryData, start[i])
			if err != nil {
				reply.Found = false
				reply.err = err
				return err
			} else {
				buf := new(bytes.Buffer)
				buf.Write(binaryData)
				var decodingData []entry
				err1 := binary.Read(buf, binary.BigEndian, decodingData)
				if err1 != nil {
					reply.Found = false
					reply.err = err1
					return err1
				}
				var keys keySet
				for index, pair := range decodingData {
					keys[index] = pair.key
				}
				keyIndex := sort.Search(len(keys), func(i int) bool {
					return bytes.Compare(keys[i], args.Key) == 0
				})
				if bytes.Compare(decodingData[keyIndex].key, args.Key) == 0 {
					reply.Found = true
					reply.Value = decodingData[keyIndex].value
					reply.err = nil
					return nil
				} else {
					continue
				}
			}
		}
		reply.Found = false
		return nil
	}
	return nil
}
