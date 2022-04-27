package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

type memTable struct {
	str    underStr
	rwMu   *sync.RWMutex
	fulled bool
}

type underStr interface {
	insert(key, value []byte)
	delete(key []byte) bool
	find(key []byte) *findResult
	export() *[]pairs
}

type findResult struct {
	rb *RBTreeNode
	sl *listNode
}

type skipList struct {
	mu        *sync.RWMutex
	head      *listNode
	keyNum    int
	maxHeight uint8
	iter      *Container
}

type listNode struct {
	height uint8
	entry  *memEntry
	next   []*listNode
}

type memEntry struct {
	keyLen   int
	key      []byte
	keyType  byte // del(0x0) or add(0x1)
	valueLen int
	value    []byte
}

type Container struct {
	container []*listNode
}

func (c *Container) NewIterator() IIterator {
	i := new(Iterator)
	i.container = c
	return i
}

func (lsm *LSMTree) initSkipList() *skipList {
	list := new(skipList)
	list.head = new(listNode)
	list.head.entry = nil
	list.head.next = make([]*listNode, 12)
	list.iter = new(Container)
	return list
}

func (s *skipList) initSkipListNode(key []byte, value []byte) *listNode {
	node := new(listNode)
	node.entry = new(memEntry)
	node.entry.keyLen = len(key) + 1
	node.entry.valueLen = len(value)
	node.entry.key = key
	node.entry.value = value
	node.height = RandomHeight()
	node.next = make([]*listNode, node.height)
	return node
}

func RandomHeight() uint8 {
	source := rand.NewSource(time.Now().Unix())
	kBranching := 4
	height := 1
	for height < 12 && (int(source.Int63())&(kBranching-1) == 0) {
		height++
	}
	return uint8(height)
}

func (s *skipList) insert(key []byte, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var i, k uint8
	insertNode := s.initSkipListNode(key, value)
	tmpHeight := s.maxHeight - 1
	s.mu.Lock()
	s.keyNum++
	if insertNode.height > s.maxHeight {
		s.maxHeight = insertNode.height
	}
	s.mu.Unlock()
	start := s.head
	changePoints := make([]*listNode, insertNode.height+tmpHeight)
	for j := insertNode.height - 1; j > 0; j-- {
		if s.head.next[j] == nil {
			changePoints[j] = insertNode
			continue
		}
		if bytes.Compare(s.head.next[j].entry.key, key) == 1 {
			changePoints[j] = insertNode
			changePoints[j+insertNode.height] = s.head.next[j]
		} else {
			for next := start.next[j]; next != nil; start = next {
				if bytes.Compare(next.next[j].entry.key, key) == 1 {
					changePoints[j] = next
					changePoints[j+insertNode.height] = next.next[j]
				}
			}
		}
	}
	for i = 0; i < insertNode.height-1; i++ {
		changePoints[i].next[i] = insertNode
	}
	for k = 0; k < tmpHeight; k++ {
		insertNode.next[k] = changePoints[k+insertNode.height]
	}
}

func (s *skipList) find(key []byte) *findResult {
	level := s.maxHeight - 1
	start := s.head.next[level]
	for {
		next := start.next[level]
		if next == nil {
			level--
			next = start.next[level]
		}
		if bytes.Compare(next.entry.key, key) == -1 {
			start = next
		} else {
			if level == 0 {
				return &findResult{nil, next}
			} else {
				level--
			}
		}
	}

}

func (s *skipList) delete(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	updatePoint := make([]*listNode, s.maxHeight-1)
	cur := s.head
	for i := cur.height - 1; i > 0; i-- {
		for bytes.Compare(cur.next[i].entry.key, key) == -1 {
			cur = cur.next[i]
		}
		updatePoint[i] = cur
	}
	cur = cur.next[0]
	if bytes.Compare(cur.entry.key, key) != 0 {
		return false
	} else {
		for j := 0; uint8(j) <= s.maxHeight-1; j++ {
			if updatePoint[j].next[j] != cur {
				break
			}
			updatePoint[j].next[j] = cur.next[j]
		}
	}
	for s.maxHeight-1 > 0 && s.head.next[s.maxHeight-1] == nil {
		s.maxHeight--
	}
	s.keyNum--
	return true
}

func (s *skipList) export() *[]pairs {
	i := 0
	pairData := make([]pairs, 200)
	start := s.head
	s.iter.container = make([]*listNode, s.keyNum)
	for next := start.next[0]; next != nil; start = next {
		s.iter.container[i] = next
	}
	i = 0
	iter := s.iter.NewIterator()
	for ; iter.Valid(); iter.Next() {
		tmpEntry := new(pairs)
		tmpEntry.key = iter.key()
		tmpEntry.value = iter.value()
		tmpEntry.keyLen = uint(len(iter.key()))
		tmpEntry.valueLen = uint(len(iter.value()))
		pairData[i] = *tmpEntry
		i++
	}
	return &pairData
}
