package storage

import (
	"bytes"
	"math/rand"
	"time"
)

type skipList struct {
	head      *listNode
	maxHeight uint8
}

type listNode struct {
	height uint8
	entry  *dataEntry
	next   []*listNode
}

type dataEntry struct {
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

func (s *skipList) initSkipList() *skipList {
	list := new(skipList)
	list.head = new(listNode)
	list.head.entry = nil
	list.head.next = make([]*listNode, 12)

	return list
}

func (s *skipList) initSkipListNode(key []byte, value []byte) *listNode {
	node := new(listNode)
	node.entry = new(dataEntry)
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
	var i, k uint8
	insertNode := s.initSkipListNode(key, value)
	tmpHeight := s.maxHeight - 1
	if insertNode.height > s.maxHeight {
		s.maxHeight = insertNode.height
	}
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

func (s *skipList) find(key []byte) *listNode {
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
				return next
			} else {
				level--
			}
		}
	}
}
