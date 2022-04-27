package storage

import (
	"bytes"
	"encoding/binary"
	"ini"
	"io"
	"log"
	"os"
	"sort"
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
	red          = true
	black        = false
	tableMaxSize = 4194304 // memory table max size = 4MB
	//fpp                = 0.05    // for the time false positive probability being as 0.05
	//exceptedInsertions = 10000   // for the time excepted insertions being as 10000
	randTimeSection = 200 // ms/second
)

// Skip list nature:
// skip list is a multilevel,ordered list structure.
// Every list node save many pointers point to many other nodes.
// Average complexity is O(logN).

// RBTree nature :
// Every node's color is red or black.
// Root node's color is black.
// If a node is red, then its children nodes is black.
// For every node, the path from current node to
// its descendant leaf nodes, all include an equal number black node.
// All of above condition is in order to ensure that
// the longest path no more than double of the shortest path.

type LSMTree struct {
	mu            *sync.Mutex
	table         *memTable
	memoryHash    []*map[*[]byte]int64
	ssTableFile   []*os.File
	ssTableNum    int
	levelNum      []uint8
	compress      *compaction
	writeAheadLog *os.File
}

type entry struct {
	key       []byte
	value     []byte
	tombstone bool
}

type logEntry struct {
	logPrefix string
	nowTime   time.Time
	data      *entry
}

type AddArgs struct {
	Key     []byte
	KeyType byte
	Value   []byte
}

type AddReply struct {
	err error
}

type GetArgs struct {
	Key []byte
}

type GetReply struct {
	Value []byte
	Found bool
	err   error
}

type DeleteArgs struct {
	Key []byte
}

type DeleteReply struct {
	Deleted bool
	Found   bool
	err     error
}

func (lsm *LSMTree) initLSMTree() *LSMTree {
	cfg, _ := ini.Load("lsm.ini")
	memType := cfg.Section("MemTable").Key("memoryTableType").String()
	tree := new(LSMTree)
	tree.ssTableFile = make([]*os.File, 10)
	tree.levelNum = make([]uint8, 20)
	tree.writeAheadLog, _ = os.Create("WAL")
	tree.memoryHash = make([]*map[*[]byte]int64, 100)
	for i := 0; i < 100; i++ {
		tmpMap := make(map[*[]byte]int64)
		tree.memoryHash[i] = &tmpMap
	}
	if memType == "skipList" {
		tree.table.str = tree.initSkipList()
	} else {
		tree.table.str = tree.initRBTree()
	}
	return tree
}

func MakeLSMTree() (*LSMTree, error) {
	var lsmTree *LSMTree
	var err error
	lsmTree = lsmTree.initLSMTree()
	if err != nil {
		return nil, err
	}
	go lsmTree.MakeCompaction()
	return lsmTree, nil
}

func (lsm *LSMTree) RBAdd(args *AddArgs, reply *AddReply) error {
	memoryStr := lsm.table
	memoryStr.rwMu.Lock()
	defer memoryStr.rwMu.Unlock()
	if memoryStr.fulled != true {
		memoryStr.str.insert(args.Key, args.Value)
		reply.err = nil
	} else {
		lsm.mu.Lock()
		lsm.table.str = lsm.initRBTree()
		lsm.mu.Unlock()
		err1 := lsm.RBAdd(args, reply)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (lsm *LSMTree) RBGet(args *GetArgs, reply *GetReply) error {
	memoryTree := lsm.table.str
	tmpNode := memoryTree.find(args.Key)
	if tmpNode != nil {
		reply.Value = tmpNode.rb.value
		reply.Found = true
		reply.err = nil
	} else {
		start, end, segNum := lsm.searchInSparseHash(args.Key)
		for i := 0; i < len(start); i++ {
			if end[i] == -1 {
				section := io.NewSectionReader(lsm.ssTableFile[segNum[i]], start[i], 1024)
				end[i] = section.Size()
			}
			binaryData := make([]byte, end[i]-start[i])
			_, err := lsm.ssTableFile[segNum[i]].ReadAt(binaryData, start[i])
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

func (lsm *LSMTree) RBDelete(args *DeleteArgs, reply *DeleteReply) error {
	result := lsm.table.str.find(args.Key)
	if result.rb != nil {
		lsm.table.str.delete(args.Key)
	} else {
		start, end, segNum := lsm.searchInSparseHash(args.Key)
		for i := 0; i < len(start); i++ {
			if end[i] == -1 {
				section := io.NewSectionReader(lsm.ssTableFile[segNum[i]], start[i], 1024)
				end[i] = section.Size()
			}
			binaryData := make([]byte, end[i]-start[i])
			_, err := lsm.ssTableFile[segNum[i]].ReadAt(binaryData, start[i])
			if err != nil {
				log.Fatal(err)
			} else {
				buf := new(bytes.Buffer)
				buf.Write(binaryData)
				var decodingData []entry
				err1 := binary.Read(buf, binary.BigEndian, decodingData)
				if err1 != nil {
					log.Fatal(err1)
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
					reply.err = nil
					decodingData[keyIndex].tombstone = true
					buf1 := new(bytes.Buffer)
					err2 := binary.Write(buf1, binary.BigEndian, decodingData)
					if err2 != nil {
						log.Fatal(err2)
					}
					var tmpData []byte
					_, err3 := buf.Read(tmpData)
					if err3 != nil {
						log.Fatal(err3)
					}
					_, err4 := lsm.ssTableFile[segNum[i]].WriteAt(tmpData, start[i])
					if err4 != nil {
						log.Fatal(err4)
					}
					reply.Deleted = true
					return nil
				} else {
					continue
				}
			}
		}
	}
	return nil
}
