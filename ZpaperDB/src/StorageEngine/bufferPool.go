package StorageEngine

import (
	"bufio"
	"errors"
	"os"
	"syscall"
)

const (
	BufferSize = 1048576
	MaxPageInBuffer = 100
)

type isDirty bool
type BufferPool struct {
	dirtyPage map[*BTreeNode]isDirty
	hashPage map[string]*ControlPage
	bufferPageNum uint32
	*FreeList
	*FlushList
	*LRUList
	*diskOperation
}

type ControlPage struct {
	fileName string
	pageType string //include "index" "data"
	cachePage *BTreeNode
	offset uint64
	prePage *ControlPage
	nextPage *ControlPage
}

type FreeList struct {
	count uint32
	headPage *ControlPage
	tailPage *ControlPage
}

type FlushList struct {
	count uint32
	headDirtyPage *ControlPage
	tailDirtyPage *ControlPage
}

type LRUList struct {
	maxBlockNum uint32
	currentBlockNum uint32
	headPage *ControlPage
	tailPage *ControlPage
}

func (b *BufferPool) InitBufferPool() *BufferPool {
	bufPool := new(BufferPool)
	bufPool.FreeList = new(FreeList)
	bufPool.InitFullEmptyFreeList(bufPool.FreeList)
	bufPool.FlushList = new(FlushList)
	bufPool.LRUList = new(LRUList)
	bufPool.LRUList.maxBlockNum = MaxPageInBuffer
	return bufPool
}

func (b *BufferPool) InitFullEmptyFreeList(list *FreeList)  {
	list.headPage=new(ControlPage)
	list.tailPage=list.headPage
	list.count=1
	for i:=0;i<MaxPageInBuffer-1;i++ {
		Tmp:=new(ControlPage)
		Tmp.prePage=nil
		Tmp.nextPage=list.headPage
		list.headPage.prePage=Tmp
		list.headPage=Tmp
	}
	return
}

func (b *BTree) TransferToControlPage(node *BTreeNode) *ControlPage {
	controlPage:=new(ControlPage)
	switch {
	case node.NodeType=="index":
		controlPage.pageType="index"
	case node.NodeType=="leaf":
		controlPage.pageType="data"
	}
	controlPage.cachePage=node
	controlPage.offset=b.diskMap[node]
	controlPage.fileName=b.FileName
	return controlPage
}

func (b *BufferPool) ReadNodeToBuffer(node *BTreeNode,tree *BTree) error {
	var TmpDiskNode *diskNode
	var err3 error
	TmpFile,err:=os.OpenFile(tree.FileName,syscall.O_RDWR,0666)
	if err!=nil {
		return err
	}
	data:=make([]byte,pageSize)
	defer TmpFile.Close()
	bufRead:=bufio.NewReaderSize(TmpFile,BufferSize)
	offset:=tree.diskMap[node]
	_, err1 := TmpFile.Seek(int64(offset),0)
	if err1!=nil {
		return err1
	}
	_, err2 := bufRead.Read(data)
	if err2 != nil {
		return err2
	}
	TmpDiskNode,err3=TmpDiskNode.DecodingJsonToDiskNode(data)
	if err3!=nil {
		return err3
	}
	node=tree.RefactorDiskNode(TmpDiskNode)
	node.HasLoaded=true
	err4 := b.DeleteUsedPageFromFreeList()
	if err4 != nil {
		return err4
	}
	TmpPage:=tree.TransferToControlPage(node)
	err5 := b.JoinToLRUList(TmpPage)
	if err5 !=nil {
		return err5
	}
	return nil
}

func (b *BufferPool) DeleteUsedPageFromFreeList() error {
	if b.FreeList.count==0 {
		return errors.New("delete: free list is nil")
	}
	Tmp:=b.FreeList.tailPage
	b.FreeList.tailPage=Tmp.prePage
	Tmp.prePage.nextPage=nil
	Tmp.prePage=nil
	return nil
}

func (b *BufferPool) JoinToLRUList(page *ControlPage) error {
	if b.LRUList.currentBlockNum==100 {
		return errors.New("LRUList: the list is full")
	}
	page.prePage=nil
	page.nextPage=b.LRUList.headPage
	b.LRUList.headPage.prePage=page
	b.LRUList.headPage=page
	b.LRUList.currentBlockNum++
	return nil
}

func (b *BufferPool) EliminateFromLRUList() {
	b.LRUList.tailPage.prePage.nextPage=nil
	b.LRUList.tailPage.prePage=nil
}










