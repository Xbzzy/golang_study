package storage

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
	DirtyPage map[*BTreeNode]isDirty
	HashPage map[string]*ControlPage
	BufferPageNum uint32
	*FreeList
	*FlushList
	*LRUList
	*diskOperation
}

type ControlPage struct {
	FileName string
	PageType string //include "index" "data"
	CachePage *BTreeNode
	Offset uint64
	PreFreePage *ControlPage
	NextFreePage *ControlPage
	PreFlushPage *ControlPage
	NextFlushPage *ControlPage
	PreLRUPage *ControlPage
	NextLRUPage *ControlPage
}

type FreeList struct {
	Count uint32
	HeadPage *ControlPage
	TailPage *ControlPage
}

type FlushList struct {
	Count uint32
	HeadDirtyPage *ControlPage
	TailDirtyPage *ControlPage
}

type LRUList struct {
	MaxBlockNum uint32
	CurrentBlockNum uint32
	HeadPage *ControlPage
	TailPage *ControlPage
}

func (b *BufferPool) InitBufferPool() {
	dp:=make(map[*BTreeNode]isDirty)
	b.DirtyPage=dp
	hp:=make(map[string]*ControlPage)
	b.HashPage=hp
	b.FreeList = new(FreeList)
	b.InitFullEmptyFreeList(b.FreeList)
	b.FlushList = new(FlushList)
	b.LRUList = new(LRUList)
	b.LRUList.MaxBlockNum = MaxPageInBuffer
	return
}

func (b *BufferPool) InitFullEmptyFreeList(list *FreeList)  {
	list.HeadPage=new(ControlPage)
	list.TailPage=list.HeadPage
	list.Count=MaxPageInBuffer
	for i:=0;i<MaxPageInBuffer-1;i++ {
		Tmp:=new(ControlPage)
		Tmp.PreFreePage=nil
		Tmp.NextFreePage=list.HeadPage
		list.HeadPage.PreFreePage=Tmp
		list.HeadPage=Tmp
	}
	return
}

func (b *BufferPool) JoinFreeList(page *ControlPage) error {
	if b.FreeList.Count==MaxPageInBuffer {
		return errors.New("FreeList: list is full")
	}
	b.FreeList.TailPage.NextFreePage=page
	page.PreFreePage=b.FreeList.TailPage
	b.FreeList.Count++
	return nil
}

func (b *BufferPool) DeleteUsedPageFromFreeList() error {
	if b.FreeList.Count==0 {
		return errors.New("delete: free list is nil")
	}
	Tmp:=b.FreeList.TailPage
	b.FreeList.TailPage=Tmp.PreFreePage
	Tmp.PreFreePage.NextFreePage=nil
	Tmp.PreFreePage=nil
	b.FreeList.Count--
	return nil
}

func (b *BufferPool) JoinLRUList(page *ControlPage) error {
	if b.LRUList.CurrentBlockNum==100 {
		return errors.New("LRUList: the list is full")
	}
	page.PreLRUPage=nil
	page.NextLRUPage=b.LRUList.HeadPage
	b.LRUList.HeadPage.PreLRUPage=page
	b.LRUList.HeadPage=page
	b.LRUList.CurrentBlockNum++
	return nil
}

func (b *BufferPool) UpdateLRUList(page *ControlPage) {
	page.PreLRUPage.NextLRUPage=page.NextLRUPage
	page.NextLRUPage.PreLRUPage=page.PreLRUPage
	page.PreLRUPage=nil
	page.NextLRUPage=b.LRUList.HeadPage
	b.LRUList.HeadPage=page
}

func (b *BufferPool) EliminateFromLRUList() {
	b.LRUList.TailPage.PreLRUPage.NextLRUPage=nil
	b.LRUList.TailPage.PreLRUPage=nil
	b.LRUList.TailPage=b.LRUList.TailPage.PreLRUPage
	b.LRUList.CurrentBlockNum--
}

func (b *BufferPool) EmptyFlushListTail()  {
	b.FlushList.TailDirtyPage.PreFlushPage.NextFlushPage=nil
	b.FlushList.TailDirtyPage.PreFlushPage=nil
	b.FlushList.TailDirtyPage=b.FlushList.TailDirtyPage.PreFlushPage
}

func (b *BufferPool) FsyncFromFlushList(tree *BTree,num uint32) error {
	TmpPage:=tree.FlushList.TailDirtyPage
	b.FlushList.Count-=num
	TmpFile,err:=os.OpenFile(tree.FileName,syscall.O_RDWR,0666)
	if err!=nil {
		return err
	}
	defer TmpFile.Close()
	for num!=0 {
		err1 := tree.FlushNodeToDisk(TmpFile,TmpPage.CachePage)
		if err1 != nil {
			return err1
		}
		TmpPage.CachePage.HasLoaded=false
		b.EmptyFlushListTail()
		num--
	}
	return nil
}

func (b *BTree) TransferToControlPage(node *BTreeNode) *ControlPage {
	controlPage:=new(ControlPage)
	switch {
	case node.NodeType=="index":
		controlPage.PageType="index"
	case node.NodeType=="leaf":
		controlPage.PageType="data"
	}
	controlPage.CachePage=node
	controlPage.Offset=b.DiskMap[node]
	controlPage.FileName=b.FileName
	return controlPage
}

func (b *BufferPool) ReadToBufferFromDisk(node *BTreeNode,tree *BTree) error {
	var TmpDiskNode *diskNode
	var err3 error
	TmpFile,err:=os.OpenFile(tree.FileName,syscall.O_RDWR,0666)
	if err!=nil {
		return err
	}
	data:=make([]byte,pageSize)
	defer TmpFile.Close()
	bufRead:=bufio.NewReaderSize(TmpFile,BufferSize)
	offset:=tree.DiskMap[node]
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
	err5 := b.JoinLRUList(TmpPage)
	if err5 !=nil {
		return err5
	}
	return nil
}







