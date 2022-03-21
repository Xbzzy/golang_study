package storage

import (
	"encoding/json"
	"errors"
	"os"
	"syscall"
)

const (
	pageSize = 4096
)

type diskNode struct {
	KeyElement
	CurrentOffset uint64
	Pre uint64
	Next uint64
	ChildrenOffset []uint64
	*diskOperation
}

type diskOperation struct {
	*os.File
	*diskNode
}

func (b *BTree) InitDiskNode() *diskNode {
	tmpDiskNode:=new(diskNode)
	ke:= new(KeyElement)
	tmpDiskNode.ChildrenOffset=make([]uint64,DataNodeMaxKeyNum)
	ke.Key=make([]byte,DataNodeMaxKeyNum)
	ke.Value=make([]string,DataNodeMaxKeyNum)
	tmpDiskNode.KeyElement=*ke
	return tmpDiskNode
}

func (b *BTree) InitBTreeNode() *BTreeNode {
	node := new(BTreeNode)
	ke:= new(KeyElement)
	ke.Key=make([]byte,DataNodeMaxKeyNum)
	ke.Value=make([]string,DataNodeMaxKeyNum)
	node.Children=make([]*BTreeNode,DataNodeMaxKeyNum)
	node.KeyElement=ke
	return node
}

func (b *BTree) RefactorBTreeNode(node *BTreeNode) *diskNode {
	var i uint16
	Tmp:=b.InitDiskNode()
	Tmp.KeyNum=node.KeyNum
	Tmp.Key=node.Key
	Tmp.Value=node.Value
	Tmp.CurrentOffset=node.CurrentOffset
	Tmp.Pre=b.DiskMap[node.Pre]
	Tmp.Next=b.DiskMap[node.Next]
	for i=0 ; i<node.KeyNum+1 ;i++ {
		Tmp.ChildrenOffset[i]=b.DiskMap[node.Children[i]]
	}
	return Tmp
}

func (b *BTree) RefactorDiskNode(node *diskNode) *BTreeNode {
	var i uint16
	Tmp:=b.InitBTreeNode()
	Tmp.KeyNum=node.KeyNum
	Tmp.Key=node.Key
	Tmp.Value=node.Value
	Tmp.CurrentOffset=node.CurrentOffset
	Tmp.Pre=b.MemoryMap[node.Pre]
	Tmp.Next=b.MemoryMap[node.Next]
	for i=0 ; i<node.KeyNum+1 ;i++ {
		Tmp.Children[i]=b.MemoryMap[node.ChildrenOffset[i]]
	}
	return Tmp
}

func (d *diskNode) EncodingDiskNodeToJson(node *diskNode) ([]byte,error) {
	ByteStream,err:=json.Marshal(node)
	if err!=nil {
		return nil,err
	}
	return ByteStream,nil
}

func (d *diskNode) DecodingJsonToDiskNode(data []byte) (*diskNode,error) {
	var node *diskNode
	err:=json.Unmarshal(data,node)
	if err!=nil {
		return nil,err
	}
	return node,nil
}

func (b *BTree) InitDataNodeOffset () {
	currentNum:=1
	tempPointer:=b.StartLeafNode
	for {
		tempPointer.CurrentOffset=uint64(currentNum-1)*pageSize
		currentNum++
		b.UpdateMap(tempPointer,tempPointer.CurrentOffset)
		if tempPointer.Next != nil {
			tempPointer=tempPointer.Next
		} else {
			return
		}
	}
}

func (b *BTree) FlushNodeToDisk(TmpFile *os.File,node *BTreeNode) error {
	tmp:=b.RefactorBTreeNode(node)
	data,_:=tmp.EncodingDiskNodeToJson(tmp)
	_,err := TmpFile.Seek(int64(node.CurrentOffset),0)
	if err != nil {
		return err
	}
	_,err1 := TmpFile.Write(data)
	if err1 != nil {
		return err1
	}
	return nil
}

func (b *BTree) WriteSoredNode (file *os.File) error {
	for memoryAddress,diskAddress:= range b.DiskMap {
		memoryAddress.HasLoaded=false
		tmp:=b.RefactorBTreeNode(memoryAddress)
		data,_:=tmp.EncodingDiskNodeToJson(tmp)
		_, err := file.WriteAt(data,int64(diskAddress))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *BTree) UpdateMap (node *BTreeNode,offset uint64) {
	b.MemoryMap[offset]=node
	b.DiskMap[node]=offset
}

func (b *BTree) FsyncAll() error {
	b.InitDataNodeOffset()
	TmpFile,err:=os.OpenFile(b.FileName,syscall.O_RDWR,0666)
	if err != nil {
		return err
	}
	defer TmpFile.Close()
	err1:=b.WriteSoredNode(TmpFile)
	if err1!=nil {
		return err1
	}
	return nil
}

func (b *BTree) ReadNodeFromFile(node *BTreeNode) error {
	if node == nil {
		return errors.New("read error: nil page")
	}
	data := make([]byte,pageSize)
	TmpFile,err := os.OpenFile(b.FileName,syscall.O_RDWR,0666)
	if err != nil {
		return err
	}
	defer TmpFile.Close()
	offset := b.DiskMap[node]
	_,err1 := TmpFile.ReadAt(data,int64(offset))
	if err1 != nil {
		return err1
	}
	TmpDiskNode,err2:=b.DecodingJsonToDiskNode(data)
	if err2 != nil{
		return err2
	}
	node=b.RefactorDiskNode(TmpDiskNode)
	node.HasLoaded=true
	return nil
}

func (b *BTree) FindNodeFromDisk(key byte,node *BTreeNode) (*BTreeNode,error) {
	site := b.SearchSite(key,node)
	err := b.ReadNodeFromFile(node.Children[site])
	if err != nil {
		return nil,err
	}
	if node.Children[site] != nil {
		return b.FindNodeFromDisk(key,node.Children[site])
	} else{
		return node,nil
	}
}

func (b *BTree) SearchFromDisk(key byte) (*FindResult,error) {
	tmpResult := new(FindResult)
	node,err1 := b.FindNodeFromDisk(key,b.Root)
	if err1 != nil {
		return nil,err1
	}
	site, err2 := b.FindSite(key, node)
	if err2 != nil {
		return nil, err2
	}
	tmpResult.BlockOffset = b.DiskMap[node]
	tmpResult.Value = node.Value[site]
	tmpResult.Founded = true
	return tmpResult,nil
}