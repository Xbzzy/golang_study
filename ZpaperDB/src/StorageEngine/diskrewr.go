package StorageEngine

import (
	"encoding/json"
	"errors"
	"fmt"
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
}

func (b *BTree) RefactorBTreeNode(node *BTreeNode) *diskNode {
	var i uint16
	Tmp:=new(diskNode)
	Tmp.KeyNum=node.KeyNum
	Tmp.key=node.key
	Tmp.Value=node.Value
	Tmp.CurrentOffset=node.CurrentOffset
	Tmp.Pre=b.diskMap[node.Pre]
	Tmp.Next=b.diskMap[node.Next]
	for i=0 ; i<node.KeyNum+1 ;i++ {
		Tmp.ChildrenOffset[i]=b.diskMap[node.Children[i]]
	}
	return Tmp
}

func (b *BTree) RefactorDiskNode(node *diskNode) *BTreeNode {
	var i uint16
	Tmp:=new(BTreeNode)
	Tmp.KeyNum=node.KeyNum
	Tmp.key=node.key
	Tmp.Value=node.Value
	Tmp.CurrentOffset=node.CurrentOffset
	Tmp.Pre=b.memoryMap[node.Pre]
	Tmp.Next=b.memoryMap[node.Next]
	for i=0 ; i<node.KeyNum+1 ;i++ {
		Tmp.Children[i]=b.memoryMap[node.ChildrenOffset[i]]
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

func (b *BTree) InitSoredNodeOffset (node *BTreeNode,sum uint16) {
	var i uint16
	if b.IsLeaf(node)==true {
		return
	}
	sum++
	for i=0; i<node.KeyNum+1; i++ {
		node.CurrentOffset=uint64(sum-1)*pageSize
		b.UpdateMap(node,node.CurrentOffset)
		b.InitSoredNodeOffset(node.Children[i],sum)
	}
}

func (b *BTree) WriteSoredNode (file *os.File) error {
	for memoryAddress,diskAddress:= range b.diskMap {
		memoryAddress.HasLoaded=false
		tmp:=b.RefactorBTreeNode(memoryAddress)
		data,_:=tmp.EncodingDiskNodeToJson(tmp)
		_, err := file.Seek(int64(diskAddress),0)
		if err != nil {
			return err
		}
		_, err1 := file.Write(data)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (b *BTree) UpdateMap (node *BTreeNode,offset uint64) {
	b.memoryMap[offset]=node
	b.diskMap[node]=offset
}

func (b *BTree) FsyncAll(tree *BTree) error {
	sum:=0
	tree.InitSoredNodeOffset(tree.root,uint16(sum))
	TmpFile,err:=os.OpenFile(tree.FileName,syscall.O_RDWR,0666)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer TmpFile.Close()
	err1:=tree.WriteSoredNode(TmpFile)
	if err1!=nil {
		return err1
	}
	return nil
}

func (d *diskOperation) ReadNodeFromFile(node *BTreeNode,tree *BTree) error {
	var TmpDiskNode *diskNode
	var err3 error
	data:=make([]byte,pageSize)
	TmpFile,err:=os.OpenFile(tree.FileName,syscall.O_RDWR,0666)
	if err!=nil {
		return err
	}
	defer TmpFile.Close()
	offset:=tree.diskMap[node]
	_, err1 := TmpFile.Seek(int64(offset),0)
	if err1!=nil {
		return err1
	}
	_,err2:=TmpFile.Read(data)
	if err2!=nil {
		return err2
	}
	TmpDiskNode,err3=TmpDiskNode.DecodingJsonToDiskNode(data)
	if err3!=nil {
		return err3
	}
	node=tree.RefactorDiskNode(TmpDiskNode)
	node.HasLoaded=true
	return nil
}

func (b *BTree) FindNodeFromDisk(key byte,node *BTreeNode,tree *BTree) (*BTreeNode,error) {
	var i uint16
	for i=0 ; key < node.key[i] && i < node.KeyNum; {
		i++
	}
	err := tree.ReadNodeFromFile(node, tree)
	if err != nil {
		return nil,err
	}
	if node.key[i]==key {
		return node,nil
	}
	_,err1:=tree.FindNodeFromDisk(key,node.Children[i],tree)
	if err1 != nil {
		return nil,err1
	}
	return nil,errors.New("CannotFind")
}
