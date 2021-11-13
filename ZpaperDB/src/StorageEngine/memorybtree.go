package storage

import (
	"errors"
	"os"
)

const (
	IndexNodeMinKeyNum = 225
	IndexNodeMaxKeyNum = 450
	DataNodeMinKeyNum = 115
	DataNodeMaxKeyNum = 230
)

type BTreeNodeType string //include "leaf","index","nil"
type BTree struct{
	Root *BTreeNode
	StartLeafNode *BTreeNode
	*AddressTranslationTable
	*BTreeArgs
	*FreeSpace
	*diskOperation
	*BufferPool
}

type BTreeNode struct {
	*KeyElement
	Pre *BTreeNode
	Next *BTreeNode
	Children []*BTreeNode
	CurrentOffset uint64      //The current node's file offset.
	HasLoaded bool
	NodeType BTreeNodeType
	ControlInfo *ControlPage
}

type KeyElement struct {
	KeyNum uint16
	Key []byte
	Value []string
}

type AddressTranslationTable struct {
	MemoryMap map[uint64]*BTreeNode
	DiskMap map[*BTreeNode]uint64
}

type BTreeArgs struct {       //Basic parameters about btree
	FileName string
	NodeNum uint64
	OrderNum byte
	Height byte
}

type FreeSpace struct {
	FreeBlockNum uint32
	FirstFreeBlockAddress *FreeAddress
}

type FreeAddress struct {
	CurrentAddress uint64
	NextFreeAddress uint64
}

type Index struct {
	Key byte
	Val string
}

type FindResult struct {
	BlockOffset uint64
	Founded bool
	Value string
}

func (b *BTree)InitBTree(order byte,filename string) error {
	args:=new(BTreeArgs)
	b.BTreeArgs=args
	b.OrderNum=order
	file,err:=os.Create(filename)
	if err!=nil {
		return errors.New("fileError: create file failed")
	}
	defer file.Close()
	b.FileName=filename
	att:=new(AddressTranslationTable)
	b.AddressTranslationTable=att
	b.AddressTranslationTable.MemoryMap=make(map[uint64]*BTreeNode)
	b.AddressTranslationTable.DiskMap=make(map[*BTreeNode]uint64)
	fs:=new(FreeSpace)
	b.FreeSpace=fs
	bp:=new(BufferPool)
	b.BufferPool=bp
	b.BufferPool.InitBufferPool()
	return nil
}

func (b *BTree) CreatBTreeRoot(data *Index,left *BTreeNode,right *BTreeNode) *BTreeNode {
	TmpRoot := b.CreatBTreeDataNode(data)
	TmpRoot.KeyNum = 1
	TmpRoot.Key[1] = data.Key
	TmpRoot.Value[1] = data.Val
	TmpRoot.Children[0] = left
	TmpRoot.Children[1] = right
	b.OrderNum++
	b.Root = TmpRoot
	return TmpRoot
}

func (b *BTree) CreateBTreeIndexNode(key byte) *BTreeNode {
	if b.FreeBlockNum == 0 {
		node := new(BTreeNode)
		ke := new(KeyElement)
		ke.Key=make([]byte,IndexNodeMaxKeyNum)
		ke.Value=nil
		node.Children=make([]*BTreeNode,IndexNodeMaxKeyNum)
		node.KeyElement=ke
		node.Key[1]=key
		return node
	} else {
		return b.MemoryMap[b.FirstFreeBlockAddress.CurrentAddress]
	}
}

func (b *BTree) CreatBTreeDataNode(data *Index) *BTreeNode {
	if b.FreeBlockNum == 0 {
		node := new(BTreeNode)
		ke:= new(KeyElement)
		ke.Key=make([]byte,DataNodeMaxKeyNum)
		ke.Value=make([]string,DataNodeMaxKeyNum)
		node.Children=make([]*BTreeNode,DataNodeMaxKeyNum)
		node.KeyElement=ke
		node.Key[1]=data.Key
		node.Value[1]=data.Val
		return node
	} else {
		return b.MemoryMap[b.FirstFreeBlockAddress.CurrentAddress]
	}
}

func (b *BTree) CreateIndex(key byte,value string) *Index {
	index:=new(Index)
	index.Key=key
	index.Val=value
	return index
}

func (b *BTree) IsLeaf (node *BTreeNode) bool {
	if node.Children[0]==nil {
		return true
	} else {
		return false
	}
}

func (b *BTree) FindSite(key byte,node *BTreeNode) (uint16,error) {
	var start,end,mid uint16
	start,end,mid = 0,node.KeyNum-1,0
	for start<=end {
		mid = (start + end)/2
		if node.Key[mid] == key {
			return mid,nil
		} else if node.Key[mid] > key {
			end = mid -1
		} else {
			start = mid + 1
		}
	}
	return 0,errors.New("find: can not find key of site")
}

func (b *BTree) FindInsertSite(key byte,node *BTreeNode) uint16 {
	i:=uint16(0)
	for key > node.Key[i+1] && i < node.KeyNum {
		i++
	}
	return i+1
}

func (b *BTree) FindInsertDataNode(key byte,node *BTreeNode) *BTreeNode {
	site := b.FindInsertSite(key,node)
	if node.Children[site] != nil {
		return b.FindInsertDataNode(key,node.Children[site])
	} else{
		return node
	}
}

// FindNodeParent : split function need back to parent node.
func (b *BTree) FindNodeParent(node *BTreeNode,root *BTreeNode) (*BTreeNode,uint16) {
	var i uint16
	if root==b.Root {
		return nil,0
	}
	for i = 0 ; i < root.KeyNum+1 ; i++ {
		if root.Children[i] == node {
			return root,i
		} else {
			if root.Children[i] == nil {
				continue
			} else {
				b.FindNodeParent(node, root.Children[i])
			}
		}
	}
	return nil,0
}

func (b *BTree) InsertNode(node *BTreeNode,site uint16,data *Index,Tmp *BTreeNode) {
	for i := node.KeyNum ; i>site ; i-- {
		node.Key[i+1] = node.Key[i]
		node.Value[i+1] = node.Value[i]
		node.Children[i+1] = node.Children[i]
	}
	node.Key[site] = data.Key
	node.Value[site] = data.Val
	node.Children[site] = Tmp
	node.KeyNum++
	b.DirtyPage[node]=true
	return
}

// SplitNode :when index node or data node is full,then split it.
//the new node comes from free block in current disk file.
func (b *BTree) SplitNode(node *BTreeNode) *BTreeNode {
	var MinKeyNum,MaxKeyNum uint16
	var Tmp *BTreeNode
	if b.FreeBlockNum == 0 {
		Tmp = new(BTreeNode)
	} else {
		Tmp = b.MemoryMap[b.FirstFreeBlockAddress.CurrentAddress]
	}
	node.Next = Tmp
	Tmp.Pre = node
	if node.NodeType == "index" {
		MinKeyNum = IndexNodeMinKeyNum
		MaxKeyNum = IndexNodeMaxKeyNum
	} else {
		MinKeyNum = DataNodeMinKeyNum
		MaxKeyNum = DataNodeMaxKeyNum
	}
	splitNum:= MinKeyNum
	for i := splitNum+1; i <= MaxKeyNum; i++ {
		Tmp.Key[i-splitNum] = node.Key[i]
		Tmp.Value[i-splitNum] = node.Value[i]
		Tmp.Children[i-splitNum] = node.Children[i]
	}
	Tmp.KeyNum = node.KeyNum-splitNum
	node.KeyNum = splitNum - 1
	return Tmp
}

func (b *BTree) AdjustAfterSplit(parent *BTreeNode,tmpSplitNode *BTreeNode,site uint16) {
	for i := parent.KeyNum+1; i>site; i--{
		parent.Key[i+1] = parent.Key[i]
		parent.Children[i+1] = parent.Children[i]
	}
	parent.Key[site+1] = tmpSplitNode.Key[1]
	parent.Children[site+1] = tmpSplitNode
}

func (b *BTree) Insert(data *Index) {
	var Tmp *BTreeNode
	if b.Root==nil {
		b.CreatBTreeRoot(data,nil,nil)
		return
	} else {
		insertDataNode:=b.FindInsertDataNode(data.Key,b.Root)
		site:=b.FindInsertSite(data.Key,insertDataNode)
		b.DirtyPage[insertDataNode]=true
		if insertDataNode.KeyNum < DataNodeMaxKeyNum-1 { 		//leaf node non-full and index node non-full
			b.InsertNode(insertDataNode,site,data,Tmp)
			return
		} else { 		// leaf node full and index node non-full
			parent,site := b.FindNodeParent(insertDataNode, b.Root)
			if parent.KeyNum < IndexNodeMaxKeyNum-1 {
				TmpSplit := b.SplitNode(insertDataNode)
				b.AdjustAfterSplit(parent,TmpSplit,site)
				b.InsertNode(parent, site, data, Tmp)
				return
			} else { 		// leaf node full and index node full
				TmpSplit := b.SplitNode(insertDataNode)
				b.AdjustAfterSplit(parent,TmpSplit,site)
				b.InsertNode(parent, site, data, Tmp)
				indexParent,indexSite := b.FindNodeParent(parent, b.Root)
				TmpSplit2 := b.SplitNode(indexParent)
				b.AdjustAfterSplit(indexParent,TmpSplit2,indexSite)
				b.InsertNode(indexParent, indexSite, data, Tmp)
				return
			}
		}
	}
}

func (b *BTree) MoveToLeft(node *BTreeNode,site uint16)  {
	var i uint16
	left := node.Children[site-1]
	move := node.Children[site]
	left.KeyNum++
	left.Key[left.KeyNum] = node.Key[site]
	left.Value[left.KeyNum] = node.Value[site]
	node.Key[site] = move.Key[site]
	node.Value[site] = move.Value[site]
	move.KeyNum--
	for	i=1 ;i<=move.KeyNum;i++{
		move.Key[i] = move.Key[i+1]
		move.Value[i] = move.Value[i+1]
	}
}

func (b *BTree) MoveToRight(node *BTreeNode,site uint16)  {
	var i uint16
	right := node.Children[site+1]
	move := node.Children[site]
	for i=right.KeyNum;i>0;i--{
		right.Key[i+1] = right.Key[i]
		right.Value[i+1] = right.Value[i]
	}
	right.Key[1] = node.Key[site]
	right.Value[1] = node.Value[site]
	right.KeyNum++
	node.Key[site] = move.Key[move.KeyNum]
	node.Value[site] = move.Value[move.KeyNum]
	move.KeyNum--
}

func (b *BTree) Combine(tree *BTree,node *BTreeNode,site uint16)  {
	var i,j uint16
	combine := node.Children[site-1]
	disappeared := node.Children[site]
	for i=0;i<=disappeared.KeyNum;i++{
		combine.KeyNum++
		combine.Key[combine.KeyNum]=disappeared.Key[i]
	}
	for j=site;j<node.KeyNum;j++{
		node.Key[j] = node.Key[j+1]
	}
	node.KeyNum--
	tree.FreeBlockNum++
	diskAddress := tree.DiskMap[disappeared]
	newFreeSpace := new(FreeAddress)
	newFreeSpace.CurrentAddress = diskAddress
	newFreeSpace.NextFreeAddress = tree.FirstFreeBlockAddress.CurrentAddress
	tree.FirstFreeBlockAddress = newFreeSpace
}

func (b *BTree) Remove(node *BTreeNode,site uint16)  {
	var i uint16
	for	i = site+1 ; i<=node.KeyNum ; i++{
		node.Key[i-1] = node.Key[i]
		node.Value[i-1] = node.Value[i]
	}
	node.KeyNum--
}

func (b *BTree) AdjustBTree(tree *BTree,node *BTreeNode,site uint16) {
	var MinKeyNum uint16
	if node.NodeType == "leaf" {
		MinKeyNum = DataNodeMaxKeyNum
	} else {
		MinKeyNum = IndexNodeMaxKeyNum
	}
	if site==0 {
		if node.Children[1].KeyNum > MinKeyNum + 1 {
			b.MoveToLeft(node,1)
		} else {
			b.Combine(tree,node,1)
		}
	} else if site==node.KeyNum {
		if node.Children[site-1].KeyNum > MinKeyNum +1 {
			b.MoveToRight(node,site-1)
		} else {
			b.Combine(tree,node,site)
		}
	} else if node.Children[site-1].KeyNum > MinKeyNum +1 {
		b.MoveToRight(node,site)
	} else if node.Children[site+1].KeyNum > MinKeyNum +1 {
		b.MoveToLeft(node,site+1)
	} else {
		b.Combine(tree,node,site)
	}
}

func (b *BTree)Delete(tree *BTree,key byte,node *BTreeNode) (*BTreeNode,error) {
	var MinKeyNum uint16
	if node.NodeType == "leaf" {
		MinKeyNum = DataNodeMaxKeyNum
	} else {
		MinKeyNum = IndexNodeMaxKeyNum
	}
	Tmp:=b.FindInsertDataNode(key,node)
	tree.DirtyPage[Tmp]=true
	site:=b.FindInsertSite(key,node)
	sign,err:=b.FindSite(key,node)
	if sign == 0 && err==nil {
		return nil,errors.New("delete: can not find")
	} else {
		tree.Remove(Tmp,sign)
		if Tmp.KeyNum < MinKeyNum +1 {
			b.AdjustBTree(tree,node,site)
		}
	}

	return node,nil
}

func (b *BTree) Search(tree *BTree,key byte,node *BTreeNode) *FindResult {
	var r FindResult
	if tree == nil {
		return nil
	}
	Tmp:=b.FindInsertDataNode(key,node)
	site,_:=b.FindSite(key,Tmp)
	value := Tmp.Value[site]
	r.Value = value
	r.Founded = true
	r.BlockOffset = tree.DiskMap[Tmp]
	return &r
}
