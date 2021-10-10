package StorageEngine

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
	root *BTreeNode
	StartLeafNode *BTreeNode
	*AddressTranslationTable
	*BTreeArgs
	*FreeSpace
	*diskOperation
}

type BTreeNode struct {
	*KeyElement
	Pre *BTreeNode
	CurrentOffset uint64      //The current node's file offset.
	Next *BTreeNode
	Children []*BTreeNode
	HasLoaded bool
	NodeType BTreeNodeType
}

type KeyElement struct {
	KeyNum uint16
	key []byte
	Value []string
}

type AddressTranslationTable struct {
	memoryMap map[uint64]*BTreeNode
	diskMap map[*BTreeNode]uint64
}

type BTreeArgs struct {       //Basic parameters about btree
	FileName string
	NodeNum uint64
	OrderNum byte
	Height byte
}

type FreeSpace struct {       //Manages free blocks in btreeNode file.
	freeBlockNum uint64
	firstFreeBlockAddress *FreeAddress
}

type FreeAddress struct {
	currentAddress uint64
	nextFreeAddress uint64
}

type Record struct {
	Key byte
	Val string
}

type FindResult struct {
	BlockOffset uint64
	founded bool
	value string
}

func (b *BTree) InitBTree(order byte,filename string) (*BTree,error) {
	tree := new(BTree)
	tree.OrderNum=order
	file,err:=os.Create(filename)
	if err!=nil {
		return nil,errors.New("CreateFailed")
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)
	tree.FileName=filename
	return tree,nil
}

func (b *BTree) CreatBTreeRoot(data *Record,left *BTreeNode,right *BTreeNode) *BTreeNode {
	root := new(BTreeNode)
	root.KeyNum = 1
	root.key[1] = data.Key
	root.Value[1] = data.Val
	root.Children[0] = left
	root.Children[1] = right
	b.OrderNum++
	return root
}

func (b *BTree) CreatBTreeNode() *BTreeNode {
	if b.freeBlockNum == 0 {
		node := new(BTreeNode)
		return node
	} else {
		return b.memoryMap[b.firstFreeBlockAddress.currentAddress]
	}
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
		if node.key[mid] == key {
			return mid,nil
		} else if node.key[mid] > key {
			end = mid -1
		} else {
			start = mid + 1
		}
	}
	return 0,errors.New("CannotFound")
}

func (b *BTree) FindInsertSite(key byte,node *BTreeNode) uint16 {
	var i uint16
	i=0
	for key < node.key[i+1] && i < node.KeyNum{
		i++
	}
	return i
}

func (b *BTree) FindInsertNode(key byte,node *BTreeNode) *BTreeNode {
	site := b.FindInsertSite(key,node)
	if node.Children[site-1] != nil {
		return b.FindInsertNode(key,node.Children[site-1])
	} else{
		return node
	}
}

func (b *BTree) FindNodeParent(node *BTreeNode,root *BTreeNode) *BTreeNode {
	for i := 0 ; i<int(root.KeyNum) ; i++ {
		if root.Children[i]!=node {
			b.FindNodeParent(node,root.Children[i])
		} else {
			return root.Children[i]
		}
	}
	return nil
}
func (b *BTree) InsertNode(node *BTreeNode,site uint16,data *Record,Tmp *BTreeNode) {
	for i := node.KeyNum ; i>site ; i++ {
		node.key[i+1] = node.key[i]
		node.Value[i+1] = node.Value[i]
		node.Children[i+1] = node.Children[i]
	}
	node.key[site] = data.Key
	node.Value[site] = data.Val
	node.Children[site] = Tmp
	node.KeyNum++
}

func (b *BTree) SplitNode(parent *BTreeNode,site uint16 ) {
	var split,i uint16
	var MinKeyNum,MaxKeyNum uint16
	var Tmp *BTreeNode
	node:=parent.Children[site]
	if b.freeBlockNum == 0 {
		Tmp = new(BTreeNode)
	} else {
		Tmp = b.memoryMap[b.firstFreeBlockAddress.currentAddress]
	}
	Tmp.Children[0] = node.Children[split]
	node.Next = Tmp
	Tmp.Pre = node
	if node.NodeType == "index" {
		MinKeyNum = IndexNodeMinKeyNum
		MaxKeyNum = IndexNodeMaxKeyNum
	} else {
		MinKeyNum = DataNodeMinKeyNum
		MaxKeyNum = DataNodeMaxKeyNum
	}
	split = MinKeyNum
	for i = split+1; i <= MaxKeyNum; i++ {
		Tmp.key[i-split] = node.key[i]
		Tmp.Value[i-split] = node.Value[i]
		Tmp.Children[i-split] = node.Children[i]
	}
	Tmp.KeyNum = node.KeyNum-split
	node.KeyNum = split - 1
}

func (b *BTree) Insert(data *Record,tree *BTree) (*BTreeNode,error) {
	var Tmp *BTreeNode
	var MaxKeyNum uint16
	if tree.root==nil {
		tree.root = tree.CreatBTreeRoot(data,nil,nil)
		return tree.root,nil
	} else {
		insertNode:=tree.FindInsertNode(data.Key,tree.root)
		if insertNode.NodeType == "leaf" {
			MaxKeyNum = DataNodeMaxKeyNum
		} else {
			MaxKeyNum = IndexNodeMaxKeyNum
		}
		for {
			site:=tree.FindInsertSite(data.Key,insertNode)
			tree.InsertNode(insertNode,site,data,Tmp)
			if insertNode.KeyNum <= MaxKeyNum {
				return tree.root,nil
			} else {
				parent:=tree.FindNodeParent(insertNode,tree.root)
				tree.SplitNode(parent,site)
				return tree.root,nil
			}
		}
	}
}

func (b *BTree) MoveToLeft(node *BTreeNode,site uint16)  {
	var i uint16
	left := node.Children[site-1]
	move := node.Children[site]
	left.KeyNum++
	left.key[left.KeyNum] = node.key[site]
	left.Value[left.KeyNum] = node.Value[site]
	node.key[site] = move.key[site]
	node.Value[site] = move.Value[site]
	move.KeyNum--
	for	i=1 ;i<=move.KeyNum;i++{
		move.key[i] = move.key[i+1]
		move.Value[i] = move.Value[i+1]
	}
}

func (b *BTree) MoveToRight(node *BTreeNode,site uint16)  {
	var i uint16
	right := node.Children[site+1]
	move := node.Children[site]
	for i=right.KeyNum;i>0;i--{
		right.key[i+1] = right.key[i]
		right.Value[i+1] = right.Value[i]
	}
	right.key[1] = node.key[site]
	right.Value[1] = node.Value[site]
	right.KeyNum++
	node.key[site] = move.key[move.KeyNum]
	node.Value[site] = move.Value[move.KeyNum]
	move.KeyNum--
}

func (b *BTree) Combine(tree *BTree,node *BTreeNode,site uint16)  {
	var i,j uint16
	combine := node.Children[site-1]
	disappeared := node.Children[site]
	for i=0;i<=disappeared.KeyNum;i++{
		combine.KeyNum++
		combine.key[combine.KeyNum]=disappeared.key[i]
	}
	for j=site;j<node.KeyNum;j++{
		node.key[j] = node.key[j+1]
	}
	node.KeyNum--
	tree.freeBlockNum++
	diskAddress := tree.diskMap[disappeared]
	newFreeSpace := new(FreeAddress)
	newFreeSpace.currentAddress = diskAddress
	newFreeSpace.nextFreeAddress = tree.firstFreeBlockAddress.currentAddress
	tree.firstFreeBlockAddress = newFreeSpace
}

func (b *BTree) Remove(node *BTreeNode,site uint16)  {
	var i uint16
	for	i = site+1 ; i<=node.KeyNum ; i++{
		node.key[i-1] = node.key[i]
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
	Tmp:=b.FindInsertNode(key,node)
	site:=b.FindInsertSite(key,node)
	sign,err:=b.FindSite(key,node)
	if sign == 0 && err==nil {
		return nil,errors.New("CannotFind")
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
	Tmp:=b.FindInsertNode(key,node)
	site,_:=b.FindSite(key,Tmp)
	value := Tmp.Value[site]
	r.value = value
	r.founded = true
	r.BlockOffset = tree.diskMap[Tmp]
	return &r
}
















