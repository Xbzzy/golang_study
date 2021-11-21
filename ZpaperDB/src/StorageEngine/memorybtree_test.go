package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
)

func (t *Test)TestCreateBTree() *BTree {
	btree:=new(BTree)
	_=btree.InitBTree(3,"data")
	for i:=0;i<10;i++ {
		TmpIndex:=btree.CreateIndex(byte(i),strconv.Itoa(i))
		btree.Insert(TmpIndex)
	}
	return btree
}

func (t *Test) TestInsert(tree *BTree,MaxInsertNum int)  {
	for i:=10; i < MaxInsertNum; i++ {
		TmpIndex:=tree.CreateIndex(byte(i), strconv.Itoa(i))
		tree.Insert(TmpIndex)
	}
	return
}

func (t *Test) TestDelete(tree *BTree,MaxDeleteNum int) {
	randomDeletedNum := rand.Intn(MaxDeleteNum)
	for i:=0; i<randomDeletedNum; i++ {
		randomDeleteKey := rand.Intn(1000)
		err := tree.Delete(byte(randomDeleteKey))
		if err != nil {
			fmt.Println(err)
		}
	}
	if tree.FreeBlockNum == 0 {
		fmt.Println(errors.New("memory error: deleted page free field"))
		return
	}
	return
}

func (t *Test) TestSearch(tree *BTree,MaxSearchNum uint16) {
	for i:=uint16(0);i<MaxSearchNum;i++ {
		randomNum := rand.Intn(300)
		findResult := tree.Search(byte(randomNum))
		if findResult.Value != strconv.Itoa(randomNum) {
			fmt.Println(errors.New("data error: data abnormal"))
		}
	}
	return
}

func (t *Test) TestTraverseDataNode(tree *BTree)  {
	temp := tree.StartLeafNode
	for {
		fmt.Println(temp.Key[1])
		if temp.Next == nil {
			return
		}
		temp = temp.Next
	}
}
