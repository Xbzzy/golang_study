package StorageEngine

import (
	"fmt"
)

func TestCreateBTree() (tree *BTree,error) {
	btree:=new(BTree)
	_=btree.InitBTree(3,"test.txt")
	for i:=1;i<500;i++ {
		TmpIndex:=btree.CreateIndex(byte(i),"")
		btree.Insert(TmpIndex)
	}
	return btree,nil
}
func TestInsertBTree() (tree *BTree,error) {
	return tree,nil
}
