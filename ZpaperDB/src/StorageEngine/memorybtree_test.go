package StorageEngine

import (
	"fmt"
)

func TestCreateBTree() (*storage.BTree,error) {
	btree:=new(storage.BTree)
	_=btree.InitBTree(3,"test.txt")
	for i:=1;i<500;i++ {
		TmpIndex:=btree.CreateIndex(byte(i),"")
		btree.Insert(TmpIndex)
	}
	return btree,nil
}
