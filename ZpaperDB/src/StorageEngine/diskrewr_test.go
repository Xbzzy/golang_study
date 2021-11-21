package storage

import "fmt"

func (t *Test) TestFsync(tree *BTree) {
	tree.InitDataNodeOffset()
	err := tree.FsyncAll()
	if err != nil {
		return 
	}
}

func (t *Test) TestSearchFromDisk(tree *BTree) error {
	_, err := tree.SearchFromDisk(5)
	if err != nil {
		return err
	} else {
		fmt.Println("find successfully")
		return nil
	}
}
