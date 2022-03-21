package storage

import "testing"

func TestInsert(t *testing.T) {
	var lsmTree *LSMTree
	var err error
	lsmTree, err = lsmTree.initLSMTree()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {

}

func TestSearch(t *testing.T) {

}

func TestPutGet(t *testing.T) {
	var lsmTree *LSMTree
	var err error
	lsmTree, err = MakeLSMTree()
	if err != nil {
		t.Fatal(err)
	}
	args := new(WriteArgs)
	args.Key = nil
	args.Value = nil
	reply := new(WriteReply)
	err1 := lsmTree.Put(args, reply)
	if err1 != nil {
		t.Fatal(err1)
	}

}

func TestConcurrentGet(t *testing.T) {

}
