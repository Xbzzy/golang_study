package Concurrent_Component

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
)

type Test struct {
	TestModeName string
	Errors error
}

// SameAddressSpaceTest is in order to verify goroutine will do something in same address space the creator created.
func (T *Test) SameAddressSpaceTest() {
	var wg sync.WaitGroup
	salutation := "hello"
	wg.Add(1)
	go func() {
		defer wg.Done()
		salutation = "welcome"
	}()
	wg.Wait()
	if salutation == "welcome" {
		return
	} else {
		T.Errors = errors.New("" + "testError: goroutine not shared same address space")
		fmt.Println(T.Errors)
		return
	}
}
//RandomnessOfGoroutineTest is in order to verify the randomness of goroutine run.
func (T *Test) RandomnessOfGoroutineTest()  {
	var wg sync.WaitGroup
	for _,salutation := range []string{"1","2","3","4"} {
		wg.Add(1)
		go func(salutation string) {  //there is an Iterated over a copy of the variable incoming.
			defer wg.Done()
			fmt.Println(salutation)
		}(salutation)
	}
	wg.Wait()
}
//LightweightGoroutineTest is in order calculate to memory consumption of goroutines.
func (T *Test) LightweightGoroutineTest()  {
	memConsumed := func() uint64 {
		runtime.GC()
		var s runtime.MemStats
		return s.Sys
	}
	var c <- chan interface{}
	var wg sync.WaitGroup
	noop := func() {
		wg.Done(); <-c
	}
	const numGoroutine = 1e4
	wg.Add(numGoroutine)
	before := memConsumed()
	for i := numGoroutine ; i>0 ; i-- {
		go noop()
	}
	wg.Wait()
	after := memConsumed()
	fmt.Printf("%.3fkb",float64(after-before)/numGoroutine/1000)
}
