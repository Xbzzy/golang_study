package Concurrency

import (
	"fmt"
	"math"
	"os"
	"sync"
	"text/tabwriter"
	"time"
)
//WaitGroup is seen as a concurrent security counter;
//use add() to increment counter and done() to decrement counter;
//wait() will blocking goroutine while counter set to 0;
func (T *Test) TestWaitGroup()  {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("1st goroutine sleeping...")
		time.Sleep(1)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("2nd goroutine sleeping...")
		time.Sleep(2)
	}()
	wg.Wait()
	fmt.Println("all goroutine complete")
}

//Mutex access shared memory synchronously by convention;
//lock() to exclusive use of critical sections,unlock() to free the lock before;
func (T *Test) TestMutex()  {
	var count int
	var lock sync.Mutex
	increment := func() {
		lock.Lock()
		defer lock.Unlock()
		count++
		fmt.Printf("Incrementing: %d\n",count)
	}
	decrement := func() {
		lock.Lock()
		defer lock.Unlock()
		count--
		fmt.Printf("Decrementing: %d\n",count)
	}
	var arithmetic sync.WaitGroup
	for i := 0; i <= 5; i++ {
		arithmetic.Add(1)
		go func() {
			defer arithmetic.Done()
			increment()
		}()
	}
	for i := 0; i <= 5; i++ {
		arithmetic.Add(1)
		go func() {
			defer arithmetic.Done()
			decrement()
		}()
	}
	arithmetic.Wait()
	fmt.Println("Arithmetic complete")
}

//RWMutex is similar to Mutex in concept;
//any number of Read consumers can hold an ReadLock as long as nothing hold WriteLock;
//usually use RWMutex rather than Mutex,the former is more logical
func (T *Test) TestRWMutex() {
	producer := func(wg *sync.WaitGroup,l sync.Locker) {
		defer wg.Done()
		for i:=5;i>0;i-- {
			l.Lock()
			l.Unlock()
			time.Sleep(1)
		}
	}
	observer := func(wg *sync.WaitGroup,l sync.Locker) {
		defer wg.Done()
		l.Lock()
		defer l.Unlock()
	}
	test := func(count int,mutex,rwMutex sync.Locker) time.Duration {
		var wg sync.WaitGroup
		wg.Add(count+1)
		beginTestTime:=time.Now()
		go producer(&wg,mutex)
		for i:= count;i>0;i-- {
			go observer(&wg,rwMutex)
		}
		wg.Wait()
		return time.Since(beginTestTime)
	}
	tw:=tabwriter.NewWriter(os.Stdout,0,1,2,' ',0)
	defer tw.Flush()

	var m sync.RWMutex
	fmt.Fprintf(tw,"Readers\tRWMutex\tMutex\n")
	for i:=0 ;i<20 ;i++ {
		count := int(math.Pow(2,float64(i)))
		fmt.Fprintf(
			tw,
			"%d\t%v\t%v\n",
			count,
			test(count,&m,m.RLocker()),
			test(count,&m,&m),
			)
	}
}
//a rally point of a goroutine,waiting or issue a event(a signal between two or more goroutine);
//every time send signal to the longgest waiting time goroutine;
func (T *Test) TestCondSignal()  {
	c := sync.NewCond(&sync.Mutex{})
	queue := make([]interface{},0,10)
	removeFromQueue := func(delay time.Duration) {
		time.Sleep(delay)
		c.L.Lock()
		queue = queue[1:]
		fmt.Println("Removed from queue")
		c.L.Unlock()
		//let the waiting goroutine know what happened;
		c.Signal()
	}
	for i:=0 ;i<10 ;i++ {
		c.L.Lock()
		for len(queue) == 2 {
			//suspend the main goroutine until send one signal;
			c.Wait()
		}
		fmt.Println("Adding to queue")
		queue = append(queue, struct {}{})
		go removeFromQueue(1*time.Second)
		c.L.Unlock()
	}
}
//unlike signal send signal to one goroutine once,
//the solution of broadcast is communication with more than one goroutine;
//so in this testfunc,call broadcast in clicked cond will handle three application;
func (T *Test) TestCondBroadcast()  {
	type Button struct {
		Clicked *sync.Cond
	}
	button := Button{Clicked: sync.NewCond(&sync.Mutex{})}
	subscribe := func(c *sync.Cond,fn func()) {
		var goroutineRunning sync.WaitGroup
		goroutineRunning.Add(1)
		go func() {
			goroutineRunning.Done()
			c.L.Lock()
			defer c.L.Unlock()
			c.Wait()
			fn()
		}()
		goroutineRunning.Wait()
	}

	var clickRegistered sync.WaitGroup
	clickRegistered.Add(3)
	subscribe(button.Clicked, func() {
		fmt.Println("Maximizing window.")
		clickRegistered.Done()
	})
	subscribe(button.Clicked, func() {
		fmt.Println("Displaying annoying dialog box!")
		clickRegistered.Done()
	})
	subscribe(button.Clicked, func() {
		fmt.Println("Mouse clicked.")
		clickRegistered.Done()
	})
	button.Clicked.Broadcast()
	clickRegistered.Wait()
}

//once is in order to make sure only call once function of Do() passed;
func (T *Test) TestOnce()  {
	var count int
	increment := func() {
		count++
	}
	var once sync.Once
	var increments sync.WaitGroup
	increments.Add(100)
	for i:=0 ;i<100 ;i++ {
		go func() {
			defer increments.Done()
			once.Do(increment)
		}()
	}
	increments.Wait()
	fmt.Printf("Count is %d\n",count)
}
//pool module is a method that created and provided a fixed number of instances.
//when using pool module,remember the following points:
//when instantiation sync.pool,use new() to create member var to keep thread safety.
//when got the get() instance,never make assumptions for the state of accepted object.
//when run out of a object in get(),make sure to call put(),otherwise,pool will not reuse this instance(use defer).
//keep uniform distribution in pool. 
func (T *Test) TestPool()  {
	var numCalcsCreated int
	calcPool := &sync.Pool{
		New: func() interface{}{
			numCalcsCreated +=1
			mem := make([]byte,1024)
			return &mem
		},
	}
	calcPool.Put(calcPool.New())
	calcPool.Put(calcPool.New())
	calcPool.Put(calcPool.New())
	calcPool.Put(calcPool.New())

	const numWorkers = 1024*1024
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i:= numWorkers ;i>0 ;i-- {
		go func() {
			defer wg.Done()
			mem:=calcPool.Get().(*[]byte)
			defer  calcPool.Put(mem)
		}()
	}
	wg.Wait()
	fmt.Printf("%d calculators were created.", numCalcsCreated)
}
