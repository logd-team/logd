package lib

import (
	"fmt"
	"sync"
	// "time"
	"container/list"
)

type GlobalList struct {
	list *list.List
	m    *sync.Mutex
	// once sync.Once
}

func GlobalListInit() (globalList *GlobalList) {

	globalList = new(GlobalList)
	globalList.list = list.New()
	globalList.m = new(sync.Mutex)

	return globalList
}

// var globaleFileList *list.List = list.New()
// var m *sync.Mutex = new(sync.Mutex)
// var once sync.Once

func (gl *GlobalList) Setup() {
	for i := 10; i < 20; i++ {
		gl.list.PushBack(i)
	}
	fmt.Println("setup over")
}

func (gl *GlobalList) Remove() (e *list.Element) {

	gl.m.Lock()
	// var result int
	if gl.list.Len() > 0 {
		e = gl.list.Front()
		// result = front.Value.(int)
		gl.list.Remove(e)
	} else {
		e = nil
	}
	// fmt.Println("gl len:",gl.list.Len())
	gl.m.Unlock()

	return e
}

func (gl *GlobalList) PushBack(v interface{}) {

	gl.m.Lock()
	gl.list.PushBack(v)
	gl.m.Unlock()

}
