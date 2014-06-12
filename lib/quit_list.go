/*
用于串行的等待各模块退出，各模块需提供一个钩子方法：Quit() bool
*/

package lib

import (
	"container/list"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type QuitFunc func() bool

type QuitList struct {
	lst   *list.List
	mutex *sync.RWMutex
}

func NewQuitList() *QuitList {
	lst := list.New()
	mutex := &sync.RWMutex{}

	return &QuitList{lst, mutex}
}

func (this *QuitList) Append(f QuitFunc) (lstLen int) {
	this.mutex.Lock()
	this.lst.PushBack(f)
	lstLen = this.lst.Len()
	this.mutex.Unlock()

	return
}

//自带信号处理方法，需要在ExecQuit()执行前调用
//也可用其他信号处理方法替代
func (this *QuitList) HandleQuitSignal() {
	//signal handling, for elegant quit
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGQUIT)
	s := <-ch
	log.Println("get signal:", s)
}

func (this *QuitList) ExecQuit() {
	log.Println("begin quit...")
	for this.lst.Len() > 0 {

		this.mutex.Lock()
		for e := this.lst.Front(); e != nil; e = e.Next() {
			f, ok := e.Value.(QuitFunc)
			if ok {
				if f() {
					this.lst.Remove(e)
				}
			} else {
				log.Println("trans error")
				this.lst.Remove(e)
			}

		}

		this.mutex.Unlock()
	}
}
