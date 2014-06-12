/*
  封装一个类用于等待类退出，分为两步：
  1、类处理完退出操作后调用AllDone()
  2、类的Quit()方法中调用Quit()

*/

package lib

import (
	"logd/loglib"
	"time"
)

type WaitQuit struct {
	name         string
	allowTimeout int //允许的超时次数, 小于0表示不限次数
	nTimeout     int //已超时次数
	ch           chan bool
}

func NewWaitQuit(modName string, allow ...int) *WaitQuit {
	ch := make(chan bool)
	a := 2
	if len(allow) > 0 {
		a = allow[0]
	}
	return &WaitQuit{modName, a, 0, ch}
}

func (this *WaitQuit) AllDone() {
	this.ch <- true
}

func (this *WaitQuit) Quit() bool {
	ret := false
	select {
	case <-this.ch:
		loglib.Info(this.name + " safe quit.")
		ret = true
	case <-time.After(2 * time.Second):
		loglib.Info(this.name + " quit timeout")
		this.nTimeout++
		if this.allowTimeout > 0 && this.nTimeout >= this.allowTimeout {
			ret = true
		}
	}
	return ret

}
