package main

import (
	"bytes"
	"compress/zlib"
	"container/list"
	"fmt"
	"logd/lib"
	"logd/loglib"
	"logd/tcp_pack"
	"time"
)

type Receiver struct {
	sendBuffer     chan bytes.Buffer
	logList        *list.List
	listBufferSize int //多少条日志发送一次
	receiveChan    chan string
	nTailedLines   int //tailler重启时用于计算开始的id
	wq             *lib.WaitQuit
}

//工厂初始化函数
func ReceiverInit(buffer chan bytes.Buffer, c chan string, listBufferSize int, nTailedLines int) (r Receiver) {
	// var r Receiver
	r.sendBuffer = buffer
	r.logList = list.New()
	r.receiveChan = c
	r.listBufferSize = listBufferSize
	r.wq = lib.NewWaitQuit("receiver")
	r.nTailedLines = nTailedLines
	return r
}

func (r Receiver) clearList() (b bytes.Buffer) {
	var result bytes.Buffer
	for r.logList.Len() > 0 {
		a := r.logList.Front()
		r.logList.Remove(a)
		result.WriteString(a.Value.(string))
		// fmt.Println("removed : ",a.Value)
	}
	// fmt.Println("removed : ",result)
	// var b bytes.Buffer

	w := zlib.NewWriter(&b)
	w.Write(result.Bytes())
	w.Close()

	// fmt.Println("ziped : ",b)
	return b

}

//goroutine
//clear list & zipping & send_to_buffer
func (r Receiver) writeList() {
	//收尾工作
	defer func() {
		if err := recover(); err != nil {
			loglib.Error(fmt.Sprintf("receiver panic:%v", err))
		}
		close(r.sendBuffer)
	}()

	st := time.Now()
	var nLines = 0
	var id = r.initId()
	ip := lib.GetIp()
	var changed = false
	var hourFmt = "2006010215"
	var hour = time.Now().Format(hourFmt)

	for logLine := range r.receiveChan {

		changed = false

		if logLine == "logfile changed" {
			changed = true
		} else {
			r.logList.PushBack(logLine)
		}
		nLines = r.logList.Len()
		//达到指定行数或发现日志rotate
		//因此每小时只有最后一个包比listBufferSize小
		//如果quit时包小于listBufferSize就丢弃，重启后再读
		if nLines >= r.listBufferSize || (nLines > 0 && changed) {
			b := r.clearList()
			//r.sendBuffer <- b
			ed := time.Now()
			elapse := ed.Sub(st)
			loglib.Info(fmt.Sprintf("add a pack, id: %s_%d elapse: %s", hour, id, elapse))

			//route信息
			m := make(map[string]string)
			m["ip"] = ip
			m["hour"] = hour
			m["id"] = fmt.Sprintf("%d", id)
			m["lines"] = fmt.Sprintf("%d", nLines)
			m["stage"] = "make pack"
			m["st"] = st.Format("2006-01-02 15:04:05.000")
			m["ed"] = ed.Format("2006-01-02 15:04:05.000")
			m["elapse"] = elapse.String()
			if changed {
				m["done"] = "1"
			}

			vbytes := tcp_pack.Packing(b.Bytes(), m, false)
			b.Reset()
			b.Write(vbytes)
			r.sendBuffer <- b
			id++
			st = time.Now()
			nLines = 0
		}

		if changed {
			now := time.Now()
			tStr := now.Format(hourFmt)
			//发现日志rotate了，但是时间未变，则强制改为下一小时
			if tStr == hour {
				hour = now.Add(time.Hour).Format(hourFmt)
			} else {
				hour = tStr
			}

			id = 1 //每小时id刷新
		}

	}

	if nLines > 0 {
		loglib.Info(fmt.Sprintf("receiver abandon %d lines", nLines))
	}

}

// //goroutine
// func sender() {
// 	for {
// 		fmt.Println("ready to get data from sendBuffer")
// 		b := <- sendBuffer
// 		fmt.Println("sender get:",b)
// 	}
// }

func (r Receiver) Start() {
	r.writeList()
	r.wq.AllDone()
}

func (r Receiver) Quit() bool {
	return r.wq.Quit()
}

func (r Receiver) initId() int {
	return (r.nTailedLines / r.listBufferSize) + 1
}
