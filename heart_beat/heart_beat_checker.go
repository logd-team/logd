package heart_beat

import (
	"logd/loglib"
	"net"
	"time"
)

type HeartBeatChecker struct {
}

type CheckResult struct {
	Addr string
	Err  bool //false表示成功
	Msg  string
}

func NewHeartBeatChecker() *HeartBeatChecker {
	return &HeartBeatChecker{}
}

func (this *HeartBeatChecker) Run(addrs []string, interval int, processor ResultProcessor) {
	for {
		this.CheckAround(addrs, processor)
		time.Sleep(time.Duration(interval) * time.Second)
	}
}
func (this *HeartBeatChecker) CheckAround(addrs []string, processor ResultProcessor) {

	nAddrs := len(addrs)
	if nAddrs > 0 {
		chans := make(chan CheckResult, nAddrs)
		results := make([]CheckResult, nAddrs)

		for _, addr := range addrs {
			go check(addr, chans)
		}

		for i := 0; i < nAddrs; i++ {
			results[i] = <-chans
		}
		//处理结果
		processor.Process(results)
	}
}
func check(addr string, ch chan CheckResult) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		msg := "[heart beat] connect to " + addr + " error"
		loglib.Error(msg)
		ch <- CheckResult{addr, true, msg}
	} else {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second)) //读超时
		var buf = make([]byte, 2)
		n, err := conn.Read(buf)
		if err != nil {
			msg := "[heart beat] read data from " + addr + " error"
			loglib.Error(msg)
			ch <- CheckResult{addr, true, msg}
		}
		if string(buf[:n]) == "!" {
			ch <- CheckResult{addr, false, ""}
		} else {
			ch <- CheckResult{addr, true, "unknow return code"}
		}
		conn.Close()
	}
}

//用于处理结果的回调类型，需要实现Process方法
type ResultProcessor interface {
	Process(results []CheckResult)
}
