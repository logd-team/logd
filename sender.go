package main

import (
	"bytes"
	"container/list"
	"fmt"
	"io/ioutil"
	"logd/lib"
	"logd/loglib"
	"logd/tcp_pack"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var once sync.Once
var fileList *lib.GlobalList = lib.GlobalListInit()

type Sender struct {
	id                   int
	sBuffer              chan bytes.Buffer
	mem_max_len          int
	file_mem_folder_name string
	// mem_dump_to_file int
	memCacheList *list.List
	// fileCacheList *list.List
	// conn *net.TCPConn
	connection    Connection
	status        *int
	sendToAddress string

	wq *lib.WaitQuit
}

//工厂初始化函数
//增加备用地址，暂时支持一个备用地址
func SenderInit(buffer chan bytes.Buffer, addr string, bakAddr string, id int) (s Sender) {
	// var s Sender
	// s = new(Sender)
	s.id = id
	s.sBuffer = buffer
	s.mem_max_len = 100
	s.file_mem_folder_name = "tempfile"
	//auto make dir
	if _, err := os.Stat(s.file_mem_folder_name); err != nil && os.IsNotExist(err) {
		os.MkdirAll(s.file_mem_folder_name, 0775)
	}
	// s.mem_dump_to_file = 3
	s.memCacheList = list.New()
	// s.fileCacheList = list.New()
	s.sendToAddress = addr
	// s.conn = s.getConnection()
	s.connection = SingleConnectionInit(s.sendToAddress, bakAddr)
	a := 1
	s.status = &a
	s.wq = lib.NewWaitQuit("sender", -1)

	return s
}

//should be run by once
func (s *Sender) reloadFileCache() {
	list := lib.GetFilelist(s.file_mem_folder_name)
	for _, filename := range list {
		// s.fileCacheList.PushBack(filename)
		loglib.Info("reloading:" + filename)
		fileList.PushBack(filename)
	}
}

//goroutine
func (s *Sender) Start() {
	// conn := s.getConnection()
	//初始化fileCacheList
	once.Do(s.reloadFileCache)

	//收尾工作
	defer func() {
		if err := recover(); err != nil {
			loglib.Error(fmt.Sprintf("sender %d panic:%v", s.id, err))
		}

		s.saveBufferInChan()

		s.saveMemCache()

		s.connection.close()

		s.wq.AllDone()

	}()

	//var connLost = 0
	var quit = false
	go lib.HandleQuitSignal(func() {
		quit = true
		s.connection.close()
	})

	var sendInterval = time.Duration(10)

	var timeoutChan = time.After(sendInterval * time.Millisecond)
	for !quit {

		select {
		case b := <-s.sBuffer:
			//send b
			//result := s.sendBuffer(b)
			//if result == false {
			//write to mem
			//改为直接放入缓存
			if b.Len() > 0 {
				s.insertToMemCache(b)
			}
			//}

			// case <- time.After(1 * time.Millisecond):
			//如果status<=0,将不尝试发送缓存内内容
		case <-timeoutChan:

			/*
							if *s.status <= 0 {
			                    //log.Println("status <= 0",*s.status)
			                    connLost++
			                    //减少报警次数
			                    //连续5次才报
			                    if connLost >= 20 {
			                        loglib.Info("no send connection")
			                        connLost = 0
			                    }
								break
							}
			                connLost = 0
			*/
			//send mem or file data
			if s.memCacheList.Len() > 0 { // send from mem
				front := s.memCacheList.Front()
				tempBuffer := front.Value.(bytes.Buffer)

				packId := tcp_pack.GetPackId(tempBuffer.Bytes())
				loglib.Info(fmt.Sprintf("get pack %s from mem, left %d", packId, s.memCacheList.Len()))
				result := s.sendBuffer(tempBuffer)
				if result == true {
					s.memCacheList.Remove(front)
				}
			}
			//}else {// send from file
			e := fileList.Remove()
			if e != nil { // file list is not empty
				filename := e.Value.(string)
				// fmt.Println("sender ",s.id,": get file :",filename)
				data, err := ioutil.ReadFile(filename)
				if err != nil {
					// fmt.Println("sender ",s.id,":",err)
					fileList.PushBack(filename)
					loglib.Error(fmt.Sprintf("read file cache %s error:%s", filename, err.Error()))
				} else {

					packId := tcp_pack.GetPackId(data)                                                           //debug info
					loglib.Info(fmt.Sprintf("read pack %s from file: %s, len: %d", packId, filename, len(data))) //debug info
					result := s.sendData2(data)
					if result == true {
						// s.fileCacheList.Remove(front)
						// log.Println("sender ",s.id,":removed file:",filename, "for pack", packId)//debug info
						err = os.Remove(filename)
						lib.CheckError(err)
					} else {
						fileList.PushBack(filename)
						// fmt.Println("sender ",s.id,": pushback file :",filename)
					}
				}
			}
			//}
			timeoutChan = time.After(sendInterval * time.Millisecond)

		}
	}

}

func (s *Sender) Quit() bool {
	return s.wq.Quit()
}

func (s *Sender) saveBufferInChan() {
	loglib.Info(fmt.Sprintf("sender %d begin to save pack in chan", s.id))
	i := 0
	for b := range s.sBuffer {
		s.writeToFile(b)
		i++
	}
	loglib.Info(fmt.Sprintf("sender %d saved num of pack in chan: %d", s.id, i))
}

func (s *Sender) saveMemCache() {
	loglib.Info(fmt.Sprintf("sender %d begin to save pack in mem cache", s.id))
	i := 0
	for e := s.memCacheList.Front(); e != nil; e = e.Next() {
		tempBuffer := e.Value.(bytes.Buffer)

		//write to file
		s.writeToFile(tempBuffer)
		i++

	}
	loglib.Info(fmt.Sprintf("sender %d saved num of pack in mem cache: %d", s.id, i))
}

func (s *Sender) insertToMemCache(data bytes.Buffer) {
	//检查内存缓存
	if s.memCacheList.Len() >= s.mem_max_len { //内存缓存已满,mem pop to file
		loglib.Info(fmt.Sprintf("memCacheList len: %d writing data to tempfile", s.memCacheList.Len()))
		//pop a buffer
		a := s.memCacheList.Front()
		tempBuffer := a.Value.(bytes.Buffer)

		//write to file
		s.writeToFile(tempBuffer)

		//remove from list
		s.memCacheList.Remove(a)

		packId := tcp_pack.GetPackId(data.Bytes())

		loglib.Info("add pack " + packId + " to mem cache")

		s.memCacheList.PushBack(data)

	} else { //内存缓存未满
		// fmt.Println("memCacheList insert")
		packId := tcp_pack.GetPackId(data.Bytes())

		s.memCacheList.PushBack(data)
		loglib.Info(fmt.Sprintf("add pack %s to mem cache, total:%d", packId, s.memCacheList.Len()))
	}
}

func (s *Sender) writeToFile(data bytes.Buffer) {
	//写入文件
	filename := createFileName(s.id)
	//创建文件
	_, err := os.Create(filename)
	lib.CheckError(err)

	d := data.Bytes()

	packId := tcp_pack.GetPackId(d)

	loglib.Info(fmt.Sprintf("save pack %s to file %s len:%d", packId, filename, len(d)))
	err = ioutil.WriteFile(filename, d, os.ModeTemporary)
	if err != nil {
		loglib.Error("write to file " + filename + " error:" + err.Error())
		lib.CheckError(err)
	}

	//追加fileCacheList
	fileList.PushBack(filename)

}

func (s *Sender) sendBuffer(data bytes.Buffer) bool {
	result := s.sendData(data.Bytes(), s.connection.getConn())
	//发送失败，tcp连接可能已经失效，重新建立tcp连接
	if result == false {
		s.connection.reconnect(s.connection.getConn())
		*s.status = -1
		loglib.Info(fmt.Sprintf("reconnected by sendBuffer(),status:%d", *s.status))
	} else {
		*s.status = 1
	}
	return result
}

func (s *Sender) sendData2(data []byte) bool {
	result := s.sendData(data, s.connection.getConn())
	//发送失败，tcp连接可能已经失效，重新建立tcp连接
	if result == false {
		s.connection.reconnect(s.connection.getConn())
		*s.status = -1
		loglib.Info(fmt.Sprintf("reconnected by sendData2(),status:%d", *s.status))
	}
	return result
}

func (s Sender) sendData(data []byte, conn *net.TCPConn) bool {
	if len(data) == 0 {
		return true
	}

	if conn == nil {
		return false
	}

	/*
	   lenBuf := make([]byte, 4)
	   nData := len(data)
	   binary.PutUvarint(lenBuf, uint64(nData))
	   data = append(lenBuf, data...)
	*/

	st := time.Now()
	packId := tcp_pack.GetPackId(data)

	conn.SetDeadline(time.Now().Add(5 * time.Minute)) //设置超时
	loglib.Info(fmt.Sprintf("start sending pack:%s length:%d", packId, len(data)))
	n, err := conn.Write(data)
	ed := time.Now()
	loglib.Info(fmt.Sprintf("end sending pack:%s length:%d elapse:%s", packId, n, ed.Sub(st)))

	lib.CheckError(err)

	//写失败了就不用等应答了，肯定拿不到
	if err == nil {
		conn.SetReadDeadline(time.Now().Add(8 * time.Minute)) //设置超时
		time1 := time.Now()
		var temp []byte = make([]byte, 128)
		count, err := conn.Read(temp)
		if err == nil {
			loglib.Info(fmt.Sprintf("get anwser data len:%d for pack:%s elapse:%s", count, packId, time.Now().Sub(time1)))
		} else {
			loglib.Info(fmt.Sprintf("get anwser data len:%d for pack:%s elapse:%s, error:%s", count, packId, time.Now().Sub(time1), err.Error()))
		}

		temp = temp[:count]
		if string(temp) == "ok" { //发送成功
			return true
		} else if string(temp) == "wrong header" {
			//包头错误,丢弃
			loglib.Info(packId + " has wrong header")
			return true
		} else { //发送失败
			//报警
			return false
		}
	} else {
		loglib.Error(fmt.Sprintf("write pack %d error:%s", packId, err.Error()))
	}
	return false
}
func createFileName(id int) string {
	t := time.Now()
	nanoSecond := strconv.FormatInt(t.UnixNano(), 10)
	filename := "tempfile/senderBufferTempFile_" + strconv.Itoa(id) + "_" + nanoSecond

	return filename

}
