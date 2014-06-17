package main
import (
	"fmt"
	"bytes"
	"io/ioutil"
	"os"
	"logd/lib"
    "logd/tcp_pack"
	"net"
	"time"
	"strconv"
	"sync"
    "logd/loglib"
)

var once sync.Once
var fileList *lib.GlobalList = lib.GlobalListInit()

type Sender struct {
	id int
	sBuffer chan bytes.Buffer
	file_mem_folder_name string
    memBuffer chan bytes.Buffer     //sender自己的chan，用于保证sBuffer不阻塞
	connection Connection
	status *int
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
    s.memBuffer = make(chan bytes.Buffer, 20)
	s.file_mem_folder_name = "tempfile"
    //auto make dir
    if _,err := os.Stat(s.file_mem_folder_name); err != nil && os.IsNotExist(err) {
        os.MkdirAll(s.file_mem_folder_name, 0775)
    }
	s.sendToAddress = addr
	s.connection = SingleConnectionInit(s.sendToAddress, bakAddr)
	a := 1
	s.status = &a
    s.wq = lib.NewWaitQuit("sender", -1)
	
	return s
}

//should be run by once
func (s *Sender) reloadFileCache() {
	list := lib.GetFilelist(s.file_mem_folder_name)
	for _,filename := range list {
		// s.fileCacheList.PushBack(filename)
		loglib.Info("reloading:" + filename)
		fileList.PushBack(filename)
	}
}
//从公用的chan读pack到私有的chan，若私有chan已满则写入文件缓存
//保证公用chan不会阻塞
func (s *Sender) pickPacks() {
    for buf := range s.sBuffer {
        select {
            case s.memBuffer <- buf:
                break
            default:
                loglib.Info(fmt.Sprintf("sender %d mem buffer is full, total %d, pub chan:%d", s.id, len(s.memBuffer), len(s.sBuffer)))
                s.writeToFile(buf)
        }
    }
    close(s.memBuffer)
}
//goroutine
func (s *Sender) Start() {
	// conn := s.getConnection()
	//初始化fileCacheList
	once.Do(s.reloadFileCache)

    //收尾工作
	defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("sender %d panic:%v", s.id, err))
        }

        s.saveBufferInChan()

        //s.saveMemCache()

        s.connection.close()

        s.wq.AllDone()

    }()

    go s.pickPacks()
    //var connLost = 0
    var quit = false
    go lib.HandleQuitSignal(func(){
        quit = true
        s.connection.close()
    })
    
    var sendInterval = time.Duration(2000)   //间隔稍大，避免发送文件缓存时因无连接或其他错误进入死循环

    var timeoutChan = time.After(sendInterval * time.Millisecond)
	for ; !quit; {

		select {
			case b := <- s.memBuffer:
				//send b
				result := s.sendBuffer(b)
				if result == false {
                    //改为直接放入文件缓存
                    s.writeToFile(b)
                }

            case <- timeoutChan :
                timeoutChan = time.After(sendInterval * time.Millisecond)

				// send from file
                e := fileList.Remove()
                if e != nil { // file list is not empty
                    filename := e.Value.(string)
                    // fmt.Println("sender ",s.id,": get file :",filename)
                    data,err := ioutil.ReadFile(filename)
                    if (err != nil) {
                        // fmt.Println("sender ",s.id,":",err)
                        if err != os.ErrNotExist {
                            fileList.PushBack(filename)
                        }
                        loglib.Error(fmt.Sprintf("read file cache %s error:%s", filename, err.Error()))
                    }else{
                    
                        packId := tcp_pack.GetPackId(data)//debug info
                        loglib.Info(fmt.Sprintf("read pack %s from file: %s, len: %d", packId, filename, len(data)))//debug info
                        result := s.sendData2(data)
                        if result == true {
                            // s.fileCacheList.Remove(front)
                            // log.Println("sender ",s.id,":removed file:",filename, "for pack", packId)//debug info
                            err = os.Remove(filename)
                            lib.CheckError(err)
                            timeoutChan = time.After(time.Millisecond)  //发送成功，不用再等待
                        }else {
                            fileList.PushBack(filename)
                            // fmt.Println("sender ",s.id,": pushback file :",filename)
                        }
                    }
                }

		}
	}

}

func (s *Sender) Quit() bool {
    return s.wq.Quit()
}

func (s *Sender) saveBufferInChan() {
    loglib.Info(fmt.Sprintf("sender %d begin to save pack in chan", s.id))
    i := 0
    for b := range s.memBuffer {
		s.writeToFile(b)
        i++
    }
    loglib.Info(fmt.Sprintf("sender %d saved num of pack in chan: %d", s.id, i))
}

func (s *Sender) writeToFile(data bytes.Buffer) {
	//写入文件
	filename := createFileName(s.id)
	//创建文件
	_,err := os.Create(filename)
	lib.CheckError(err)

    d := data.Bytes()

    packId := tcp_pack.GetPackId(d)

    loglib.Info(fmt.Sprintf("save pack %s to file %s len:%d", packId, filename, len(d) ))
	err = ioutil.WriteFile(filename, d, 0666)
	if (err != nil) {
        loglib.Error("write to file " + filename + " error:" + err.Error())
		lib.CheckError(err)
	}

	//追加fileCacheList
	fileList.PushBack(filename)

}


func (s *Sender) sendBuffer(data bytes.Buffer) bool {
	result := s.sendData(data.Bytes(),s.connection.getConn())
	//发送失败，tcp连接可能已经失效，重新建立tcp连接
	if result == false {
		s.connection.reconnect(s.connection.getConn())
		*s.status = -1
		loglib.Info(fmt.Sprintf("reconnected by sendBuffer(),status:%d",*s.status))
	}else {
		*s.status = 1
	}
	return result
}

func (s *Sender) sendData2(data []byte) bool {
	result := s.sendData(data,s.connection.getConn())
	//发送失败，tcp连接可能已经失效，重新建立tcp连接
	if result == false {
		s.connection.reconnect(s.connection.getConn())
		*s.status = -1
		loglib.Info(fmt.Sprintf("reconnected by sendData2(),status:%d",*s.status))
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

    conn.SetDeadline(time.Now().Add(5 * time.Minute))  //设置超时
    loglib.Info(fmt.Sprintf("start sending pack:%s length:%d", packId, len(data)))
	n,err := conn.Write(data)
    ed := time.Now()
    loglib.Info(fmt.Sprintf("end sending pack:%s length:%d elapse:%s", packId, n, ed.Sub(st)) )

	lib.CheckError(err)

    //写失败了就不用等应答了，肯定拿不到
    if err == nil {
        conn.SetReadDeadline(time.Now().Add(8 * time.Minute))  //设置超时
        time1 := time.Now()
        var temp []byte = make([]byte,128)
        count,err := conn.Read(temp)
        if err == nil {
            loglib.Info(fmt.Sprintf("get anwser data len:%d for pack:%s elapse:%s", count, packId, time.Now().Sub(time1)))
        }else{
            loglib.Info(fmt.Sprintf("get anwser data len:%d for pack:%s elapse:%s, error:%s", count, packId, time.Now().Sub(time1), err.Error()))
        }

        temp = temp[:count]
        if (string(temp) == "ok") {//发送成功
            return true
        }else if(string(temp) == "wrong header"){
            //包头错误,丢弃
            loglib.Info(packId + " has wrong header")
            return true
        }else {//发送失败
            //报警
            return false
        }
    }else{
        loglib.Error(fmt.Sprintf("write pack %s error:%s", packId, err.Error()))
    }
	return false
}
func createFileName(id int) string {
	t := time.Now()
	nanoSecond :=strconv.FormatInt(t.UnixNano(),10)
	filename := "tempfile/senderBufferTempFile_"+strconv.Itoa(id)+"_"+nanoSecond

	return filename

}

