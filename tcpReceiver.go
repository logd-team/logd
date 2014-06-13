package main

import (
	"net"
	"logd/lib"
    "logd/loglib"
	"time"
	"fmt"
	// "compress/zlib"
	"bytes"
    "strings"
	"io/ioutil"
    "os"
    "sync"
    "encoding/binary"
    "encoding/json"
    "crypto/md5"
    "logd/tcp_pack"
)

type TcpReceiver struct {

	buffer chan bytes.Buffer
	receiveFromAddress string

    footPrint map[string]PackAppear   //记录包的MD5, key是md5
    footPrintFile string
    mutex *sync.RWMutex

    wq *lib.WaitQuit      //用于安全退出
}

type PackAppear struct {
    Time int64   //包首次出现的时间戳
    Id string    //包的id
}

//工厂初始化函数
func TcpReceiverInit(buffer chan bytes.Buffer, addr string) (t TcpReceiver) {

	t.buffer = buffer
	t.receiveFromAddress = addr

    t.footPrintFile = getFilePath()
    t.footPrint = t.loadFootPrint(t.footPrintFile)
    t.mutex = &sync.RWMutex{}
    t.wq = lib.NewWaitQuit("tcp receiver", -1)
	return t
}


func getFilePath() string {
    var d = lib.GetBinPath() + "/var"
    if !lib.FileExists(d) {
        os.MkdirAll(d, 0775)
    }
    return d + "/footprint.json"
}
func (t *TcpReceiver) Start() {

	tcpAddr, err := net.ResolveTCPAddr("tcp4", t.receiveFromAddress)
	lib.CheckError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	lib.CheckError(err)

    wg := &sync.WaitGroup{}

    wg.Add(1)
    go t.clearFootPrint(wg)

    //主routine信号处理
    go lib.HandleQuitSignal(func(){
        //接收到信号关闭listenner，此时Accept会马上返回一个nil 的conn
        listener.Close()
        loglib.Info("close tcp receiver's listener.")
    })

    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("tcp receiver panic:%v", err))
        }

        loglib.Info("wait connections finish...")
        wg.Wait()
        loglib.Info("all connections have been processed. quit.")
        close(t.buffer)    //关闭chan
        t.saveFootPrint()

        t.wq.AllDone()
    
    }()

	for {
        conn, err := listener.Accept()
        if conn == nil {
            break
        }
        lib.CheckError(err)
        wg.Add(1)
        go t.handleConnnection(conn, wg)
	}

}

func (t *TcpReceiver) Quit() bool {
    return t.wq.Quit()
}

//包是否出现过
func (t *TcpReceiver) hasAppeared(buf *bytes.Buffer) (PackAppear, bool, string) {
    h := md5.New()
    h.Write(buf.Bytes())
    code := fmt.Sprintf("%x", h.Sum(nil))
    t.mutex.RLock()
    appeared, ok := t.footPrint[code]
    t.mutex.RUnlock()
    return appeared, ok, code
}

func (t *TcpReceiver) clearFootPrint(wg *sync.WaitGroup) {
    defer wg.Done()

    ch1 := make(chan bool)                //用于安全退出
    ch2 := time.After(time.Hour)          //用于定时任务
    go lib.HandleQuitSignal(func(){
        ch1 <- true
    })

loop:
    for {
        select {
            //监听一个chan以便安全退出
            case <- ch1:
                break loop
            case <- ch2:
                //若这个case未执行完，而ch1已可读，select会保证这个case执行完
                now := time.Now().Unix()
                t.mutex.Lock()
                for code, appear := range t.footPrint {
                    if now - appear.Time >= 86400 {
                        delete(t.footPrint, code)
                    }
                }
                t.mutex.Unlock()
                ch2 = time.After(time.Hour)          //用于定时任务
        }
    }
    loglib.Info("clear footprint quit!")
}

func (t *TcpReceiver) handleConnnection (conn net.Conn, wg *sync.WaitGroup) {
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("tcp receiver connection panic:%v", err))
        }
        conn.Close()
        wg.Done()
    }()
    /*
      用于标识收到退出信号后，能否直接退出
      只要接收信号时，包没有收完，都是可退出的，
      发送方会缓存以后重传;
      如果收完了就不能直接退出，可能包已传给下一级处理但是
      却告诉发送方发送失败
    */
    var quit = false      //用于标识是否要退出

    go lib.HandleQuitSignal(func(){
        //关闭连接，避免阻塞在网络io上
        conn.Close() 
        quit = true
    })

    request := make([]byte, 10240 * 1024) //包最大为10m
    
    var packLen int = 0 
    currLen := 0
    var b = new(bytes.Buffer)
    var content = new(bytes.Buffer)
    inAddr := conn.RemoteAddr().String()
    parts := strings.Split(inAddr, ":")
    inIp := parts[0]

    packId := "unkown"

    var routeInfo map[string]string
    var rePull = false              //是否补拉，如果是补拉就不做重复包检验

    loglib.Info("incoming: " + inAddr)

outer:
    for ; !quit; {

        st := time.Now()
        if packLen == 0 {
            conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
            time1 := time.Now()  //时间打点
            // read zlib pack header length        
            buf := make([]byte, 4)
            _, err := conn.Read(buf)
            if err != nil {
                loglib.Error(fmt.Sprintf("conn:%s, get header len, tcp receiver read error:%s, elapse:%s", inAddr, err.Error(), time.Now().Sub(time1)))
                break
            }
            l, _ := binary.Uvarint(buf)
            headerLen := int(l)
            //get pack header
            headerBuf := make([]byte, headerLen)
            time2 := time.Now()
            _, err = conn.Read(headerBuf)
            if err != nil {
                loglib.Error(fmt.Sprintf("conn:%s, get header, tcp receiver read error:%s, elapse:%s", inAddr, err.Error(), time.Now().Sub(time2)))
                break
            }
            
            //是否补拉
            route0 := tcp_pack.ParseHeader(headerBuf)
            if v, ok := route0["repull"]; ok && v == "1" {
                rePull = true
            }else{
                rePull = false
            }

            buf = append(buf, headerBuf...)
            header,_, err := tcp_pack.ExtractHeader(buf)
            if err != nil {
                loglib.Error("wrong format header" + err.Error())
                conn.Write([]byte("wrong header"))
                break
            }
            
            packId = tcp_pack.GetPackId(buf)
            packLen = header.PackLen
            currLen = 0
            routeInfo = make(map[string]string)
            b = new(bytes.Buffer)
            content = new(bytes.Buffer)

            loglib.Info(fmt.Sprintf("conn:%s, start receive pack %s, pack len:%d, header len:%d", inAddr, packId, packLen, headerLen))
            b.Write(buf)

            routeInfo["ip"] = lib.GetIp()
            routeInfo["stage"] = "tcp recv"
            routeInfo["st"] = st.Format("2006-01-02 15:04:05.000")
        }       
        //读包体的超时
        conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
        time3 := time.Now()
        //read enough bytes
        for currLen < packLen {
            requestLen, err := conn.Read(request)
            if requestLen == 0 || err != nil {
                //sender有重发机制，所以可丢弃
                packLen = 0     //设为0以便读取新的包

                ed := time.Now()
                loglib.Warning(fmt.Sprintf("conn:%s, not full! ip:%s, packid:%s, received:%d, end recv:%s, elapse:%s, body elapse:%s, error:%s", inAddr, inIp, packId, currLen, ed, ed.Sub(st), ed.Sub(time3), err.Error()))
                break outer     //连接出错直接跳出外层循环
            }
            currLen += requestLen
            content.Write(request[:requestLen])
        }       
        if packLen > 0 && currLen >= packLen{
            //收完马上应答
            _, err := conn.Write([]byte("ok"))
            if err != nil {
                loglib.Error(fmt.Sprintf("ip:%s, packid:%s received, but response back error:%s", inIp, packId, err.Error()))
            }else{
                loglib.Info(fmt.Sprintf("conn:%s, response to packid:%s", inAddr, packId))
            }
            //避免收到重复包（补拉例外）
            appeared, ok, code := t.hasAppeared(content)
            if !ok || rePull {
                ed := time.Now()
                routeInfo["ed"] = ed.Format("2006-01-02 15:04:05.000")
                routeInfo["elapse"] = ed.Sub(st).String()
                b.Write(content.Bytes())
                vbytes := tcp_pack.Packing(b.Bytes(), routeInfo, true)
                b = bytes.NewBuffer(vbytes)
                t.buffer <- *b
                packAppear := PackAppear{time.Now().Unix(), packId}
                t.mutex.Lock()
                t.footPrint[code] = packAppear   //这里挂过
                t.mutex.Unlock()

                loglib.Info(fmt.Sprintf("conn:%s, finish ip:%s, packid:%s, repull:%v, received:%d, elapse:%s, body elapse:%s", inAddr, inIp, packId, rePull, currLen, ed.Sub(st), ed.Sub(time3)))
            }else{
                loglib.Info(fmt.Sprintf("conn:%s, pack %s repeat %s already appear at %s", inAddr, packId, appeared.Id, time.Unix(appeared.Time, 0)))
            }
            packLen = 0
        }

    }
    loglib.Info("conn finish: " + inAddr)
}


//保存footprint
func (t *TcpReceiver) saveFootPrint() {
    vbytes, err := json.Marshal(t.footPrint)
    if err != nil {
        loglib.Error("marshal footprint error:" + err.Error())
        return
    }
    err = ioutil.WriteFile(t.footPrintFile, vbytes, 0664)
    if err == nil {
        loglib.Info("save footprint success !")
    }else{
        loglib.Error("save footprint error:" + err.Error())
    }
}

func (t *TcpReceiver) loadFootPrint(fname string) map[string]PackAppear {
    fp := make(map[string]PackAppear)
    if lib.FileExists( fname ) {
        vbytes, err := ioutil.ReadFile( fname )
        if err != nil {
            loglib.Error("read footprint file error:" + err.Error())
        }else{
            err = json.Unmarshal(vbytes, &fp)
            if err != nil {
                loglib.Error("unmarshal footprint error:" + err.Error())
            }else{
                loglib.Info("load footprint success !")
            }
        }
    }else{
        loglib.Warning("footprint file " + fname + " not found!")
    }
    return fp
}
