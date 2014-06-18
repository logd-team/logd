package main

import (
	"compress/zlib"
	"bytes"
	"os"
    "fmt"
    "strconv"
    "encoding/binary"
    "path/filepath"
    "baidu.com/etl"
    "logd/lib"
    "io"
    "sync"
    "time"
    "logd/tcp_pack"
    "logd/integrity"
    "logd/loglib"
)

type etlOutputer struct {

	buffer chan bytes.Buffer
    saveDir string
    dataDir string
    headerDir string
    icDir string
    etlDir string
    etlDoneDir string
    etlFailDir string
    writers map[string]*os.File    //存日志的fd
    headerWriters map[string]*os.File   //存header的fd
    config  map[string]string
    ic *integrity.IntegrityChecker
    wq *lib.WaitQuit
}


//工厂初始化函数
func EtlOutputerInit(buffer chan bytes.Buffer, config map[string]string) (e etlOutputer) {

	e.buffer = buffer
    saveDir, _ := config["save_dir"]
    e.saveDir = saveDir
    e.dataDir = filepath.Join(saveDir, "log_data")
    e.headerDir = filepath.Join(saveDir, "headers")
    e.icDir = filepath.Join(saveDir, "received")
    e.etlDir = filepath.Join(saveDir, "etl")
    e.etlDoneDir = filepath.Join(saveDir, "etl_done")
    e.etlFailDir = filepath.Join(saveDir, "etl_fail")
    e.ic = integrity.NewIntegrityChecker(e.icDir)

    os.MkdirAll(e.dataDir, 0775)
    os.MkdirAll(e.headerDir, 0775)
    os.MkdirAll(e.etlDir, 0775)
    os.MkdirAll(e.etlDoneDir, 0775)
    os.MkdirAll(e.etlFailDir, 0775)

    e.writers = make(map[string]*os.File)
    e.headerWriters = make(map[string]*os.File)
    e.config = config
    e.wq = lib.NewWaitQuit("etl outputer", -1)   //不限退出超时时间，以便etl能做完
    
	return e
}

func (e *etlOutputer) Start() {
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("etl outputer panic:%v", err))
        }
    
        e.wq.AllDone()
    }()

    spiderList, _ := e.config["spider_list"]
    colsFile , _ := e.config["columns_file"]

    if colsFile != "" {
        e.runEtl(spiderList, colsFile)
    }else{
        loglib.Error("[error] miss columns map file!")
    }
}

func (e *etlOutputer) Quit() bool {
    return e.wq.Quit()
}

func (e *etlOutputer) runEtl(spiderList string, colsFile string) {
    wg := &sync.WaitGroup{}
    fkeyChan := make(chan string, 100)
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("runEtl() panic:%v", err))
        }
        
        e.ic.SaveStatus()
        close(fkeyChan)
        //等待etl routine结束
        wg.Wait()
    }()

    for i:=0; i<2; i++ {
        wg.Add(1)
        go e.doEtl(fkeyChan, e.dataDir, e.etlDir, e.etlDoneDir, e.etlFailDir, spiderList, colsFile, wg)
    }
    nextCheckTime := time.Now().Add(10 * time.Minute)
    //使用range遍历，方便安全退出，只要发送方退出时关闭chan，这里就可以退出了
    for b := range e.buffer {
        loglib.Info(fmt.Sprintf("pack in chan: %d", len(e.buffer)))
        buf := make([]byte, 4) 
        bp := &b
        bp.Read(buf)

        l, _ := binary.Uvarint(buf)
        headerLen := int(l)
        //get pack header
        buf = make([]byte, headerLen)  
        bp.Read(buf)
        header := tcp_pack.ParseHeader(buf)

        r, err := zlib.NewReader(bp)
        if err != nil {
            loglib.Error("zlib reader Error: " + err.Error())
        }else{
            lines, _ := strconv.Atoi(header["lines"])
            done := false
            if header["done"] == "1" {
                done = true
            }
            e.ic.Add(header["ip"], header["hour"], header["id"], lines, done)

            writerKey := header["ip"] + "_" + header["hour"]
            fout := e.getWriter(e.writers, e.dataDir, writerKey)

            buf = append(buf, '\n')
            /*
            //一头一尾写头信息，节省硬盘
            n, err := fout.Write(buf)
            if err != nil {
                loglib.Info(fmt.Sprintf("write %s %d %s", writerKey, n, err.Error()))
            }
            */
            nn, err := io.Copy(fout, r)
            if err != nil {
                loglib.Warning(fmt.Sprintf("save %s_%s_%s error:%s, saved:%d", header["ip"], header["hour"], header["id"], err, nn))
            }
            //fout.Write(buf)
            //单独存一份header便于查数
            fout = e.getWriter(e.headerWriters, e.headerDir, writerKey)
            n, err := fout.Write(buf)
            if err != nil {
                loglib.Info(fmt.Sprintf("writer header %s %d %s", writerKey, n, err.Error()))
            }
            //增加十分钟check一次的规则，避免done包先到，其他的包未到，则可能要等到下一小时才能check
            if done || time.Now().Unix() > nextCheckTime.Unix() {
                hourFinish, _ := e.ic.Check()
                for ip, hours := range hourFinish {
                    for _, hour := range hours {
                        writerKey = ip + "_" + hour
                        e.closeWriter(e.writers, writerKey)
                        e.closeWriter(e.headerWriters, writerKey)
                        loglib.Info(fmt.Sprintf("fkeychan %d", len(fkeyChan)))
                        fkeyChan <- writerKey
                    }
                }
                nextCheckTime = time.Now().Add(10 * time.Minute)
            }

            r.Close()
        }
    }
}

func (e *etlOutputer) getWriter(writers map[string]*os.File, parentDir string, key string) *os.File {
    w, ok := writers[key]
    if !ok || w == nil {
        fname := filepath.Join(parentDir, key)
        w1, err := os.OpenFile(fname, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
        writers[key] = w1
        w = w1
        if err != nil {
            loglib.Error(fmt.Sprintf("file outputer create writer: %s error: %s", fname, err.Error()))
        }
    }
    return w
}

func (e *etlOutputer) closeWriter(writers map[string]*os.File, key string) {
    w, ok := writers[key]
    if ok {
        if w != nil {
            w.Close()
        }
        delete(writers, key)
    }
}

func (e *etlOutputer) doEtl(fkeyChan chan string, logDataDir string, etlDir string, etlDoneDir string, etlFailDir string, spiderList string, colsFile string, wg *sync.WaitGroup) {
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("doEtl() panic:%v", err))
        }
    
        wg.Done()
    }()
    loglib.Info("etl routine start")
    for fkey := range fkeyChan {
        d := etl.NewDispatcher(colsFile, etlDir, 5, fkey)
        g := etl.NewGlobalHao123(spiderList, 100, 200, 8, d)
        go g.Start(false)
        
        fname := filepath.Join(logDataDir, fkey)
        loglib.Info("start etl for " + fname)

        err := g.ParseFile(fname)
        g.Wait()
        // etl success
        // mark success
        if err == nil {
            //采用循环，增加打tag的成功率
            for i:=0; i<5; i++ {
                fd, err := os.Create(filepath.Join(etlDoneDir, fkey)) 
                if err == nil {
                    fd.Close()
                    loglib.Info("finish etl for " + fname)
                    break
                }else{
                    loglib.Warning("mark etl done for " + fname + " failed! error: " + err.Error())
                }
            }
        }else{
            //采用循环，增加打tag的成功率
            for i:=0; i<5; i++ {
                fd, err := os.Create(filepath.Join(etlFailDir, fkey)) 
                if err == nil {
                    fd.Close()
                    loglib.Info("failed etl for " + fname)
                    break
                }else{
                    loglib.Warning("mark etl fail for " + fname + " failed! error: " + err.Error())
                }
            }
            
        }
    }
    loglib.Info("etl routine finish")
}
