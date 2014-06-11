package main

import (
	"logd/lib"
    "logd/loglib"
	"compress/zlib"
	"bytes"
	"os"
    "fmt"
    "strconv"
    "encoding/binary"
    "bufio"
    "path/filepath"
    "logd/integrity"
    "logd/tcp_pack"
)

type fileOutputer struct {

	buffer chan bytes.Buffer
    saveDir string
    dataDir string
    headerDir string
    icDir string
    writers map[string]*os.File    //存日志的fd
    headerWriters map[string]*os.File   //存header的fd
    ic *integrity.IntegrityChecker

    wq *lib.WaitQuit
}


//工厂初始化函数
func FileOutputerInit(buffer chan bytes.Buffer, saveDir string) (f fileOutputer) {

	f.buffer = buffer
    f.saveDir = saveDir
    f.dataDir = filepath.Join(saveDir, "log_data")
    f.headerDir = filepath.Join(saveDir, "headers")
    f.icDir = filepath.Join(saveDir, "received")
    f.ic = integrity.NewIntegrityChecker(f.icDir)

    os.MkdirAll(f.dataDir, 0775)
    os.MkdirAll(f.headerDir, 0775)

    f.writers = make(map[string]*os.File)
    f.headerWriters = make(map[string]*os.File)

    f.wq = lib.NewWaitQuit("file outputer")
	return f
}

func (f *fileOutputer) Start() {
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("file outputer panic:%v", err))
        }
    
        f.ic.SaveStatus()
        f.wq.AllDone()
    }()

    //使用range遍历，方便安全退出，只要发送方退出时关闭chan，这里就可以退出了
	for b := range f.buffer {
        f.extract(&b)
	}
}

func (f *fileOutputer) Quit() bool {
    return f.wq.Quit()
}

func (f *fileOutputer) extract(bp *bytes.Buffer) {
    buf := make([]byte, 4) 
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
        f.ic.Add(header["ip"], header["hour"], header["id"], lines, done)

        writerKey := header["ip"] + "_" + header["hour"]
        fout := f.getWriter(f.writers, f.dataDir, writerKey)

        //一头一尾写头信息，节省硬盘
        buf = append(buf, '\n')
        fout.Write(buf)
        scanner := bufio.NewScanner(r)
        for scanner.Scan() {
            line := scanner.Bytes()
            line = append(line, '\n')

            _,err = fout.Write(line)
            lib.CheckError(err)
            
        }
        fout.Write(buf)

        //单独存一份header便于查数
        fout = f.getWriter(f.headerWriters, f.headerDir, writerKey)
        fout.Write(buf)

        if done {
            hourFinish, _ := f.ic.Check()
            for ip, hours := range hourFinish {
                for _, hour := range hours {
                    writerKey = ip + "_" + hour
                    f.closeWriter(f.writers, writerKey)
                    f.closeWriter(f.headerWriters, writerKey)
                }
            }
        }

        r.Close()
    }
}

func (f *fileOutputer) getWriter(writers map[string]*os.File, parentDir string, key string) *os.File {
    w, ok := writers[key]
    if !ok || w == nil {
        fname := filepath.Join(parentDir, key)
        w1, err := os.Create(fname)
        writers[key] = w1
        w = w1
        if err != nil {
            loglib.Error(fmt.Sprintf("file outputer create writer: %s error: %s", fname, err.Error()))
        }
    }
    return w
}

func (f *fileOutputer) closeWriter(writers map[string]*os.File, key string) {
    w, ok := writers[key]
    if ok {
        if w != nil {
            w.Close()
        }
        delete(writers, key)
    }
}
