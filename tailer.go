package main

import (
    "log"
    "fmt"
    "os"
    "time"
    "os/exec"
    "path/filepath"
    "runtime"
    "strconv"
    "strings"
    "bufio"
    "logd/lib"
    "logd/loglib"
)

var logFileKey = "log_file"   //配置文件中的key名
var recordFileKey = "record_file"
var recordFile = "line.rec"

type Tailler struct{
    lineNum int           //记录已扫过的行数
    recordPath string
    config map[string]string
    //receiver的buffer size，每tail这么多行就记录，不够就不记录，
    //简化重启后包的id设置逻辑，这样只有最后一个包可能少于buffer size
    recvBufSize int
    wq *lib.WaitQuit
}

func NewTailler(config map[string]string) *Tailler{
    val, ok := config[logFileKey]
    if !ok || val == "" {
        log.Fatal("config need log_file!")
    }
    val, ok = config[recordFileKey]
    if !ok || val == "" {
        config[recordFileKey] = getRecordPath()
    }
    lineNum := getLineRecord(config[recordFileKey])
    wq := lib.NewWaitQuit("tailler")
    bufSize, _ := strconv.Atoi(config["recv_buffer_size"])

    return &Tailler{lineNum: lineNum, config: config, recvBufSize:bufSize, wq: wq}
}

func getRecordPath() string {
    d, _ := filepath.Abs(filepath.Dir(os.Args[0]))
    d = filepath.Join(d, "var")
    if _, err := os.Stat(d); err != nil && os.IsNotExist(err) {
        os.MkdirAll(d, 0775)
    }
    return d + "/" + recordFile
}

func getLineRecord(path string) int {
    fin, err := os.Open(path)
    if err != nil {
        _, f, l, _ := runtime.Caller(0)
        log.Printf("%s:%d open line record `%s` error\n", f, l, path)
        return 0           //从最后开始读
    }
    var line int
    _, err = fmt.Fscanf(fin, "%d", &line)
    fin.Close()
    if err != nil {
        _, f, l, _ := runtime.Caller(0)
        log.Printf("%s:%d read line record `%s` error\n", f, l, path)
        return 0
    }
    return line
} 

func saveLineRecord(path string, lineNum int) {
    fout, err := os.Create(path)
    defer fout.Close()
    if err != nil {
        log.Println("save line record error!", err)
        return
    }
    _, err = fmt.Fprintf(fout, "%d", lineNum)
    if err != nil {
        log.Println("Write line record error", err)
        return
    }
    log.Println("save line record success!")
}


func (tlr *Tailler) Tailling(receiveChan chan string){
    var n_lines = ""
    if tlr.lineNum > 0 {
        n_lines = fmt.Sprintf("+%d", tlr.lineNum+1)  //略过已经tail过的行
    }else{
        n_lines = "0"                                //从最后开始
    }

    var quit = false
    //收尾工作
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("tailler panic:%v", err))
        }
    
        close(receiveChan)
        //如果是quit，丢弃不完整的包
        if quit {
            tlr.lineNum -= tlr.lineNum % tlr.recvBufSize
        }
        saveLineRecord(tlr.config[recordFileKey], tlr.lineNum)

        tlr.wq.AllDone()
    }()

    var path = tlr.config[logFileKey]
    realFile, _ := filepath.EvalSymlinks(path)         //获取符号链所指的文件
    fInfo, _ := os.Stat(path)
    //启动时读取行号，以后都从首行开始
    cmd := exec.Command("tail", "-f", "-n", n_lines, path)
    n_lines = "+1"
    stdout, err := cmd.StdoutPipe()

    if err != nil {
        fmt.Println("open pipe error")
    }

    //系统信号监听
    go lib.HandleQuitSignal(func(){
        quit = true
        if cmd.Process != nil {
            cmd.Process.Kill() //关闭tail命令，不然读取循环无法终止
        }
    })

    //日志切割检测
    go func(){
        for {
            if quit {
                break
            }
            newInfo, _ := os.Stat(path)
            if newInfo != nil && !os.SameFile(fInfo, newInfo) {
                totalLines := tlr.GetTotalLines(realFile)
                loglib.Info(fmt.Sprintf("%s change! previous file: %s, total lines: %d", path, realFile, totalLines))

                //发现日志切割，等待2分钟，以便读完pipe中的内容(pipe比单纯tail慢一个数量级)
                i := 0
                done := false
                for {
                    if tlr.lineNum >= totalLines {
                        done = true
                    }
                    if done || i > 120 {
                        if cmd.Process != nil {
                            cmd.Process.Kill() //关闭tail命令，不然读取循环无法终止
                        }
                        if done {
                            loglib.Info("finish tail " + realFile)
                        }else{
                            loglib.Info("tail " + realFile + " timeout")
                        }
                        break
                    }
                    i++
                    time.Sleep(1 * time.Second)
                }
                fInfo = newInfo
                realFile, _ = filepath.EvalSymlinks(path)
            }
            time.Sleep(10 * time.Millisecond)
        }

    }()

    outer:
    for {
        cmd.Start()
        rd := bufio.NewReader(stdout)
        for line, err := rd.ReadString('\n'); err == nil; line, err = rd.ReadString('\n'){
            //fmt.Print(line)
            if quit {
                break outer
            }
            tlr.lineNum++
            receiveChan <- line
        }
        if err := cmd.Wait(); err != nil {
            loglib.Info("wait sys tail error!" + err.Error())
        }
        loglib.Info(fmt.Sprintf("tailed %d lines", tlr.lineNum))
        if quit {
            break
        }
        // 完整tail一个文件
        receiveChan <- "logfile changed"
        //begin a new file
        tlr.lineNum = 0
        cmd = exec.Command("tail", "-f", "-n", n_lines, path)
        stdout, err = cmd.StdoutPipe()

        if err != nil {
            fmt.Println("open pipe error")
            break
        }
    }
}

func (tlr *Tailler) Quit() bool {
    return tlr.wq.Quit()
}

func (tlr *Tailler) GetLineNum() int {
    return tlr.lineNum
}

func (tlr *Tailler) GetTotalLines(fname string) int {
    cmd := exec.Command("/bin/sh", "-c", `wc -l ` + fname + ` | awk '{print $1}'`)
    out, err := cmd.Output()
    if err == nil {
        n, err := strconv.Atoi(strings.Trim(string(out), "\n"))
        if err != nil {
            loglib.Error("trans total lines " + string(out) + " error: " + err.Error())
        }
        return n
    }
    return 0
}
