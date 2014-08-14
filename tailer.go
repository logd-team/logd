package main

import (
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
var changeStr = "logfile changed"

type Tailler struct{
    logPath string        //日志路径（带时间格式）
    nLT []int             //logPath中时间格式前后的字符数
    currFile string       //当前tail的文件
    fileHour time.Time    //当前日志文件名上的小时
    hourStrFmt string
    lineNum int           //记录已扫过的行数
    goFmt string          //时间格式
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
        loglib.Error("config need log_file!")
        os.Exit(1)
    }
    logPath := val
    val, ok = config[recordFileKey]
    if !ok || val == "" {
        config[recordFileKey] = getRecordPath()
    }
    lineNum, fname := getLineRecord(config[recordFileKey])
    goFmt, nLT := extractTimeFmt(logPath)
    if goFmt == "" {
        loglib.Error("log path has no time format!")
        os.Exit(1)
    }
    wq := lib.NewWaitQuit("tailler")
    bufSize, _ := strconv.Atoi(config["recv_buffer_size"])

    return &Tailler{logPath: logPath, nLT:nLT, currFile: fname, hourStrFmt: "2006010215", lineNum: lineNum, goFmt: goFmt, recordPath: config[recordFileKey], config: config, recvBufSize: bufSize, wq: wq}
}

func getRecordPath() string {
    d, _ := filepath.Abs(filepath.Dir(os.Args[0]))
    d = filepath.Join(d, "var")
    if _, err := os.Stat(d); err != nil && os.IsNotExist(err) {
        os.MkdirAll(d, 0775)
    }
    return d + "/" + recordFile
}
/*
* 行数为非负值表示已tail的行数
* 行数为负值则都将从文件末尾开始
* 自动保存的行数只可能是bufSize的倍数或者文件总行数
*/
func getLineRecord(path string) (line int, fname string) {
    fin, err := os.Open(path)
    if err != nil {
        _, f, l, _ := runtime.Caller(0)
        loglib.Error(fmt.Sprintf("%s:%d open line record `%s` error\n", f, l, path))
        return -1, ""           //从最后开始读
    }
    var txt string
    var lineStr = ""
    //只读第一行
    scanner := bufio.NewScanner(fin)
    for scanner.Scan() {
        txt = strings.Trim(scanner.Text(), " ")
        break
    }
    fin.Close()
    parts := strings.Split(txt, " ")
    if len(parts) == 2 {
        fname = parts[0]
        lineStr = parts[1]
    }else{
        lineStr = parts[0]
    }
    line, err = strconv.Atoi(lineStr)
    if err != nil {
        loglib.Error("convert line record error:" + err.Error())
        line = -1
    }
    return line, fname
} 

func saveLineRecord(path string, fname string, lineNum int) {
    fout, err := os.Create(path)
    defer fout.Close()
    if err != nil {
        loglib.Error("save line record error: " + err.Error())
        return
    }
    _, err = fmt.Fprintf(fout, "%s %d", fname, lineNum)
    if err != nil {
        loglib.Error("Write line record error" + err.Error())
        return
    }
    loglib.Info("save line record success!")
}
//从带时间格式的路径中分离出时间格式，并转为go的格式
//格式由<>括起
func extractTimeFmt(logPath string) (goFmt string, nLT []int ) {
    size := len(logPath)
    unixFmt := ""
    // 格式前面的字符数
    nLeading := size
    // '<'的位置
    lPos := strings.Index(logPath, "<")
    // 格式后面的字符数
    nTailling := 0
    // '>'的位置
    tPos := strings.LastIndex(logPath, ">")
    if lPos > 0 && tPos > 0 && lPos < tPos {
        nLeading = lPos
        nTailling = size - tPos - 1
        unixFmt = logPath[ lPos + 1 : tPos ]  //+1,-1是扔掉<>
    }
    goFmt = transFmt(unixFmt)
    nLT = []int{nLeading, nTailling}
    return

}
//unix的时间格式的文件名转为go时间格式的
func transFmt(unixFmt string) string {
    if unixFmt == "" {
        return ""
    }
    var timeFmtMap = map[string]string{"%Y":"2006", "%m":"01", "%d":"02", "%H":"15"}
    fmt := unixFmt
    for k, v := range timeFmtMap {
        fmt = strings.Replace(fmt, k, v, -1) 
    }   
    return fmt 
}
//从日志文件名获取时间，不依赖系统时间
func (this *Tailler) getTimeFromLogName(name string) (time.Time, error) {
    size := len(name)
    timePart := ""
    if this.nLT[0] < size && this.nLT[1] < size {
        timePart = name[ this.nLT[0] : size - this.nLT[1] ]
    }
    layout := this.goFmt

    loc, _ := time.LoadLocation("Local")
    t, err := time.ParseInLocation(layout, timePart, loc)
    if err != nil {
        loglib.Error("parse " + timePart + " against " + layout + " error:" + err.Error())
    }
    return t, err 
}
//根据时间得到日志文件
func (this *Tailler) getLogFileByTime(tm time.Time) string {
    size := len(this.logPath)
    prefix := this.logPath[ 0 : this.nLT[0] ]
    suffix := this.logPath[ size-this.nLT[1] : ]
    return prefix + tm.Format(this.goFmt) + suffix
}

func (this *Tailler) Tailling(receiveChan chan map[string]string) {
    if this.currFile == "" {
        //兼容老格式，老格式无文件路径
        this.currFile = this.getLogFileByTime(time.Now())
    }
    var err error
    this.fileHour, err = this.getTimeFromLogName(this.currFile)
    if err != nil {
        loglib.Error("can't get time from current log file:" + this.currFile + "error:" + err.Error())
        os.Exit(1)
    }
    isQuit := false
    for time.Since(this.fileHour).Hours() >= 1 {
        //说明重启时已经跟记录行号时不属于同一个小时了
        isQuit = this.taillingPrevious(this.currFile, this.lineNum, this.fileHour.Format(this.hourStrFmt), receiveChan)
        if isQuit {
            break
        }
        //继续下一个小时
        this.fileHour = this.fileHour.Add(time.Hour)
        this.currFile = this.getLogFileByTime(this.fileHour)
        this.lineNum = 0
    }
    if !isQuit {
        //处理当前这个小时
        this.taillingCurrent(receiveChan)
    }
    close(receiveChan)
    this.wq.AllDone()
}

func (this *Tailler) taillingPrevious(filePath string, lineNum int, hourStr string, receiveChan chan map[string]string) bool {
    var n_lines = ""
    if lineNum >= 0 {
        n_lines = fmt.Sprintf("+%d", lineNum+1)  //略过已经tail过的行
    }else{
        n_lines = "0"                                //从最后开始
    }
    
    loglib.Info("begin previous log: " + filePath + " from line: " + n_lines)
    var quit = false
    //收尾工作
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("tailler panic:%v", err))
        }
    
        //如果是quit，丢弃不完整的包
        if quit {
            lineNum -= lineNum % this.recvBufSize
        }
        saveLineRecord(this.recordPath, filePath, lineNum)
    }()

    //启动时读取行号，以后都从首行开始
    cmd := exec.Command("tail", "-n", n_lines, filePath)
    stdout, err := cmd.StdoutPipe()

    if err != nil {
        loglib.Error("open pipe error")
    }

    //系统信号监听
    go lib.HandleQuitSignal(func(){
        quit = true
        if cmd.Process != nil {
            cmd.Process.Kill() //关闭tail命令，不然读取循环无法终止
        }
    })


    cmd.Start()
    rd := bufio.NewReader(stdout)
    for line, err := rd.ReadString('\n'); err == nil; line, err = rd.ReadString('\n'){
        //fmt.Print(line)
        if quit {
            break
        }
        lineNum++
        m := map[string]string{"hour":hourStr, "line":line}
        receiveChan <- m
        if lineNum % this.recvBufSize == 0 {
            saveLineRecord(this.recordPath, filePath, lineNum)
        }
    }
    if err := cmd.Wait(); err != nil {
        loglib.Info("wait sys tail error!" + err.Error())
    }
    loglib.Info(fmt.Sprintf("%s tailed %d lines", filePath, lineNum))
    if !quit {
        // 完整tail一个文件
        m := map[string]string{"hour":hourStr, "line": changeStr}
        receiveChan <- m
        saveLineRecord(this.recordPath, filePath, lineNum)
    }
    return quit

}
func (this *Tailler) taillingCurrent(receiveChan chan map[string]string) {
    var n_lines = ""
    if this.lineNum >= 0 {
        n_lines = fmt.Sprintf("+%d", this.lineNum+1)  //略过已经tail过的行
    }else{
        n_lines = "0"                                //从最后开始
    }

    var quit = false
    //收尾工作
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("tailler panic:%v", err))
        }
    
        //如果是quit，丢弃不完整的包
        if quit {
            this.lineNum -= this.lineNum % this.recvBufSize
        }
        saveLineRecord(this.recordPath, this.currFile, this.lineNum)

        this.wq.AllDone()
    }()

    //启动时读取行号，以后都从首行开始
    cmd := exec.Command("tail", "-F", "-n", n_lines, this.currFile)
    n_lines = "+1"
    stdout, err := cmd.StdoutPipe()

    if err != nil {
        loglib.Error("open pipe error")
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
        nextHour := this.fileHour.Add(time.Hour)
        nextHourFile := this.getLogFileByTime(nextHour)
        for {
            if quit {
                break
            }
            if lib.FileExists(nextHourFile) {
                currFile := this.currFile
                totalLines := this.GetTotalLines(currFile)
                loglib.Info(fmt.Sprintf("log rotated! previous file: %s, total lines: %d", currFile, totalLines))

                //在kill前进行文件切换，避免kill后新的tail启动时文件名还是旧的
                this.fileHour = nextHour
                this.currFile = nextHourFile
                nextHour = nextHour.Add(time.Hour)
                nextHourFile = this.getLogFileByTime(nextHour)

                //发现日志切割，等待1分钟
                i := 0
                done := false
                for {
                    if this.lineNum >= totalLines {
                        done = true
                    }
                    if done || i > 60 {
                        if cmd.Process != nil {
                            cmd.Process.Kill() //关闭tail命令，不然读取循环无法终止
                        }
                        if done {
                            loglib.Info("finish tail " + currFile)
                        }else{
                            loglib.Info("tail " + currFile + " timeout")
                        }
                        break
                    }
                    i++
                    time.Sleep(time.Second)
                }
            }
            time.Sleep(time.Second)
        }

    }()

    outer:
    for {
        currFile := this.currFile      //缓存当前tail的文件名
        hourStr := this.fileHour.Format( this.hourStrFmt )
        cmd.Start()
        loglib.Info("begin current log: " + currFile)
        rd := bufio.NewReader(stdout)
        for line, err := rd.ReadString('\n'); err == nil; line, err = rd.ReadString('\n'){
            //fmt.Print(line)
            if quit {
                break outer
            }
            this.lineNum++
            m := map[string]string{"hour":hourStr, "line":line}
            receiveChan <- m
            if this.lineNum % this.recvBufSize == 0 {
                saveLineRecord(this.recordPath, currFile, this.lineNum)
            }
        }
        if err := cmd.Wait(); err != nil {
            loglib.Info("wait sys tail error!" + err.Error())
        }
        loglib.Info(fmt.Sprintf("%s tailed %d lines", currFile, this.lineNum))
        if quit {
            break
        }
        // 完整tail一个文件
        m := map[string]string{"hour":hourStr, "line": changeStr }
        receiveChan <- m
        saveLineRecord(this.recordPath, currFile, this.lineNum)
        //begin a new file
        this.lineNum = 0
        cmd = exec.Command("tail", "-F", "-n", n_lines, this.currFile)
        stdout, err = cmd.StdoutPipe()

        if err != nil {
            loglib.Error("open pipe error")
            break
        }
    }
}

func (this *Tailler) Quit() bool {
    return this.wq.Quit()
}

func (this *Tailler) GetLineNum() int {
    return this.lineNum
}

func (this *Tailler) GetTotalLines(fname string) int {
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
