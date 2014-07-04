package loglib

import (
    "log"
    "time"
    "os"
    "path"
    "sync"
)

type FileLog struct{
    logger *log.Logger 
    fout *os.File
    level int
    dir string
    dateStr string
    mutex *sync.Mutex
}


func NewFileLog(dir string, level int) *FileLog {
    if dir != "-" {
        if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
            err = os.MkdirAll(dir, 0775)
            if err != nil{
                log.Println("Try to create dir [" + dir + "] failed")
                dir = "-"   //创建目录失败，则使用stdout
            }
        }
    }
    logger, fout := initFileLogger(dir)
    dateStr := time.Now().Format("20060102")
    mutex := &sync.Mutex{}
    return &FileLog{logger:logger, fout:fout, dir:dir, level: level, dateStr: dateStr, mutex:mutex}
}

func initFileLogger(dir string) (logger *log.Logger, w *os.File) {
    //增加输出到标准输出，便于程序调试
    if dir == "-" {
        logger = log.New(os.Stdout, "", log.LstdFlags)
        w = os.Stdout
    }else{
        var fname = "logd.log." + time.Now().Format("20060102")
        logFile := path.Join(dir, fname)
        fout, err := os.OpenFile(logFile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0644)
        if err != nil {
            log.Println("Open log file [" + logFile + "] failed: ", err)
        }else{
            logger = log.New(fout, "", log.LstdFlags | log.Lshortfile)
            w = fout
        }
    }
    return logger, w
}

func (l *FileLog) logging(level int, msg string) {
    if level >= 0  && level < len(prefixes) {
        if l.dir != "-" {
            dateStr := time.Now().Format("20060102")
            l.mutex.Lock()
            if dateStr != l.dateStr {
                logger, fout := initFileLogger(l.dir)   //自动切割
                if logger != nil && fout != nil {       //出错则不切割
                    l.fout.Close()
                    l.dateStr = dateStr
                    l.logger, l.fout = logger, fout
                }
            }
            l.mutex.Unlock()
        }
        l.logger.Println("[" + prefixes[level] + "] " + msg)
    }    
}

//设置日志级别，低于该级别的日志将不输出
func (l *FileLog) SetLevel(level int) bool {
    if level >= 0  && level < len(prefixes) {
        l.level = level
        return true
    }else {
        log.Println("invalid log level")
    }
    return false
    
}

func (l *FileLog) Debug(msg string) {
    if l.level == DEBUG {
       l.logging(DEBUG, msg) 
    }
}

func (l *FileLog) Info(msg string) {
    if l.level <= INFO {
       l.logging(INFO, msg) 
    }
}

func (l *FileLog) Warning(msg string) {
    if l.level <= WARNING {
       l.logging(WARNING, msg) 
    }
}

func (l *FileLog) Error(msg string) {
    if l.level <= ERROR {
       l.logging(ERROR, msg) 
    }
}
