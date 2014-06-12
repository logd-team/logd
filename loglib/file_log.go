package loglib

import (
	"log"
	"os"
	"path"
	"time"
)

type FileLog struct {
	logger  *log.Logger
	level   int
	dir     string
	dateStr string
}

func NewFileLog(dir string, level int) *FileLog {
	if dir != "-" {
		if _, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0775)
			if err != nil {
				log.Println("Try to create dir [" + dir + "] failed")
				dir = "-" //创建目录失败，则使用stdout
			}
		}
	}
	logger := initFileLogger(dir)
	dateStr := time.Now().Format("20060102")
	return &FileLog{logger: logger, dir: dir, level: level, dateStr: dateStr}
}

func initFileLogger(dir string) (logger *log.Logger) {
	//增加输出到标准输出，便于程序调试
	if dir == "-" {
		logger = log.New(os.Stdout, "", log.LstdFlags)
	} else {
		var fname = "logd.log." + time.Now().Format("20060102")
		logFile := path.Join(dir, fname)
		fout, err := os.OpenFile(logFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.Println("Open log file [" + logFile + "] failed")
		}
		logger = log.New(fout, "", log.LstdFlags|log.Lshortfile)
	}
	return logger
}

func (l *FileLog) logging(level int, msg string) {
	if level >= 0 && level < len(prefixes) {
		if l.dir != "-" {
			dateStr := time.Now().Format("20060102")
			if dateStr != l.dateStr {
				l.logger = initFileLogger(l.dir) //自动切割
			}
		}
		l.logger.Println("[" + prefixes[level] + "] " + msg)
	}
}

//设置日志级别，低于该级别的日志将不输出
func (l *FileLog) SetLevel(level int) bool {
	if level >= 0 && level < len(prefixes) {
		l.level = level
		return true
	} else {
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
