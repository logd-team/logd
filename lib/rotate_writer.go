package lib

import (
    "os"
    "log"
    "io"
    "errors"
    "time"
)

var nilWriterErr = errors.New("get nil rotate writer")

type RotateWriter struct {
    fout io.Writer
    currHour string
    outDir string
}

func NewRotateWriter(outDir string) *RotateWriter {
    currHour, fout := initCurrentHourWriter(outDir)
    return &RotateWriter{fout, currHour, outDir}
}

func initCurrentHourWriter(outDir string) (string, io.Writer) {
    currHour := time.Now().Format("2006010215")
    logFile,err := os.OpenFile(outDir + "/" + currHour, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
    if err != nil {
        log.Println("[error] init rotate writer", currHour, "failed!")
    }
    return currHour, logFile
}

func (this *RotateWriter) Write(p []byte) (n int, err error) {
    hour := time.Now().Format("2006010215")
    if this.currHour != hour {
        this.currHour, this.fout = initCurrentHourWriter(this.outDir)
    }
    if this.fout != nil {
        return this.fout.Write(p)
    }
    return 0, nilWriterErr
}
