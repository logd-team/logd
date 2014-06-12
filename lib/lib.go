package lib

import (
	"bytes"
	"compress/zlib"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

func CheckError(err error) {
	if err != nil {
		// _,file,line,_ := runtime.Caller(1)
		// fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error(),file,"error line:",line)

		log.Println(err.Error())

		var pc []uintptr = make([]uintptr, 100)
		i := runtime.Callers(0, pc)
		i -= 1

		for j := 2; j < i; j++ {
			// _,_,line,_ := runtime.Caller(j)
			f := runtime.FuncForPC(pc[j])
			if f.Name() == "main.main" {
				break
			}

			file, line := f.FileLine(pc[j])
			log.Println("statck info======", file, f.Name(), line)
		}
		// os.Exit(1)
	}
}

func StringTrimLast(s string) string {
	if len(s) == 0 {
		return s
	} else {
		return s[0 : len(s)-1]
	}
}

func Unzip(data []byte) []byte {
	b := bytes.NewBuffer(data)
	r, err := zlib.NewReader(b)
	result, err := ioutil.ReadAll(r)
	CheckError(err)
	return result
}

func GetFilelist(path string) (fileList []string) {

	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		//println(path)
		fileList = append(fileList, path)
		return nil
	})
	if err != nil {
		log.Printf("filepath.Walk() returned %v\n", err)
	}

	return fileList
}
func GetIp() string {
	out, _ := exec.Command("/bin/sh", "-c", `/sbin/ifconfig | awk -F"[: ]+" '/inet addr/{print $4}' | head -n 1`).Output()
	return strings.Trim(string(out), "\n")
}

func GetHostname() string {
	out, _ := exec.Command("/bin/sh", "-c", `uname -n`).Output()
	return strings.Trim(string(out), "\n")
}

//获取可执行文件的所在路径
func GetBinPath() string {
	d, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Println("[get bin path] error:", err)
	}
	return d
}

//检查文件或目录是否存在
func FileExists(f string) bool {
	if _, err := os.Stat(f); err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func HandleSignal(f func(), sig ...os.Signal) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, sig...)
	s := <-ch
	f()

	log.Println("get signal:", s)
}

func HandleQuitSignal(f func()) {
	HandleSignal(f, syscall.SIGINT, syscall.SIGQUIT)
}
