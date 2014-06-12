package loglib

import (
	"encoding/json"
	"log"
	"logd/tcp_pack"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type NetLog struct {
	conn  *net.TCPConn
	level int
	addr  string //tcp address
	ip    string //self ip
}

func NewNetLog(addr string, level int) *NetLog {
	conn, _ := getConnection(addr)
	return &NetLog{conn, level, addr, getIp()}
}

func getIp() string {
	out, _ := exec.Command("/bin/sh", "-c", `/sbin/ifconfig | awk -F"[: ]+" '/inet addr/{print $4}' | head -n 1`).Output()
	return strings.Trim(string(out), "\n")
}

//获取可执行文件的所在路径
func getBinPath() string {
	d, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Println("[get bin path] error:", err)
	}
	return d
}

func getConnection(addr string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		_, f, l, _ := runtime.Caller(1)
		f = strings.Replace(f, getBinPath(), "", -1)
		log.Println(f, ":", l, "[GetConnection] resolve tcp address failed", err)
		return nil, err
	}
	conn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err != nil {
		_, f, l, _ := runtime.Caller(1)
		f = strings.Replace(f, getBinPath(), "", -1)
		log.Println(f, ":", l, "[GetConnection] connect to address:", addr, "failed!")
	}
	return conn, err
}

func (l *NetLog) logging(level int, msg string) {
	if level >= 0 && level < len(prefixes) && l.conn != nil {
		m := map[string]string{"time": time.Now().Format("2006/01/02 15:04:05"), "type": prefixes[level], "msg": msg, "ip": l.ip, "port": HeartBeatPort}
		data, err := json.Marshal(m)
		if err != nil {
			log.Println("marshal net log error:", err)
			return
		}
		data = tcp_pack.Pack(data)
		_, err = l.conn.Write(data)
		if err != nil {
			log.Println("send log failed: ", msg)
			l.conn, _ = getConnection(l.addr) //重连
		} else {
			log.Println("send log :" + msg)
		}
	}

	if l.conn == nil {
		l.conn, _ = getConnection(l.addr) //重连
	}

}

//设置日志级别，低于该级别的日志将不输出
func (l *NetLog) SetLevel(level int) bool {
	if level >= 0 && level < len(prefixes) {
		l.level = level
		return true
	} else {
		log.Println("invalid log level")
	}
	return false

}

func (l *NetLog) Debug(msg string) {
	if l.level == DEBUG {
		l.logging(DEBUG, msg)
	}
}

func (l *NetLog) Info(msg string) {
	if l.level <= INFO {
		l.logging(INFO, msg)
	}
}

func (l *NetLog) Warning(msg string) {
	if l.level <= WARNING {
		l.logging(WARNING, msg)
	}
}

func (l *NetLog) Error(msg string) {
	if l.level <= ERROR {
		l.logging(ERROR, msg)
	}
}
