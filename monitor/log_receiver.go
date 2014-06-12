package monitor

import (
	"encoding/json"
	"fmt"
	"logd/db"
	"logd/loglib"
	"logd/tcp_pack"
	"net"
	"sync"
	"time"
)

var errorLogTable = "service_error_log"

type LogReceiver struct {
	port      int
	dbConn    *db.Mysql
	ipRoleMap map[string]string
	mutex     *sync.RWMutex
}

func NewLogReceiver(port int, dbConn *db.Mysql, ipRoleMap map[string]string, mutex *sync.RWMutex) *LogReceiver {
	return &LogReceiver{port, dbConn, ipRoleMap, mutex}
}

func (lr *LogReceiver) Run() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", lr.port))
	if err != nil {
		loglib.Error("[log receiver] " + err.Error())
		return
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			loglib.Error("[log receiver] " + err.Error())
			return
		}
		go lr.handleConnection(conn)
	}

}

func (lr *LogReceiver) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		var m map[string]string
		n, buf := tcp_pack.UnPack(conn)
		if n <= 0 {
			break
		}
		err := json.Unmarshal(buf, &m)
		if err == nil {
			_, ok := m["req"]
			if ok {
				m = lr.handleRegister(m)
				data, _ := json.Marshal(m)
				conn.Write(tcp_pack.Pack(data))
			} else {
				lr.handleLog(m)
			}
		}
	}
}

func (lr *LogReceiver) AddLog(timeStr string, ip string, role string, logType string, msg string) (db.MysqlResult, error) {
	return lr.dbConn.Exec("insert into "+errorLogTable+"(ctime, ip, role, error_type, error_msg) values(?,?,?,?,?)", timeStr, ip, role, logType, msg)
}

//处理日志上报
func (lr *LogReceiver) handleLog(logInfo map[string]string) {
	ip, ok := logInfo["ip"]
	port, ok := logInfo["port"]
	remoteAddr := ip
	if port != "" {
		remoteAddr += ":" + port
	}
	role, ok := lr.ipRoleMap[remoteAddr]
	if !ok {
		role = ""
	}
	res, err := lr.AddLog(logInfo["time"], remoteAddr, role, logInfo["type"], logInfo["msg"])
	if err == nil {
		loglib.Info(fmt.Sprintf("[log receiver] add %d error log", res.NumRows))
	}

}

//处理注册请求
func (lr *LogReceiver) handleRegister(registerInfo map[string]string) map[string]string {
	req, ok := registerInfo["req"]
	m := map[string]string{"err": "-1", "msg": "unkown request <" + req + ">"}
	ip, _ := registerInfo["ip"]
	port, _ := registerInfo["port"]
	hostname, _ := registerInfo["hostname"]
	role, _ := registerInfo["role"]
	addr := ip
	loglib.Info(ip + ":" + port + " " + req)

	if ok && (req == "register" || req == "unregister") {
		if port != "" {
			addr = ip + ":" + port
		}
		if req == "register" {
			ret, err := lr.register(ip, port, role, hostname)
			if ret {
				lr.mutex.Lock()
				lr.ipRoleMap[addr] = role
				lr.mutex.Unlock()

				m["err"] = "0"
				m["msg"] = "success"
			} else {
				m["err"] = "1"
				if err != nil {
					m["msg"] = err.Error()
				} else {
					m["msg"] = "done"
				}
			}
		} else if req == "unregister" {
			ret, err := lr.unRegister(ip, port)
			if ret {
				lr.mutex.Lock()
				delete(lr.ipRoleMap, addr)
				lr.mutex.Unlock()

				m["err"] = "0"
				m["msg"] = "success"
			} else {
				m["err"] = "1"
				if err != nil {
					m["msg"] = err.Error()
				} else {
					m["msg"] = "done"
				}
			}
		}
	}
	loglib.Info("[monitor] " + ip + ":" + port + " " + role + " " + req + ": " + m["msg"])
	return m
}

//工作节点注册
func (lr *LogReceiver) register(ip string, port string, role string, hostname string) (bool, error) {
	t := time.Now().Format("2006-01-02 15:04:05")
	res, err := lr.dbConn.Exec("insert into "+registerTable+"(ip, port, role, hostname, ctime) values(?, ?, ?, ?, ?) on duplicate key update role=values(role), hostname=values(hostname), ctime=values(ctime)", ip, port, role, hostname, t)
	if err == nil && res.NumRows >= 0 {
		return true, err
	}
	return false, err
}

//工作节点取消注册
func (lr *LogReceiver) unRegister(ip string, port string) (bool, error) {
	res, err := lr.dbConn.Exec("delete from "+registerTable+" where ip=? and port=?", ip, port)
	if err == nil && res.NumRows >= 0 {
		return true, err
	}
	return false, err
}
