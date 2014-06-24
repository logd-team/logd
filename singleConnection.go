package main

/*
sender通过sendData调用SingleConnection:
s.connnection = SingleConnectionInit(address,bakAddress)

result := s.sendData(data.Bytes(),s.connnection.getConn())

*/

import (
    "fmt"
	"net"
	"logd/lib"
	"logd/loglib"
)

type SingleConnection struct {
	addr string
	bakAddr string
	currentAddr string
	conn *net.TCPConn

	failedTimes int
	max_try_times int
}

func SingleConnectionInit(address string,bakAddress string) (sc *SingleConnection){

	sc = new(SingleConnection)
	sc.addr = address
	sc.currentAddr = sc.addr
	sc.bakAddr = bakAddress
	
	sc.failedTimes = 0
	sc.max_try_times = 30

	sc.initConnection()


	return sc
}

//init conn list from addrMap
func (sc *SingleConnection) initConnection() {
	newConn,err := createSingleConnection(sc.currentAddr)
 	if err != nil {
 		loglib.Error("init err:" + err.Error())
 	}else {
 		sc.conn = newConn
        loglib.Info(fmt.Sprintf("initialed connection's remote addr %v",sc.conn))
 	}

 	lib.CheckError(err)
}

func createSingleConnection(address string) (conn *net.TCPConn,err error) {

	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
	lib.CheckError(err)
	if err != nil {
		return nil,err
	}
	conn, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
        loglib.Error("get connection from " + address + " failed! Error:" + err.Error())
		return nil,err
	}else {
		loglib.Info("get connection from " + address + " success! remote addr " + conn.RemoteAddr().String() )
	}
	lib.CheckError(err)
	
	return conn,nil
}

func (sc *SingleConnection) reconnect(conn *net.TCPConn) {

	if (sc.failedTimes < sc.max_try_times) {
		newConn,err := createSingleConnection(sc.currentAddr)
		if err != nil {
			sc.failedTimes++
			
		}else {
			sc.conn = newConn
			sc.failedTimes = 0
		}
	}else {
        tmpAddr := sc.currentAddr
		sc.currentAddr = sc.bakAddr
        sc.bakAddr = tmpAddr
        sc.failedTimes = 0
        loglib.Warning("try bakup address:" + sc.currentAddr)
		newConn,err := createSingleConnection(sc.currentAddr)
		if err == nil {
            loglib.Warning("use bakup address:" + sc.currentAddr)
			sc.conn = newConn
		}
	}

}

func (sc *SingleConnection) getConn() *net.TCPConn {
	
	//log.Println("get connection's remote addr ",sc.conn)
	return sc.conn

}

func (sc *SingleConnection) close() {
    if sc.conn != nil {
        sc.conn.Close()
    }
}

