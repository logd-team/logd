package main

import (
	"net"
	"logd/lib"
	"time"
	"fmt"
	"compress/zlib"
	"bytes"
	"io"
	"os"
	// "io/ioutil"
)

type Collector struct {

}

//工厂初始化函数
func CollectorInit() (collector Collector) {
	// var tc TcpClient

	return collector
}

func (collector Collector) StartCollectorServer() {
	service := ":1312"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	lib.CheckError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	lib.CheckError(err)


	for {
		conn, err := listener.Accept()
		lib.CheckError(err)

		go collector.handleConnnection(conn)
		
	}
}

func (collector Collector) handleConnnection(conn net.Conn) {
	defer conn.Close()
	filename := "test.log"
	logFile,err := os.OpenFile(filename,os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666) 
	lib.CheckError(err)
	defer logFile.Close()

	conn.SetReadDeadline(time.Now().Add(30 * time.Minute))
	request := make([]byte, 12800)
	
	for {
		//get consumer id
		requestLen, _ := conn.Read(request)
		if requestLen == 0 {
			return
		}
		// msg := string(request)
		fmt.Println("received:")
		b := bytes.NewBuffer(request)
		r, err := zlib.NewReader(b)
		if err != nil {
			panic(err)
		}

		io.Copy(os.Stdout, r)
		// result,err := ioutil.ReadAll(r)
		// lib.CheckError(err) 
		// _,err = logFile.Write(result)
		// lib.CheckError(err) 
		r.Close()
		
		// c <- msg
		conn.Write([]byte("ok"))
		// conn.Close()
	}
}
