package main
import (
	"net"
	"logd/lib"
	"time"
	"fmt"
)

type TcpClient struct {
	logChan chan string
}

//工厂初始化函数
func TcpClientInit(c chan string) (tc TcpClient) {
	// var tc TcpClient
	tc.logChan = c

	return tc
}

func (tc TcpClient) StartLogAgentServer() {
	service := ":1202"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	lib.CheckError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	lib.CheckError(err)


	for {
		conn, err := listener.Accept()
		lib.CheckError(err)

		go tc.handleConnnection(conn,tc.logChan)
		
	}
}

func (tc TcpClient) handleConnnection(conn net.Conn,c chan string) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	request := make([]byte, 128)
	

	//get consumer id
	requestLen, _ := conn.Read(request)
	if requestLen == 0 {
		return
	}
	msg := string(request)
	fmt.Println(msg)
	c <- msg
	conn.Write([]byte("ok"))
}
