package main

import (
"bufio"
"net"
"fmt"
"os"
"logd/lib"
"strconv"
"time"
"logd/loglib"
)

func testLogAgent() {
	loglib.Info("info message")
}
func testClient2() {
	//connect to server
    service := "localhost:1302"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	lib.CheckError(err)
	i := 1
	for {
		conn, err := net.DialTCP("tcp", nil, tcpAddr)
		lib.CheckError(err)
		defer conn.Close()

		fmt.Println("sending:",i)
		_,err = conn.Write([]byte(strconv.Itoa(i)+"\n"))
		// _,err = conn.Write([]byte("hahahhahahhahhahahahahahahhaahahahhhhhhhshdfsadfhasdhfajdhfahdsjfhajlhdfljadhfjhadshfjadhfljas"))
		i++

	    if err != nil {
	        fmt.Println(err)
	    }

	    time.Sleep(1000 * time.Millisecond)
	}

}
func testClient() {
    
    //connect to server
    service := "localhost:1202"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	lib.CheckError(err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	lib.CheckError(err)
	defer conn.Close()

    bio := bufio.NewReader(os.Stdin)
	for {
		//read line
		line, hasMoreInLine, err := bio.ReadLine()
		_ = hasMoreInLine
		_ = err
		data := string(line)
		if len(line) == 0 {
			continue
		}
		if data == "exit" {
			os.Exit(0)
		}
	    fmt.Println("read from stdin",data)

		// // //analyze
	 //    var words []string
	 //    for _,word := range strings.Split(data," ") {
	 //    	words = append(words,word)
	 //    }

	    //sending msg to server
	    msg := data
	    fmt.Println("sending:",msg)
	    // err = gob.NewEncoder(conn).Encode(msg)
	    _,err = conn.Write([]byte(msg))
	    if err != nil {
	        fmt.Println(err)
	    }

	    //get result
		// result, err := ioutil.ReadAll(conn)
		var temp []byte = make([]byte,1000)
		_,err = conn.Read(temp)
		// lib.CheckError(err)
		fmt.Println("received from server:",string(temp))
	}
}


// func testGlobalList() {
// 	gl := lib.GlobalListInit()
// 	var once sync.once
// 	once.Do(gl.Setup)
// }


