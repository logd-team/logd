package main

import (
    "bytes"
    "time"
    "fmt"
    "logd/heart_beat"
    "logd/lib"
    "logd/loglib"
    "strconv"
    "strings"
)
func logdGo(cfg map[string]map[string]string) {

	receiveChan := make(chan string)
	sendBuffer := make(chan bytes.Buffer)
	r := ReceiverInit(sendBuffer,receiveChan, 2000, 0)

	tc := TcpClientInit(receiveChan)
	//start tcp listener to receive log
	go tc.StartLogAgentServer()
	//start receiver to receive log from tcp listenser
	go r.Start()

	addr := "localhost:1306"
	s := SenderInit(sendBuffer,addr, addr, 0)
	go s.Start()
	for {
		time.Sleep(1000 * time.Second)
	}

}

func tailerGo(cfg map[string]map[string]string) {
    qlst := lib.NewQuitList()

	receiveChan := make(chan string, 10000)       //非阻塞
	sendBuffer := make(chan bytes.Buffer, 500)
    recvBufferSize, _ := strconv.Atoi(cfg["tail"]["recv_buffer_size"])
    tailler := NewTailler(cfg["tail"])
	r := ReceiverInit(sendBuffer, receiveChan, recvBufferSize, tailler.GetLineNum())

	//make a new log tailler
	go tailler.Tailling(receiveChan)
	//start receiver to receive log
	go r.Start()

    //一定要发送方先退出
    qlst.Append(tailler.Quit)
    qlst.Append(r.Quit)
    // heart beat
    port, _ := cfg["monitor"]["hb_port"]
    monAddr, _ := cfg["monitor"]["mon_addr"]
    if port != "" && monAddr != "" {
        hb := heart_beat.NewHeartBeat(port, monAddr, "tail")
        go hb.Run()
        qlst.Append(hb.Quit)
    }

    addrs := strings.Split(cfg["tail"]["send_to"], ",")
    addr := strings.Trim(addrs[0], " ")
    bakAddr := addr
    //有备用地址?
    if len(addrs) > 1 {
        bakAddr = strings.Trim(addrs[1], " ")
    }

    //加大发送并发，sender阻塞会影响tail的进度
    nSenders := 2
    senders, ok := cfg["tail"]["senders"]
    if ok {
        tmp, err := strconv.Atoi(senders)
        if err == nil {
            nSenders = tmp
        }
    }
    for i:=1;i<=nSenders;i++ {
        s := SenderInit(sendBuffer, addr, bakAddr, i)
		go s.Start()
        qlst.Append(s.Quit)
	}
    loglib.Info(fmt.Sprintf("total senders %d", nSenders))

    qlst.HandleQuitSignal()
    qlst.ExecQuit()
}

func collectorGo(cfg map[string]map[string]string) {
    qlst := lib.NewQuitList()

	bufferChan := make(chan bytes.Buffer, 500)
	rAddr := cfg["collector"]["listen"]
	tr := TcpReceiverInit(bufferChan,rAddr)
	go tr.Start()

    qlst.Append(tr.Quit)

    addrs := strings.Split(cfg["collector"]["send_to"], ",")
    addr := addrs[0]
    bakAddr := addr
    //有备用地址?
    if len(addrs) > 1 {
        bakAddr = addrs[1]
    }

    nSenders := 10
    senders, ok := cfg["collector"]["senders"]
    if ok {
        tmp, err := strconv.Atoi(senders)
        if err == nil {
            nSenders = tmp
        }
    }
    for i:=1;i<=nSenders;i++ {
		s := SenderInit(bufferChan, addr, bakAddr, i)
		go s.Start()
        qlst.Append(s.Quit)
	}
    loglib.Info(fmt.Sprintf("total senders %d", nSenders))

    // heart beat
    port, _ := cfg["monitor"]["hb_port"]
    monAddr, _ := cfg["monitor"]["mon_addr"]
    if port != "" && monAddr != "" {
        hb := heart_beat.NewHeartBeat(port, monAddr, "collector")
        go hb.Run()
        qlst.Append(hb.Quit)
    }

    qlst.HandleQuitSignal()
    qlst.ExecQuit()
}

func fcollectorGo(cfg map[string]map[string]string) {
	bufferChan := make(chan bytes.Buffer, 500)
	addr := cfg["fcollector"]["listen"]

	fo := FileOutputerInit(bufferChan, cfg["fcollector"]["save_dir"])
	go fo.Start()

	tr := TcpReceiverInit(bufferChan,addr)
	go tr.Start()
	
    qlst := lib.NewQuitList()

    // heart beat
    port, _ := cfg["monitor"]["hb_port"]
    monAddr, _ := cfg["monitor"]["mon_addr"]
    if port != "" && monAddr != "" {
        hb := heart_beat.NewHeartBeat(port, monAddr, "fcollector")
        go hb.Run()

        qlst.Append(hb.Quit)
    }

    qlst.Append(tr.Quit)   //tcpReceiver要先退出
    qlst.Append(fo.Quit)
    qlst.HandleQuitSignal()
    qlst.ExecQuit()
}

func etlcollectorGo(cfg map[string]map[string]string) {
    qlst := lib.NewQuitList()

	bufferChan := make(chan bytes.Buffer, 500)
	addr := cfg["etlcollector"]["listen"]

	eo := EtlOutputerInit(bufferChan, cfg["etlcollector"])
	go eo.Start()

	tr := TcpReceiverInit(bufferChan,addr)
	go tr.Start()
    //一定要发送方先退出
    qlst.Append(tr.Quit)
    qlst.Append(eo.Quit)
	
    // heart beat
    port, _ := cfg["monitor"]["hb_port"]
    monAddr, _ := cfg["monitor"]["mon_addr"]
    if port != "" && monAddr != "" {
        hb := heart_beat.NewHeartBeat(port, monAddr, "etlcollector")
        go hb.Run()
        qlst.Append(hb.Quit)
    }

    qlst.HandleQuitSignal()
    qlst.ExecQuit()
}

