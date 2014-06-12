package main

import (
	"fmt"
	"os"
	"runtime"
	// "bytes"
	// "time"
	"logd/monitor"
	// "logd/heart_beat"
	"logd/lib"
	// "strconv"
	// "strings"
	"logd/loglib"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	cfgFile := os.Args[2]
	cfg := lib.ReadConfig(cfgFile)
	loglib.Init(cfg["logAgent"])
	hbPort, ok := cfg["monitor"]["hb_port"]
	if ok {
		loglib.HeartBeatPort = hbPort
	}

	savePid()

	switch os.Args[1] {
	case "logd":
		logdGo(cfg)

	case "tail":
		tailerGo(cfg)

	case "client":
		testClient2()
	case "collector":
		collectorGo(cfg)

	case "fcollector":
		fcollectorGo(cfg)

	case "etlcollector":
		etlcollectorGo(cfg)
	case "monitor":
		mon := monitor.New(cfgFile)
		mon.Run()
	case "test":
		testClient2()
	default:
		fmt.Println("unknown parameters")
		os.Exit(1)
	}

}

func savePid() {
	var d = lib.GetBinPath() + "/var"
	os.MkdirAll(d, 0775)
	fout, _ := os.Create(d + "/logd.pid")
	fmt.Fprintf(fout, "%d", os.Getpid())
	fout.Close()
}
