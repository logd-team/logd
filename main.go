package main
import (
    "os"
    "fmt"
    "runtime"
    "runtime/pprof"
    // "bytes"
    // "time"
    "logd/monitor"
    // "logd/heart_beat"
    "logd/lib"
    // "strconv"
    // "strings"
    "logd/loglib"
    "flag"
    "log"
)
func main() {
    runtime.GOMAXPROCS(runtime.NumCPU())

    var cpuProfile = flag.String("cpuprofile", "", "profile file")
    var memProfile = flag.String("memprofile", "", "mem profile")
    flag.Parse()

    if *cpuProfile != "" {
        f, err := os.Create(*cpuProfile)
        if err != nil {
            log.Fatal(err)
        }   
        pprof.StartCPUProfile(f)
        defer pprof.StopCPUProfile()
    }           

    cfgFile := flag.Arg(1)
    cfg := lib.ReadConfig(cfgFile)
    loglib.Init(cfg["logAgent"])
    hbPort, ok := cfg["monitor"]["hb_port"]
    if ok {
        loglib.HeartBeatPort = hbPort
    }

    savePid()

	switch flag.Arg(0) {
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

    if *memProfile != "" {
        f, err := os.Create(*memProfile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.WriteHeapProfile(f)
        f.Close()
    }
}

func savePid() {
    var d = lib.GetBinPath() + "/var"
    os.MkdirAll(d, 0775)
    fout, _ := os.Create(d + "/logd.pid")
    fmt.Fprintf(fout, "%d", os.Getpid())
    fout.Close()
}
