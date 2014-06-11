package monitor

import (
    "strconv"
    "time"
    "sync"
    "logd/heart_beat"
    "logd/db"
    "logd/lib"
    "logd/loglib"
)

var registerTable = "registered_node"

type Monitor struct {
    configFile string
    ipRoleMap map[string]string
    receiver *LogReceiver
    hbChecker *heart_beat.HeartBeatChecker
    mutex *sync.RWMutex
    checkInterval int
    dbConn *db.Mysql
}

func New(configFile string) *Monitor {
    //new Monitor
    monitor := new(Monitor)
    monitor.configFile = configFile
    config := lib.ReadConfig(configFile)

    // heart beat checker
    cfg, ok := config["heart_beat"]
    if !ok {
        loglib.Error("miss heart beat config")
        return nil
    }
    checkInterval, _ := strconv.Atoi(cfg["check_interval"])
    delete(cfg, "check_interval")
    mutex := &sync.RWMutex{}

    monitor.mutex = mutex
    monitor.checkInterval = checkInterval

    hbChecker := heart_beat.NewHeartBeatChecker()
    monitor.hbChecker = hbChecker

    //log receiver
    cfg, ok = config["receiver"]
    if !ok {
        loglib.Error("miss receiver config!")
        return nil
    }
    mysql := db.NewMysql(cfg["db_host"], cfg["db_port"], cfg["db_uname"], cfg["db_passwd"], cfg["db_db"], cfg["db_charset"])
    monitor.dbConn = mysql
    monitor.ipRoleMap = getIpRoleMap(mysql)
    recvPort, _ := strconv.Atoi(cfg["recv_port"])
    receiver := NewLogReceiver(recvPort, mysql, monitor.ipRoleMap, monitor.mutex)
    monitor.receiver = receiver

    return monitor
}

func (this *Monitor) Run() {
    //错误日志
    go this.receiver.Run()

    //心跳检测
    i := 0
    for {
        ips := make([]string, 0)
        this.mutex.Lock()
        //没检查3次就从数据库同步一次ip信息
        //避免有些节点没有注册上就无法被监控
        //没有注册上可以直接在数据库添加
        if i > 2 {
            this.ipRoleMap = getIpRoleMap(this.dbConn)
            i = 0
        }
        for k, _ := range this.ipRoleMap {
            ips = append(ips, k)
        }
        this.mutex.Unlock()
        this.hbChecker.CheckAround(ips, this)
        i++
        time.Sleep(time.Duration(this.checkInterval) * time.Second)
    }
}

func getIpRoleMap(dbConn *db.Mysql) map[string]string {
    sql := "select ip, port, role from " + registerTable
    res, err := dbConn.Query(sql)
    m := make(map[string]string)
    if err != nil {
        loglib.Error("read registered nodes failed! Error: " + err.Error())
    }else{
        for _, row := range res.Rows {
            ip := row[0]
            port := row[1]
            role := row[2]
            if port != "" {
                ip = ip + ":" + port
            }
            m[ip] = role
        } 
        loglib.Info("readed working nodes from database.")
    }
    return m
}
func (this *Monitor) Process(results []heart_beat.CheckResult) {
    for _, res := range results {
        role, ok := this.ipRoleMap[ res.Addr ]
        if !ok {
            role = ""
        }
        //错误记录
        if res.Err {
            this.receiver.AddLog(time.Now().Format("2006-01-02 15:04:05"), res.Addr, role, "no heart-beat", res.Msg)
        }else{
            //this.receiver.AddLog(time.Now().Format("2006-01-02 15:04:05"), res.Addr, role, "alive", res.Msg)
        }
    }
}

