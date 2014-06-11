/**************
 * 通用心跳类
 *
 **************/

package heart_beat

import (
    "net"
    "time"
    "logd/lib"
    "logd/loglib"
    "encoding/json"
    "logd/tcp_pack"
)

type HeartBeat struct {
    port string
    monitorAddr string      //monitor地址
    role string             //工作节点的角色
    wq *lib.WaitQuit
}

func NewHeartBeat(port string, monitorAddr string, role string) *HeartBeat {
    wq := lib.NewWaitQuit("heart beat", 5)
    return &HeartBeat{port, monitorAddr, role, wq}
}

func (this *HeartBeat) Run() {
    i := 0

    //尝试注册3次
    for i < 3 && !this.registerSelf(true) {
        i++
        time.Sleep(10 * time.Second)
    }

    l, err := net.Listen("tcp", ":" + this.port)
    if err != nil {
        loglib.Error("heart beat " + err.Error())
        return
    }   
    defer l.Close()

    go lib.HandleQuitSignal(func(){
        this.registerSelf(false)
        l.Close()
        loglib.Info("close heart beat listener.")
    })
    //heart beat 不是太重要，所以退出处理比较简单
    for {
        conn, err := l.Accept()
        if conn == nil {
            //listener关闭
            break
        }
        if err != nil {
            loglib.Error("heart beat " + err.Error())
            break
        }   
        go this.handleConnection(conn)
    }

    this.wq.AllDone()
}

func (this *HeartBeat) Quit() bool {
    return this.wq.Quit()
}

func (this *HeartBeat) handleConnection(conn net.Conn) {
    defer conn.Close()
    conn.Write([]byte("!"))
}

//向monitor注册或取消注册，reg为true表示注册，否则是取消注册，返回true表示成功
func (this *HeartBeat) registerSelf(reg bool) bool {
    conn, err := lib.GetConnection(this.monitorAddr) 
    if err != nil {
        return false
    }
    defer conn.Close()

    req := "register"
    if !reg {
        req = "unregister"
    } 
    m := map[string]string{"req":req, "ip": lib.GetIp(), "port": this.port, "hostname": lib.GetHostname(), "role": this.role}
    msg, err := json.Marshal(m)
    if err != nil {
        loglib.Error("marshal " + req + " info error " + err.Error())
        return false
    }
    _, err = conn.Write(tcp_pack.Pack(msg))
    if err != nil {
        loglib.Error("send " + req + " info failed" + err.Error())
        return false
    }else{
        plen, ret := tcp_pack.UnPack(conn)

        if plen > 0 {
            err = json.Unmarshal(ret, &m)
            r, ok := m["err"]
            if err == nil && ok && r == "0" {
                loglib.Info(req + " to monitor success!")
                return true
            }
            loglib.Error(req + " heart beat failed!")
        }
    }
    return false
}
