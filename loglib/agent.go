/**************
 *
 * 根据配置文件中的local_dir和tcp_addr分别生成logger，在本地和远程上报错误日志 
 *
 */

package loglib

import (
    "strings"
    // "os"
    "runtime"
    "fmt"
)

var logAgent *LogAgent
var HeartBeatPort = ""

func Init(config map[string]string) {
    logAgent = newLogAgent(config)
}

type LogAgent struct {
    logs []Log
    // message map[string]string
}

var levels = map[string]int{"debug":0, "info":1, "warning":2, "error":3}

func newLogAgent(config map[string]string) *LogAgent {
    var level = DEBUG
    levelStr, ok := config["local_level"]
    if ok && levelStr != "" {
        tmp, ok := levels[ strings.ToLower(levelStr) ] 
        if ok {
            level = tmp
        }
    }

    agent := new(LogAgent)

    dir, ok := config["local_dir"]
    if ok {
        fileLog := NewFileLog(dir, level)
        agent.logs = append(agent.logs, fileLog)
    }

    level = WARNING   //tcp 默认level
    levelStr, ok = config["tcp_level"]
    if ok && levelStr != "" {
        tmp, ok := levels[ strings.ToLower(levelStr) ] 
        if ok {
            level = tmp
        }
    }
    addr, ok := config["tcp_addr"]
    if ok {
        netLog := NewNetLog(addr, level)
        agent.logs = append(agent.logs, netLog)
    }
    return agent
}

func SetLevel(level int) {
    logAgent.setLevel(level)
}

func Debug(msg string) {
    logAgent.debug(makeDebugMessage(msg))
}

func makeDebugMessage(msg string) string {
    //test
    var m string
    var pc []uintptr = make([]uintptr, 100)
    i := runtime.Callers(0,pc)

    for j := 0; j < i; j++ {
        // _,_,line,_ := runtime.Caller(j)
        f := runtime.FuncForPC(pc[j])
        pName := getPackageName(f.Name() )
        if pName == "runtime" || pName == "logd/loglib" {
            continue
        }else {
            file,line := f.FileLine(pc[j])
            m = fmt.Sprintf("file:%s,func_name:%s,line:%d,msg:%s",file,f.Name(),line,msg)
            break
        }

    }
    return m
}

func Info(msg string) {
    logAgent.info(msg) 
}

func Warning(msg string) {
    logAgent.warning(msg)
}
func Error(msg string) {
    logAgent.error(msg)
    
}

func (a *LogAgent) setLevel(level int) {
    for _, l := range a.logs {
        l.SetLevel(level)
    }
}

func (a *LogAgent) debug(msg string) {
    for _, l := range a.logs {
        l.Debug(msg)
    }
}

func (a *LogAgent) info(msg string) {
    for _, l := range a.logs {
        l.Info(msg)
    }
    
}

func (a *LogAgent) warning(msg string) {
    for _, l := range a.logs {
        l.Warning(msg)
    }
    
}
func (a *LogAgent) error(msg string) {
    for _, l := range a.logs {
        l.Error(msg)
    }
    
}

func getPackageName(function string) string {
    a := strings.Split(function,".")

    if len(a) != 2 {
        return function
    }else {
        return a[0]
    }


}


