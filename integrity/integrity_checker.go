package integrity

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"logd/lib"
	"logd/loglib"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//日志完整性检查类
type IntegrityChecker struct {
	dir          string
	statusFile   string
	hourReceived map[string]map[string]map[string]int // [ip][hour][id] = 1
	dayReceived  map[string]map[string]map[string]int // [ip][day][hour] = 1
}

func NewIntegrityChecker(dir string) *IntegrityChecker {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		loglib.Error("make integrity dir error:" + err.Error())
		return nil
	}
	statusFile := getFilePath()
	ic := &IntegrityChecker{}
	ic.dir = dir
	ic.statusFile = statusFile

	status := ic.LoadStatus(ic.statusFile)
	ic.hourReceived = status["hour_received"]
	ic.dayReceived = status["day_received"]
	return ic
}

func getFilePath() string {
	var d = lib.GetBinPath() + "/var"
	if !lib.FileExists(d) {
		os.MkdirAll(d, 0775)
	}
	return d + "/log_received.json"
}

func (this *IntegrityChecker) LoadStatus(filename string) map[string]map[string]map[string]map[string]int {
	m := make(map[string]map[string]map[string]map[string]int)
	m["hour_received"] = make(map[string]map[string]map[string]int)
	m["day_received"] = make(map[string]map[string]map[string]int)
	if lib.FileExists(filename) {
		vbytes, err := ioutil.ReadFile(filename)
		if err != nil {
			loglib.Error("read log received file error:" + err.Error())
		} else {
			err = json.Unmarshal(vbytes, &m)
			if err != nil {
				loglib.Error("unmarshal log received error:" + err.Error())
			} else {
				loglib.Info("load log received success !")
			}
		}
	} else {
		loglib.Warning("log received file " + filename + " not found!")
	}
	return m
}

func (this *IntegrityChecker) SaveStatus() {
	m := make(map[string]map[string]map[string]map[string]int)
	m["hour_received"] = this.hourReceived
	m["day_received"] = this.dayReceived
	vbytes, err := json.Marshal(m)
	if err != nil {
		loglib.Error("marshal log received error:" + err.Error())
		return
	}
	err = ioutil.WriteFile(this.statusFile, vbytes, 0664)
	if err == nil {
		loglib.Info("save log received success !")
	} else {
		loglib.Error("save log received error:" + err.Error())
	}
}

func (this *IntegrityChecker) Add(ip string, hour string, packId string, lines int, isDone bool) {
	_, ok := this.hourReceived[ip]
	if !ok {
		this.hourReceived[ip] = make(map[string]map[string]int)
	}
	_, ok = this.hourReceived[ip][hour]
	if !ok {
		this.hourReceived[ip][hour] = map[string]int{"total_lines": 0, "total_packs": 0}
	}
	this.hourReceived[ip][hour][packId] = 1
	this.hourReceived[ip][hour]["total_lines"] += lines
	if isDone {
		id, _ := strconv.Atoi(packId)
		this.hourReceived[ip][hour]["total_packs"] = id
		//this.Check()   //改为手动调用
	}
}

func (this *IntegrityChecker) addHour(ip string, hour string) bool {
	if len(hour) > 8 {
		day := hour[0:8]
		_, ok := this.dayReceived[ip]
		if !ok {
			this.dayReceived[ip] = make(map[string]map[string]int)
		}
		_, ok = this.dayReceived[ip][day]
		if !ok {
			this.dayReceived[ip][day] = make(map[string]int)
		}
		this.dayReceived[ip][day][hour] = 1
		return true
	}
	return false
}

//检查日志是否完整，返回当前这次检查已完成的小时和日期
func (this *IntegrityChecker) Check() (hourFinish map[string][]string, dayFinish map[string][]string) {
	hourFinish = make(map[string][]string)
	dayFinish = make(map[string][]string)
	interval := int64(86400 * 4) //4天前的不完整数据将被删除
	now := time.Now().Unix()
	//检查每小时是否完整
	for ip, m1 := range this.hourReceived {
		for hour, m2 := range m1 {
			totalPacks, ok := m2["total_packs"]
			if ok && totalPacks > 0 {
				miss := make([]string, 0)
				var id = ""
				//这小时已接收到最后一个包，可以check了
				for i := 1; i <= totalPacks; i++ {
					id = strconv.Itoa(i)
					_, ok = m2[id]
					if !ok {
						miss = append(miss, id)
					}
				}
				//if条件顺序不要错
				if len(miss) == 0 && this.makeHourTag(ip, hour, m2["total_lines"]) && this.addHour(ip, hour) {
					_, ok1 := hourFinish[ip]
					if !ok1 {
						hourFinish[ip] = make([]string, 0)
					}
					hourFinish[ip] = append(hourFinish[ip], hour)

					delete(this.hourReceived[ip], hour)
					if len(this.hourReceived[ip]) == 0 {
						delete(this.hourReceived, ip)
					}
				} else {
					loglib.Warning(fmt.Sprintf("%s_%s total %d, miss %s", ip, hour, totalPacks, strings.Join(miss, ",")))
				}
			}

			tm, err := time.Parse("2006010215", hour)
			if err != nil || (now-tm.Unix()) > interval {
				delete(this.hourReceived[ip], hour)
				loglib.Info(fmt.Sprintf("hour integrity: %s %s overtime", ip, hour))
			}
		}
	}

	//检查每天是否完整
	for ip, m1 := range this.dayReceived {
		for day, m2 := range m1 {
			if len(m2) == 24 && this.makeDayTag(ip, day) {
				loglib.Info(ip + "_" + day + " all received")

				_, ok1 := dayFinish[ip]
				if !ok1 {
					dayFinish[ip] = make([]string, 0)
				}
				dayFinish[ip] = append(dayFinish[ip], day)

				delete(this.dayReceived[ip], day)
				if len(this.dayReceived[ip]) == 0 {
					delete(this.dayReceived, ip)
				}
			}
			tm, err := time.Parse("20060102", day)
			if err != nil || (now-tm.Unix()) > interval {
				delete(this.dayReceived[ip], day)
				loglib.Info(fmt.Sprintf("day integrity: %s %s overtime", ip, day))
			}
		}
	}

	return
}

//touch一个文件表明某一小时接收完
func (this *IntegrityChecker) makeHourTag(ip string, hour string, lines int) bool {
	fname := fmt.Sprintf("%s_%s_%d", ip, hour, lines)
	filename := filepath.Join(this.dir, fname)
	fout, err := os.Create(filename)
	if err != nil {
		loglib.Error("tag " + fname + " error: " + err.Error())
		return false
	} else {
		fout.Close()
	}
	return true
}

//touch一个文件表明某一天接收完
func (this *IntegrityChecker) makeDayTag(ip string, day string) bool {
	fname := fmt.Sprintf("%s_%s", ip, day)
	filename := filepath.Join(this.dir, fname)
	fout, err := os.Create(filename)
	if err != nil {
		loglib.Error("tag " + fname + " error: " + err.Error())
		return false
	} else {
		fout.Close()
	}
	return true
}
