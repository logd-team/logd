package main

import (
	"logd/lib"
    "logd/loglib"
	"compress/zlib"
	"bytes"
	"os"
    "fmt"
    "strconv"
    "strings"
    "encoding/binary"
    "sync"
    "io"
    "io/ioutil"
    "bufio"
    "encoding/json"
    "logd/tcp_pack"
    "time"
    "errors"
    "net/url"
    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
)

type MongoDbOutputer struct {
    
	buffer     chan bytes.Buffer
    mongosAddr string
    db         string
    collection string
    isUpsert     bool
    bulkSize   int
    savers     int
	file_mem_folder_name string
    transactionIdKey string
    fileList *lib.GlobalList
    wq *lib.WaitQuit
}

func MongoDbOutputerInit(buffer chan bytes.Buffer, config map[string]string) (mo MongoDbOutputer) {
    mo.buffer = buffer
    mo.wq = lib.NewWaitQuit("mongodb outputer", -1)
    mo.mongosAddr, _ = config["mongos"]
    mo.db         , _ = config["db"]
    mo.collection , _ = config["collection"]

    upsert, _ := config["upsert"]
    if upsert == "true" {
        mo.isUpsert = true
    }else{
        mo.isUpsert = false
    }

    bulkSize, _ := config["bulk_size"]
    nBulk, err := strconv.Atoi(bulkSize)
    if err == nil {
        mo.bulkSize = nBulk
    }else{
        mo.bulkSize = 50
    }

    savers, _ := config["savers"]
    nSavers, err := strconv.Atoi(savers)
    if err == nil {
        mo.savers = nSavers
    }else{
        mo.savers = 20
    }
    
    //创建文件缓存目录
    mo.file_mem_folder_name = "tempfile"
    if !lib.FileExists(mo.file_mem_folder_name) {
        os.MkdirAll(mo.file_mem_folder_name, 0775)
    }
    
    mo.transactionIdKey = "transaction_id"
    mo.fileList = lib.GlobalListInit()
    return mo
}

func initMongoDbSession(mongosAddr string) *mgo.Session {
    session, err := mgo.Dial(mongosAddr)
    if err != nil {
        loglib.Error(fmt.Sprintf("init mongodb session error:%v", err))
        return nil
    }   

    session.SetMode(mgo.Monotonic, true)    //设置read preference
    session.SetSafe(&mgo.Safe{W:2})         //设置write concern
    return session
}

func (this *MongoDbOutputer) Start() {
    wg := &sync.WaitGroup{}
    defer func(){
        if err := recover(); err != nil {
            loglib.Error(fmt.Sprintf("mongodb outputer panic:%v", err))
        }
        this.wq.AllDone()
    }()

    this.reloadFileCache()
    
    wg.Add(this.savers)

    for i:=0; i<this.savers; i++ {
        go this.runParse(i, wg)
    }

    nRetry := this.savers / 6 + 1
    wg.Add(nRetry)
    for i:=0; i<nRetry; i++ {
        go this.retrySave(wg)
    }

    wg.Wait()


}

func (this *MongoDbOutputer) Quit() bool {
    return this.wq.Quit()
}

func (this *MongoDbOutputer) runParse( routineId int, wg *sync.WaitGroup) {
    session := initMongoDbSession(this.mongosAddr)
    var coll *mgo.Collection

    defer func(){
        if session != nil {
            session.Close()
        }
        wg.Done()
        loglib.Info(fmt.Sprintf("mongodb outputer parse routine %d quit", routineId))
    }()

    dateStr := ""

    loglib.Info(fmt.Sprintf("mongodb outputer parse routine %d start", routineId))
    for b := range this.buffer {
        r, packId, date, err := this.extract(&b)
        if err == nil {
            // 重连
            if session == nil {
                session = initMongoDbSession(this.mongosAddr)
                if session == nil {
                    coll = nil          //触发bulkSaveBson和upsertBson报错
                    loglib.Warning(fmt.Sprintf("parse routine %s reconnect to %s failed!", routineId, this.mongosAddr))
                }else{
                    loglib.Warning(fmt.Sprintf("parse routine %s reconnect to %s succeed!", routineId, this.mongosAddr))
                }
            }
            if coll == nil || date != dateStr {
                if session != nil {
                    coll = session.DB(this.db + date).C(this.collection)   //按天分库
                }
                dateStr = date
            }
            if !this.isUpsert {
                this.bulkSave(coll, r, packId, date, routineId)
            }else{
                this.upsert(coll, r, packId, date, routineId)
            }
            r.Close()
        }
    }
}
//重新保存先前失败的文档
func (this *MongoDbOutputer) retrySave(wg *sync.WaitGroup) {
    session := initMongoDbSession(this.mongosAddr)
    var coll *mgo.Collection

    defer func(){
        if session != nil {
            session.Close()
        }
        wg.Done()
        loglib.Info("mongodb outputer retry routine quit.")
    }()

    var quit = false
    go lib.HandleQuitSignal(func(){
        quit = true
    })

    dateStr := ""
    loglib.Info("mongodb outputer retry routine start")
    for !quit {
        e := this.fileList.Remove()
        if e != nil {
            filename := e.Value.(string)
            b, err := ioutil.ReadFile(filename)
            if err != nil {
                if _, ok := err.(*os.PathError); !ok {
                    this.fileList.PushBack(filename)     //非路径错误，下次再试
                }
                loglib.Error(fmt.Sprintf("load cache %s error:%v", filename, err))
            }else{
                m := bson.M{}
                err = json.Unmarshal(b, &m)
                if err != nil {
                    loglib.Error(fmt.Sprintf("unmarshar %s error:%v", filename, err))
                }else{
                    // 重连
                    if session == nil {
                        session = initMongoDbSession(this.mongosAddr)
                        if session == nil {
                            coll = nil          //触发bulkSaveBson和upsertBson报错
                            loglib.Warning(fmt.Sprintf("retry routine reconnect to %s failed!", this.mongosAddr))
                        }else{
                            loglib.Warning(fmt.Sprintf("retry routine reconnect to %s succeed!", this.mongosAddr))
                        }
                    }
                    tp, _ := m["type"].(string)
                    date, _ := m["date"].(string)
                    if coll == nil || date != dateStr {
                        if session != nil {
                            coll = session.DB(this.db + date).C(this.collection)   //按天分库
                        }
                        dateStr = date
                    }
                    if tp == "bulk" {
                        data, _ := m["data"].([]interface{})
                        err = this.bulkSaveBson(coll, data...)
                    }else{
                        data, _ := m["data"].(map[string]interface{})
                        sel := bson.M{this.transactionIdKey : data[this.transactionIdKey]}
                        up := bson.M{"$set" : data}
                        _, err = this.upsertBson(coll, sel, up)
                    }
                    if err != nil {
                        this.fileList.PushBack(filename)
                        loglib.Error(fmt.Sprintf("re-save cache %s error:%v", filename, err))
                    }else{
                        err = os.Remove(filename)
                        if err != nil {
                            loglib.Error(fmt.Sprintf("remove file: %s error:%v", filename, err));
                        }else{
                            loglib.Info(fmt.Sprintf("cache file: %s send out", filename));
                        }

                    }
                }
            }
        }
        time.Sleep(500 * time.Millisecond)
    }
}

func (this *MongoDbOutputer) extract(bp *bytes.Buffer) (r io.ReadCloser, packId string, date string, err error) {
    buf := make([]byte, 4) 
    bp.Read(buf)

    l, _ := binary.Uvarint(buf)
    headerLen := int(l)
    //get pack header
    buf = make([]byte, headerLen)  
    bp.Read(buf)
    header := tcp_pack.ParseHeader(buf)

    r, err = zlib.NewReader(bp)
    if err != nil {
        loglib.Error("zlib reader Error: " + err.Error())
    }
    date = header["hour"][0:8]   //用于按天分库
    packId = fmt.Sprintf("%s_%s_%s", header["ip"], header["hour"], header["id"])
    return 
}
//批量插入
func (this *MongoDbOutputer) bulkSave(coll *mgo.Collection, r io.Reader, packId string, date string, routineId int) {
    scanner := bufio.NewScanner(r)

    arr := make([]interface{}, 0)
    cnt := 0
    nDiscard := 0
    nInserted := 0
    nCached := 0
    for scanner.Scan() {
        line := scanner.Text()
        m := this.parseLogLine(line)
        if len(m) > 0 {
            arr = append(arr, m)
            cnt++
            if cnt >= this.bulkSize {
                err := this.bulkSaveBson(coll, arr...)
                if err != nil {
                    this.cacheData(arr, "bulk", date, routineId)
                    nCached += cnt
                }else{
                    nInserted += cnt
                }
                arr = make([]interface{}, 0)
                cnt = 0
            }
        }else{
            nDiscard++
        }

    }
    cnt = len(arr)
    if cnt > 0 {
        err := this.bulkSaveBson(coll, arr...)
        if err != nil {
            this.cacheData(arr, "bulk", date, routineId)
            nCached += cnt
        }else{
            nInserted += cnt
        }
    }

    loglib.Info(fmt.Sprintf("save pack %s: inserted:%d, cached:%d, discard %d items", packId, nInserted, nCached, nDiscard))
}
func (this *MongoDbOutputer) bulkSaveBson(coll *mgo.Collection, docs ...interface{}) (err error) {
    if coll != nil {
        err = coll.Insert(docs...)
        if err != nil {
            tmp := make([]string, 0)
            for _, doc := range docs {
                m, _ := doc.(bson.M)
                tid, _ := m[this.transactionIdKey].(string)
                tmp = append(tmp, tid)
            }
            tids := strings.Join(tmp, ",")
            loglib.Error(fmt.Sprintf("save %d bsons [%s] error:%v", len(docs), tids, err))
        }
    }else{
        err = errors.New("bulk: collection is nil")
        loglib.Error(fmt.Sprintf("save bsons error:%v", err))

    }
    return
}
//更新插入，按字段更新
func (this *MongoDbOutputer) upsert(coll *mgo.Collection, r io.Reader, packId string, date string, routineId int) {
    nDiscard := 0
    nCached := 0
    nUpdated := 0
    nInserted := 0

    scanner := bufio.NewScanner(r)
    for scanner.Scan() {
        line := scanner.Text()
        m := this.parseLogLine(line)
        if len(m) > 0 {
            selector := bson.M{ this.transactionIdKey: m[ this.transactionIdKey ] }
            up := bson.M{"$set" : m}
            info, err := this.upsertBson(coll, selector, up)
            if err != nil {
                this.cacheData(m, "upsert", date, routineId)
                nCached++
            }else{
                nInserted++
                nUpdated += info.Updated
            }
        }else{
            nDiscard++
        }
    }
    
    loglib.Info(fmt.Sprintf("save pack %s: inserted:%d, updated:%d, cached:%d, discard %d items", packId, nInserted, nUpdated, nCached, nDiscard))
}
func (this *MongoDbOutputer) upsertBson(coll *mgo.Collection, selector interface{}, doc interface{}) (info *mgo.ChangeInfo, err error) {
    m, _ := selector.(bson.M)
    tid, _ := m[this.transactionIdKey].(string)

    if coll != nil {
        info, err = coll.Upsert(selector, doc)

        if err != nil {
            loglib.Error(fmt.Sprintf("save bson [%s] error:%v", tid, err))
        }else{
            if info.Updated > 0 {
                loglib.Info(fmt.Sprintf("bson [%s] updated", tid))
            }
        }
    }else{
        info = &mgo.ChangeInfo{}
        err = errors.New("upsert: collection is nil")
        loglib.Error(fmt.Sprintf("save bson [%s] error:%v", tid, err))
    }
    return
}
//缓存写入mongodb失败的数据
//typeStr为bulk或upsert
func (this *MongoDbOutputer) cacheData(data interface{}, typeStr string, date string, routineId int) {
    mp := bson.M{"type": typeStr, "date": date, "data": data}
    saveTry := 3
    b, err := json.Marshal(mp)
    arr, ok := data.([]bson.M)
    cnt := 1
    if ok {
        cnt = len(arr)
    }
    if err != nil {
        loglib.Error(fmt.Sprintf("cache data error when marshal, discard %d item(s), error:%v", cnt, err))
        return
    }
    fname := this.createFileName(routineId)
    for i:=0; i<saveTry; i++ {
        err = ioutil.WriteFile(fname, b, 0666)
        if err == nil {
            this.fileList.PushBack(fname)
            loglib.Info(fmt.Sprintf("cache %d bson", cnt))
            break
        }
    }
}
//载入cache文件列表
func (this *MongoDbOutputer) reloadFileCache() {
	list := lib.GetFilelist(this.file_mem_folder_name)
	for _,filename := range list {
		loglib.Info("reloading:" + filename)
		this.fileList.PushBack(filename)
	}
}

func (this *MongoDbOutputer) parseLogLine(line string) (m bson.M) {
    slen := len(line)
    //截取ip
    p1 := strings.Index(line, " ")
    p2 := slen
    if p1 > 0 && p1 < slen-1 {
        p := strings.Index(line[p1+1:], " ")
        if p > 0 {
            p2 = p + p1 + 1  //注意！p只是slice中的index，不是line中的
        }
    }else{
        p1 = 0
    }
    ipInLong := lib.IpToUint32(line[p1+1 : p2])
    //截取时间
    p1 = strings.Index(line, "[")
    p2 = strings.Index(line, "]")
    hourStr := line[p1+1 : p2]
    var timestamp int64 = 0
    var day int = 0
    var hour int = -1
    tm, err := time.ParseInLocation("02/Jan/2006:15:04:05 -0700", hourStr, time.Local)
    if err != nil {
        loglib.Warning("parse time error" + err.Error())
    }else{
        timestamp = tm.Unix()
        dayStr := tm.Format("20060102")
        day, err = strconv.Atoi(dayStr)
        if err != nil {
            loglib.Error(fmt.Sprintf("conv %s to int error: %v", dayStr, err))
        }
        hour = tm.Hour()
    }
    //截取请求url
    urlStr := ""
    p3 := strings.Index(line, "\"")
    p4 := strings.Index(line[p3+1: ], "\"") + p3 + 1
    reqStr := line[p3+1 : p4]
    parts := strings.Split(reqStr, " ")

    m = make(bson.M)
    if len(parts) == 3 {
        urlStr = parts[1]
        u, err := url.Parse(urlStr)
        if err == nil {
            q := u.Query()
            tid := q.Get( this.transactionIdKey )  //检验有无transaction id
            if tid != "" {
                //参数对放入bson
                for k, _ := range q {
                    m[k] = q.Get(k)
                }
                m["ipinlong"] = ipInLong
                m["time"] = timestamp
                m["day"] = day
                m["hour"] = hour
            }
        }
    }
    return
}

func (this *MongoDbOutputer) createFileName(id int) string {
	t := time.Now()
	filename := fmt.Sprintf("%s/writeFailedTempFile_%d_%d", this.file_mem_folder_name, id, t.UnixNano())
	return filename
}

