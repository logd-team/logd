package tcp_pack

import (
    "encoding/binary"
    "encoding/json"
    "net"
    "bytes"
    "log"
    "errors"
)

//将字节数组加上4字节的长度头
func Pack(data []byte) []byte {
    lenBuf := make([]byte, 4)
    nData := len(data)
    binary.PutUvarint(lenBuf, uint64(nData))
    data = append(lenBuf, data...)
    return data
}

//解包字节数组，返回内容和长度
func UnPack(conn net.Conn) (int, []byte) {
    packLen := -1   //-1表示有错
    b := new(bytes.Buffer)

    if conn != nil {
        // read pack length        
        buf := make([]byte, 4)
        conn.Read(buf)
        l, _ := binary.Uvarint(buf)
        packLen = int(l)
        var currLen = 0
        //当包较小时，就按packlen读，不然会读多，若较大，则按一个固定值分多次读
        //一次读取不会超过10m，但是大小不定
        bufSize := packLen
        if bufSize > 10485760 {
            bufSize = 10485760
        }
        request := make([]byte, bufSize)    
        //read enough bytes
        for currLen < packLen {
            requestLen, _ := conn.Read(request)
            if requestLen == 0 {
                break 
            }
            currLen += requestLen
            b.Write(request[:requestLen])
        }
    }
    return packLen, b.Bytes()
}

type PackHeader struct {
    Route []map[string]string
    PackLen int
}

func Packing(data []byte, info map[string]string, hasHeader bool) []byte {
    content := make([]byte, 0)
    var header PackHeader
    if hasHeader {
        var l int
        var err error
        header, l, err = ExtractHeader(data)
        if err != nil {
            return data
        }
        header.Route = append(header.Route, info)
        content = data[4+l : ]
    }else{
        header.Route = append(header.Route, info)
        content = data
    }
    header.PackLen = len(content)
    headerStr, err := json.Marshal(header)
    if err != nil {
        log.Println("marshal header: ", header , "error", err)
    }
    buf := make([]byte, 4)
    binary.PutUvarint(buf, uint64(len(headerStr)))
    buf = append(buf, []byte(headerStr)...)
    buf = append(buf, content...)
    return buf
}

func ExtractHeader(data []byte) (PackHeader, int, error) {
    var header PackHeader
    var err = errors.New("pack header doesn't have enough bytes")
    var l = uint64(0)
    if len(data) > 4 {
        buf := data[0:4]
        l, _ = binary.Uvarint(buf)
        //header
        buf = data[4: 4+l]
        err = json.Unmarshal(buf, &header)
        if err != nil {
            log.Println("wrong format pack header")
        }
    }
    return header, int(l), err
    
}

func GetPackId(data []byte) string {
    header, _, err := ExtractHeader(data)
    packId := "unkown"
    if err == nil && len(header.Route) > 0 {
        route := header.Route[0]
        hour, _ := route["hour"]
        done, ok := route["done"]
        if ok {
            done = "_done"
        }
        packId = route["ip"] + "_" + hour + "_" + route["id"] + done
    }
    return packId
}

func ParseHeader(vbytes []byte) map[string]string {
    m := map[string]string{"ip":"", "hour":"", "done":"", "lines":"0"}
    var header PackHeader
    err := json.Unmarshal(vbytes, &header)
    if err != nil {
        log.Println("wrong format pack header")
    }else{
        if len(header.Route) > 0 {
            m = header.Route[0]
        }
    }
    return m
}
