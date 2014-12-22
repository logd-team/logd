package lib

import (
    "net"
    "log"
    "runtime"
    "strings"
    "bytes"
    "encoding/binary"
)

func GetConnection(addr string) (*net.TCPConn, error) {
    tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
    if err != nil {
        _, f, l, _ := runtime.Caller(1)
        f = strings.Replace(f, GetBinPath(), "", -1)
        log.Println(f, ":", l, "[GetConnection] resolve tcp address failed", err)
        return nil, err
    }
    conn, err := net.DialTCP("tcp4", nil, tcpAddr)
    if err != nil {
        _, f, l, _ := runtime.Caller(1)
        f = strings.Replace(f, GetBinPath(), "", -1)
        log.Println(f, ":", l, "[GetConnection] connect to address:" , addr, "failed!")
    }
    return conn, err
}

func IpToUint32(ip string) uint32 {
    ipo := net.ParseIP(ip)
    r := bytes.NewReader([]byte(ipo.To4()))
    var ipl uint32 = 0
    err := binary.Read(r, binary.BigEndian, &ipl)
    if err != nil {
        log.Println("ip to long error:", err)
    }
    return ipl
}
