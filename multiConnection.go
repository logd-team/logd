package main

// import (
// 	"math/rand"
// 	"time"
// 	"strings"
// )
// type SingleConnection struct {
// 	addrMap map[string] int
// 	bakAddrMap map[string] int
// 	connMap map[*net.TCPConn] string

// 	max_try_times int
// }

// func SingleConnectionInit(addressList,bakAddressList) (lc LConnection){

// 	lc.addrMap = make(map[string] int)
// 	lc.bakAddrMap = make(map[string] int)
// 	temp1 := strings.Split(addressList,",")
// 	for _,addr := range temp1 {
// 		lc.addrMap[addr] = 0
// 	}

// 	temp2 := strings.Split(bakAddressList,",")
// 	for _,addr := range temp2 {
// 		lc.bakAddrMap[addr] = 0
// 	}

// 	lc.connMap = make(map[*net.TCPConn] string)

// 	lc.max_try_times = 100
// 	return lc
// }

// //init conn list from addrMap
// func (lc *LConnection) initConnList() {
// 	 for addr,_ := range lc.addrMap {
// 	 	conn,err := createSingleConnection(addr)
//  		if err != nil {
//  			lc.connMap[conn] = addr
//  			addrMap[addr] = 1
//  		}
// 	 }
// }

// func createSingleConnection(address string) (conn *net.TCPConn,err error) {

// 	tcpAddr, err := net.ResolveTCPAddr("tcp4", address)
// 	lib.CheckError(err)
// 	if err != nil {
// 		return nil,err
// 	}
// 	conn, err = net.DialTCP("tcp", nil, tcpAddr)
// 	if err != nil {
// 		fmt.Println("get connection from ",address," failed!!!!!!")
// 		return nil,err
// 	}else {
// 		fmt.Println("get connection from ",address," success!!!!!!")
// 	}
// 	lib.CheckError(err)

// 	return conn
// }

// func (lc *LConnection) reconnect(conn *net.TCPConn) (*net.TCPConn,error) {
// 	delete(lc.connMap,conn)

// 	addr := lc.connMap[conn]

// 	newConn,err := createSingleConnection(addr)
// 	if err != nil {
// 		lc.connMap[newConn] = addr
// 		return newConn,nil
// 	}else {
// 		return nil,errors.New("reconnect to ",addr," failed")
// 	}

// }

// func (lc *LConnection) reInitConn() {

// }

// func (lc *LConnection) getConn() (conn net.TCPConn,err Error) {
// 	if len(connList) == 1 {	//只有一个collector的情况
// 		return connList[0],nil
// 	}else if len(connList)==0 { //当前没有可用的collector连接，重试机制
// 		lc.reInitConn()
// 		if len(connList)==0 {
// 			return nil,errors.New("error:length of conn list is 0") //重试失败，返回错误
// 		}else {
// 			return connList[0],nil
// 		}
// 	}else {
// 		rand.Seed(time.Now().UnixNano())
// 		i := rand.Intn(len(a))
// 		return connList[i],nil
// 	}

// }
