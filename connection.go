package main

import (
	"net"
)

type Connection interface {
	reconnect(conn *net.TCPConn)
	getConn() *net.TCPConn
	close()
}