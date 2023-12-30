package paxos

import "net"

func Read(conn net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := conn.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func Write(conn net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := conn.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

type Client struct {
	index      uint8
	connection net.Conn
	//buffer     []byte
}
