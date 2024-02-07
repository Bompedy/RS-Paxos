package paxos

import (
	"net"
	"sync"
)

func (client Client) Read(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := client.connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (client Client) Write(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := client.connection.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

type Client struct {
	connection net.Conn
	mutex      *sync.Mutex
	//buffer     []byte
}
