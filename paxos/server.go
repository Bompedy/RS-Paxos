package paxos

import (
	"encoding/binary"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"math"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

var OpWrite = uint8(0)
var OpCommit = uint8(1)

type Node struct {
	Clients []Client
	Storage Storage
	Lock    sync.Mutex
	Total   int
	Encoder reedsolomon.Encoder
}

func (node *Node) Connect(nodes []string) error {
	defer node.Lock.Unlock()
	node.Lock.Lock()

	var waiter sync.WaitGroup
	for _, address := range nodes {
		waiter.Add(1)
		address := address
		go func() {
			defer waiter.Done()
			var client net.Conn
			var err error
			for err != nil {
				client, err = net.Dial("tcp", address)
			}
			index, err := strconv.Atoi(string(address[len(address)-3]))
			if err != nil {
				panic(fmt.Sprintf("Can't parse address to index: %s", address))
			}
			node.Clients = append(node.Clients, Client{
				connection: client,
				index:      uint8(index),
				//buffer: make([]byte, 65535),
			})
		}()
	}

	waiter.Wait()
	sort.Slice(node.Clients, func(i, j int) bool {
		return node.Clients[i].index < node.Clients[j].index
	})
	return nil
}

func (node *Node) Accept(
	address string,
	block func(key []byte, value []byte),
) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	for {
		client, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		connection := Client{
			connection: client,
			index:      0,
		}

		buffer := make([]byte, 65535)
		go func() {
			err := connection.Read(buffer[:1])
			if err != nil {
				panic(err)
			}

			op := buffer[0]
			if op == OpWrite {
				err := connection.Read(buffer[:8])
				if err != nil {
					panic(err)
				}
				keySize := binary.LittleEndian.Uint32(buffer[:4])
				valueSize := binary.LittleEndian.Uint32(buffer[4:8])
				err = connection.Read(buffer[:(keySize + valueSize)])
				if err != nil {
					panic(err)
				}

				block(buffer[:keySize], buffer[keySize:(keySize+valueSize)])
				err = connection.Write(buffer[:1])
				if err != nil {
					panic(err)
				}
			} else if op == OpCommit {
				err = connection.Write(buffer[:1])
				if err != nil {
					panic(err)
				}
			}
		}()
	}
}

func (node *Node) Write(key []byte, value []byte) error {
	const numSegments = 3
	const parity = 2
	var segmentSize = int(math.Ceil(float64(len(value)) / float64(numSegments)))
	var segments = reedsolomon.AllocAligned(numSegments+parity, segmentSize)
	var startIndex = 0
	for i := range segments[:numSegments] {
		endIndex := startIndex + segmentSize
		if endIndex > len(value) {
			endIndex = len(value)
		}
		copy(segments[i], value[startIndex:endIndex])
		startIndex = endIndex
	}

	err := node.Encoder.Encode(segments)
	if err != nil {
		panic(err)
	}

	ok, err := node.Encoder.Verify(segments)
	if err != nil || !ok {
		panic(err)
	}

	println("RS_PAXOS: FINISHED ENCODING")
	return node.quorum(func(index int, client Client) error {
		// Add 1 since DS1 is the leaders segment
		shard := segments[index+1]
		buffer := make([]byte, 9+len(key)+len(shard))
		buffer[0] = OpWrite
		binary.LittleEndian.PutUint32(buffer[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(shard)))
		keyIndex := 9 + len(key)
		copy(buffer[9:keyIndex], key)
		copy(buffer[keyIndex:keyIndex+len(shard)], shard)
		fmt.Printf("RS_PAXOS: FINISHED BUFFERING FOR: %d\n", index)
		err := client.Write(buffer)
		fmt.Printf("RS_PAXOS: FINISHED WRITING FOR: %d\n", index)
		if err != nil {
			panic(err)
		}
		return client.Read(buffer[:1])
	})
}

func (node *Node) quorum(
	block func(index int, client Client) error,
) error {
	var waiter sync.WaitGroup
	waiter.Add(node.Total - 1)
	var count = uint32(0)

	for i := range node.Clients {
		fmt.Printf("RS_PAXOS: QUOROM GRABBING CLIENT: %d\n", i)
		client := node.Clients[i]
		fmt.Printf("RS_PAXOS: FOUND CLIENT CLIENT: %d\n", i)
		go func(index int, client Client) {
			err := block(index, client)
			if err != nil {
				panic(err)
			}
			if atomic.AddUint32(&count, 1) <= uint32(node.Total-1) {
				waiter.Done()
			}
		}(i, client)
	}

	waiter.Wait()
	return nil
}
