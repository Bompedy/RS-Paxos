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
	Total   int
	Encoder reedsolomon.Encoder
	Log     Log
}

type Log struct {
	Lock    *sync.Mutex
	Entries map[uint32]*Entry
}

type Entry struct {
	key       []byte
	value     []byte
	acked     uint32
	majority  uint32
	condition chan struct{}
}

func (node *Node) Connect(
	nodes []string,
	block func(key []byte, value []byte),
) error {
	var waiter sync.WaitGroup
	for _, address := range nodes {
		waiter.Add(1)
		address := address
		go func() {
			defer waiter.Done()
			var connection net.Conn
			var err error
			for {
				connection, err = net.Dial("tcp", address)
				if err != nil {
					continue
				}
				break
			}
			index, err := strconv.Atoi(string(address[len(address)-3]))
			if err != nil {
				panic(fmt.Sprintf("Can't parse address to index: %s", address))
			}
			client := Client{
				connection: connection,
				index:      uint8(index),
				mutex:      &sync.Mutex{},
			}
			node.Clients = append(node.Clients, client)
			buffer := make([]byte, 4)

			// this is the leader
			go func() {
				for {
					err = client.Read(buffer)
					if err != nil {
						panic(err)
					}
					commitIndex := binary.LittleEndian.Uint32(buffer)
					node.Log.Lock.Lock()
					entry, exists := node.Log.Entries[commitIndex]
					node.Log.Lock.Unlock()
					acked := atomic.AddUint32(&entry.acked, 1)
					//fmt.Printf("Leader got response: %d, %d, %d, %v\n", commitIndex, acked, entry.majority, exists)
					if exists && acked == entry.majority {
						go func() {
							block(entry.key, entry.value)
							close(entry.condition)
							node.Log.Lock.Lock()
							delete(node.Log.Entries, commitIndex)
							node.Log.Lock.Unlock()
						}()
					}
				}
			}()
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
	for {
		// loop here cause port might be stuck open
		listener, err := net.Listen("tcp", address)
		if err != nil {
			continue
		}

		for {
			connection, err := listener.Accept()
			if err != nil {
				panic(err)
			}

			client := Client{
				connection: connection,
				index:      0,
			}

			go func() {
				buffer := make([]byte, 65535)
				for {
					err := client.Read(buffer[:1])
					if err != nil {
						panic(err)
					}

					op := buffer[0]
					if op == OpWrite {
						err := client.Read(buffer[:12])
						if err != nil {
							panic(err)
						}
						commitIndex := binary.LittleEndian.Uint32(buffer[:4])
						keySize := binary.LittleEndian.Uint32(buffer[4:8])
						valueSize := binary.LittleEndian.Uint32(buffer[8:12])
						required := int(keySize + valueSize)

						if len(buffer) < required {
							buffer = append(buffer, make([]byte, required-len(buffer))...)
						}

						err = client.Read(buffer[:(keySize + valueSize)])
						if err != nil {
							panic(err)
						}

						block(buffer[:keySize], buffer[keySize:(keySize+valueSize)])
						binary.LittleEndian.PutUint32(buffer[:4], commitIndex)
						err = client.Write(buffer[:4])
						if err != nil {
							panic(err)
						}
					}
					//else if op == OpCommit {
					//	err = client.Write(buffer[:1])
					//	if err != nil {
					//		panic(err)
					//	}
					//}
				}
			}()
		}
	}
}

var CommitIndex uint32

func (node *Node) Write(key []byte, value []byte) {
	//1gb, .33mb, .33mb, .33mb, x amount of size, x amount size

	//fmt.Printf("Value size: %s", string(value))
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
	commitIndex := atomic.AddUint32(&CommitIndex, 1)
	entry := &Entry{
		key:       key,
		value:     value,
		acked:     0,
		majority:  uint32(node.Total - 1),
		condition: make(chan struct{}),
	}
	node.Log.Lock.Lock()
	node.Log.Entries[commitIndex] = entry
	node.Log.Lock.Unlock()

	for i := range node.Clients {
		go func(index int, client Client) {
			shard := segments[index+1]
			buffer := make([]byte, 13+len(key)+len(shard))
			buffer[0] = OpWrite
			binary.LittleEndian.PutUint32(buffer[1:5], commitIndex)
			binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
			binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(shard)))
			keyIndex := 13 + len(key) //fix
			copy(buffer[13:keyIndex], key)
			copy(buffer[keyIndex:keyIndex+len(shard)], shard)
			client.mutex.Lock()
			err := client.Write(buffer)
			client.mutex.Unlock()
			if err != nil {
				panic(err)
			}
		}(i, node.Clients[i])
	}
	<-entry.condition
}
