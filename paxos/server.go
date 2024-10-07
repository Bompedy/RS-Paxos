package paxos

import (
	"encoding/binary"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"math"
	"net"
	"sync"
	"sync/atomic"
)

var OpWrite = uint8(0)
var OpCommit = uint8(1)
var OpForward = uint8(2)
var OpAck = uint8(3)

var CommitIndex uint32

type Node struct {
	Clients  []Client
	Total    int
	Encoder  reedsolomon.Encoder
	Log      Log
	Quorum   int
	Parity   int
	Segments int
	Index    int
	Leader   int
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
	local string,
	nodes []string,
) error {
	var waiter sync.WaitGroup
	for _, address := range nodes {
		if address == local {
			continue
		}
		waiter.Add(1)
		address := fmt.Sprintf("%s:2000", address)
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
			client := Client{
				connection: connection,
				mutex:      &sync.Mutex{},
			}
			node.Clients = append(node.Clients, client)
		}()
	}

	waiter.Wait()
	return nil
}

type Task struct {
	Key       []byte
	Value     []byte
	Condition chan struct{}
}

func (node *Node) Accept(
	address string,
	etcdWrite func(key []byte, value []byte),
) error {

	for {
		// loop here cause port might be stuck open
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:2000", address))
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
			}

			go func() {
				buffer := make([]byte, 65535)
				mutex := sync.Mutex{}
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

						key := make([]byte, keySize)
						value := make([]byte, valueSize)
						copy(key, buffer[:keySize])
						copy(value, buffer[keySize:(keySize+valueSize)])

						go func() {
							etcdWrite(key, value)
							response := make([]byte, 5)
							buffer[0] = OpAck
							binary.LittleEndian.PutUint32(response, commitIndex)
							mutex.Lock()
							err = client.Write(response)
							mutex.Unlock()
							if err != nil {
								panic(err)
							}
						}()
					} else if op == OpForward {
						err := client.Read(buffer[:8])
						if err != nil {
							panic(err)
						}
						keySize := binary.LittleEndian.Uint32(buffer[:4])
						valueSize := binary.LittleEndian.Uint32(buffer[4:8])
						required := int(keySize + valueSize)
						if len(buffer) < required {
							buffer = append(buffer, make([]byte, required-len(buffer))...)
						}
						err = client.Read(buffer[:(keySize + valueSize)])
						key := make([]byte, keySize)
						value := make([]byte, valueSize)
						copy(key, buffer[:keySize])
						copy(value, buffer[keySize:(keySize+valueSize)])
						go func() {
							node.Write(key, value, etcdWrite)
						}()
					} else if op == OpAck {
						// 1
						// 1 -> write to other nodes, collect acks, respond back to client
						// 1 -> read full value -> respond back to client

						// 1, 2, 3, 4, 5
						// 2 -> 1 -> write to other nodes -> collect acks -> commit -> 2 gets commit -> responds back to client
						// 2 -> 1 -> read full value -> send back to 2 -> repsond back to client

						err = client.Read(buffer)
						if err != nil {
							panic(err)
						}
						commitIndex := binary.LittleEndian.Uint32(buffer)
						go func() {
							node.Log.Lock.Lock()
							entry, exists := node.Log.Entries[commitIndex]
							node.Log.Lock.Unlock()

							if exists && atomic.AddUint32(&entry.acked, 1) == entry.majority {
								close(entry.condition)
								node.Log.Lock.Lock()
								delete(node.Log.Entries, commitIndex)
								node.Log.Lock.Unlock()

								for i := range node.Clients {
									go func(index int, client Client) {

									}(i, node.Clients[i])
								}
							}
						}()
					} else if op == OpCommit {
						err = client.Read(buffer)
						if err != nil {
							panic(err)
						}
						//commitIndex := binary.LittleEndian.Uint32(buffer)
						go func() {

						}()
					}
				}
			}()
		}
	}
}

func (node *Node) Forward(
	key []byte,
	value []byte,
	etcdWrite func(key []byte, value []byte),
) {
	// forward request to leader
	if node.Index != node.Leader {
		buffer := make([]byte, 9+len(key)+len(value))
		buffer[0] = OpForward
		binary.LittleEndian.PutUint32(buffer[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(value)))
		var keyIndex = 9 + len(key)
		copy(buffer[9:keyIndex], key)
		copy(buffer[keyIndex:keyIndex+len(value)], value)
		//var leader = node.Clients[node.Leader]
		//leader.mutex.Lock()
		//err := leader.Write(buffer)
		//leader.mutex.Unlock()
		// close condition here
	} else {
		node.Write(key, value, etcdWrite)
	}
}

func (node *Node) Write(
	key []byte,
	value []byte,
	etcdWrite func(key []byte, value []byte),
) {
	var segmentSize = int(math.Ceil(float64(len(value)) / float64(node.Segments)))
	var segments = reedsolomon.AllocAligned(node.Segments+node.Parity, segmentSize)
	var startIndex = 0
	for i := range segments[:node.Segments] {
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
		acked:     1,
		majority:  uint32(node.Quorum),
		condition: make(chan struct{}),
	}
	node.Log.Lock.Lock()
	node.Log.Entries[commitIndex] = entry
	node.Log.Lock.Unlock()
	etcdWrite(key, value)

	for i := range node.Clients {
		go func(index int, client Client) {
			shard := segments[index+1]
			//shard := value
			buffer := make([]byte, 13+len(key)+len(shard))
			buffer[0] = OpWrite
			binary.LittleEndian.PutUint32(buffer[1:5], commitIndex)
			binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
			binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(shard)))
			keyIndex := 13 + len(key) //fix
			copy(buffer[13:keyIndex], key)
			copy(buffer[keyIndex:keyIndex+len(shard)], shard)
			//fmt.Printf("Writing shard: %d\n", shard)
			client.mutex.Lock()
			err := client.Write(buffer)
			client.mutex.Unlock()
			if err != nil {
				panic(err)
			}
		}(i, node.Clients[i])
	}

	//block(key, value)
	<-entry.condition
}
