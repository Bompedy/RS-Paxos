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

type Node struct {
	Clients  []Client
	Total    int
	Encoder  reedsolomon.Encoder
	Log      Log
	Quorum   int
	Parity   int
	Segments int
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
	block func(key []byte, value []byte),
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
					//println("Does it exist: ", exists)

					//fmt.Printf("Leader got response: %d, %d, %d, %v\n", commitIndex, acked, entry.majority, exists)
					if exists && atomic.AddUint32(&entry.acked, 1) == entry.majority {
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
	return nil
}

type Task struct {
	Key       []byte
	Value     []byte
	Condition chan struct{}
}

var taskQueue = make(chan *Task)

func (node *Node) Accept(
	address string,
	block func(key []byte, value []byte),
) error {

	go func() {
		for {
			task, ok := <-taskQueue
			if !ok {
				println("There was some error!")
				break
			}
			block(task.Key, task.Value)
			close(task.Condition)
		}
	}()

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

						go func() {
							block(buffer[:keySize], buffer[keySize:(keySize+valueSize)])
							response := make([]byte, 4)
							binary.LittleEndian.PutUint32(response, commitIndex)
							mutex.Lock()
							err = client.Write(response)
							mutex.Unlock()
							if err != nil {
								panic(err)
							}
						}()
					}
				}
			}()
		}
	}
}

var CommitIndex uint32

func (node *Node) Write(
	key []byte,
	value []byte,
	block func(key []byte, value []byte),
) {
	//fmt.Printf("Total size: %d", len(value))
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
