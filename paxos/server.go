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

var OpPropose = uint8(0)
var OpCommit = uint8(1)
var OpForward = uint8(2)
var OpAck = uint8(3)

var CommitIndex uint32
var AppliedIndex uint32

type Node struct {
	Clients       []Client
	RequestLock   *sync.Mutex
	RequestWaiter map[string]chan struct{}
	Total         int
	Encoder       reedsolomon.Encoder
	Log           Log
	Quorum        int
	Parity        int
	Segments      int
	Index         int
	Leader        int
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
					if op == OpPropose {
						err := client.Read(buffer[:12])
						if err != nil {
							panic(err)
						}
						slot := binary.LittleEndian.Uint32(buffer[:4])
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
							entry := &Entry{
								key:       key,
								value:     value,
								acked:     1,
								majority:  uint32(node.Quorum),
								condition: make(chan struct{}),
							}
							node.Log.Lock.Lock()
							node.Log.Entries[slot] = entry
							node.Log.Lock.Unlock()

							//etcdWrite(key, value)
							response := make([]byte, 5)
							buffer[0] = OpAck
							binary.LittleEndian.PutUint32(response[1:], slot)
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
							node.Write(key, value, false)
						}()
					} else if op == OpAck {
						err = client.Read(buffer)
						if err != nil {
							panic(err)
						}
						slot := binary.LittleEndian.Uint32(buffer)
						go func() {
							node.Log.Lock.Lock()
							entry, exists := node.Log.Entries[slot]
							node.Log.Lock.Unlock()

							if exists && atomic.AddUint32(&entry.acked, 1) == entry.majority {
								current := atomic.LoadUint32(&CommitIndex)
								next := current
								for {
									var i = next + 1
									node.Log.Lock.Lock()
									nextEntry, nextEntryExists := node.Log.Entries[i]
									node.Log.Lock.Unlock()

									if !nextEntryExists {
										break
									}

									if atomic.LoadUint32(&nextEntry.acked) >= nextEntry.majority {
										etcdWrite(nextEntry.key, nextEntry.value)
										close(nextEntry.condition)
										node.Log.Lock.Lock()
										delete(node.Log.Entries, i)
										node.Log.Lock.Unlock()
										next = i
									}
								}

								if next == current {
									return
								}

								for next > current && !atomic.CompareAndSwapUint32(&CommitIndex, current, next) {
									current = atomic.LoadUint32(&CommitIndex)
								}

								commitBuffer := make([]byte, 5)
								commitBuffer[0] = OpCommit
								binary.LittleEndian.PutUint32(buffer[1:5], current)
								for i := range node.Clients {
									go func(index int, client Client) {
										client.mutex.Lock()
										err := client.Write(buffer)
										if err != nil {
											panic("error writing!")
											return
										}
										client.mutex.Unlock()
									}(i, node.Clients[i])
								}
							}
						}()
					} else if op == OpCommit {
						commitBuffer := make([]byte, 4)
						err = client.Read(commitBuffer)
						if err != nil {
							panic(err)
						}

						next := binary.LittleEndian.Uint32(buffer[:4])

						go func() {
							current := atomic.LoadUint32(&CommitIndex)
							for {
								i := current + 1
								if i > next {
									break
								}

								node.Log.Lock.Lock()
								entry, exists := node.Log.Entries[i]
								delete(node.Log.Entries, i)
								node.Log.Lock.Unlock()

								if !exists {
									panic("Couldn't find it in log!")
								}

								etcdWrite(entry.key, entry.value)
								close(entry.condition)

								node.RequestLock.Lock()
								close(node.RequestWaiter[string(entry.key)])
								node.RequestLock.Unlock()
							}

							for next > current && !atomic.CompareAndSwapUint32(&CommitIndex, current, next) {
								current = atomic.LoadUint32(&CommitIndex)
							}
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
) {
	if node.Index != node.Leader {
		println("Leader didnt get request forwarding!")
		buffer := make([]byte, 9+len(key)+len(value))
		buffer[0] = OpForward
		binary.LittleEndian.PutUint32(buffer[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(value)))
		var keyIndex = 9 + len(key)
		copy(buffer[9:keyIndex], key)
		copy(buffer[keyIndex:keyIndex+len(value)], value)
		var leader = node.Clients[node.Leader]
		leader.mutex.Lock()
		err := leader.Write(buffer)
		if err != nil {
			panic("error forwarding to leader!")
		}
		leader.mutex.Unlock()
		channel := make(chan struct{})
		node.RequestLock.Lock()
		node.RequestWaiter[string(key)] = channel
		node.RequestLock.Unlock()
		<-channel
	} else {
		println("Leader got request!")
		node.Write(key, value, true)
	}
}

func (node *Node) Write(
	key []byte,
	value []byte,
	wait bool,
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

	appliedIndex := atomic.AddUint32(&AppliedIndex, 1)
	entry := &Entry{
		key:       key,
		value:     value,
		acked:     1,
		majority:  uint32(node.Quorum),
		condition: make(chan struct{}),
	}
	node.Log.Lock.Lock()
	node.Log.Entries[appliedIndex] = entry
	node.Log.Lock.Unlock()
	//etcdWrite(key, value)

	for i := range node.Clients {
		go func(index int, client Client) {
			shard := segments[index+1]
			//shard := value
			buffer := make([]byte, 13+len(key)+len(shard))
			buffer[0] = OpPropose
			binary.LittleEndian.PutUint32(buffer[1:5], appliedIndex)
			binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(key)))
			binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(shard)))
			keyIndex := 13 + len(key)
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

	//block(key, value)
	if wait {
		<-entry.condition
	}
}
