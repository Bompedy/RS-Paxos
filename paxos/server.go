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

var CommitLock sync.Mutex
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
	lock      *sync.Mutex
}

func (node *Node) Connect(
	local string,
	nodes []string,
) error {
	var waiter sync.WaitGroup
	for i, address := range nodes {
		if address == local {
			continue
		}
		waiter.Add(1)
		address := fmt.Sprintf("%s:2000", address)
		i := i
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
			indexBuffer := make([]byte, 1)
			indexBuffer[0] = uint8(node.Index)
			err = client.Write(indexBuffer)
			if err != nil {
				panic("Error writing index!")
			}

			//node.Clients = append(node.Clients, client)
			fmt.Printf("Appending: %d\n", i)
			node.Clients[i] = client
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

			reader := Client{
				connection: connection,
			}

			indexBuffer := make([]byte, 1)
			err = reader.Read(indexBuffer)
			if err != nil {
				panic("Error reading index!")
			}
			index := uint32(indexBuffer[0])

			go func() {
				buffer := make([]byte, 65535)
				for {
					//fmt.Printf("Waiting for op: %d\n", index)
					err := reader.Read(buffer[:1])
					//fmt.Printf("Anything: %d\n", index)
					if err != nil {
						panic(err)
					}
					op := buffer[0]
					//fmt.Printf("Got op: %d %d\n", index, op)
					if op == OpPropose {
						//fmt.Printf("Got proposal from: %d\n", index)
						err := reader.Read(buffer[:12])
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

						err = reader.Read(buffer[:(keySize + valueSize)])
						if err != nil {
							panic(err)
						}

						key := make([]byte, keySize)
						value := make([]byte, valueSize)
						copy(key, buffer[:keySize])
						copy(value, buffer[keySize:(keySize+valueSize)])
						//
						//entry := &Entry{
						//	key:       key,
						//	value:     value,
						//	acked:     1,
						//	majority:  uint32(node.Quorum),
						//	condition: make(chan struct{}),
						//}
						//node.Log.Lock.Lock()
						//node.Log.Entries[slot] = entry
						//fmt.Printf("Placed entry into slot: %d\n", slot)
						//node.Log.Lock.Unlock()

						entry := &Entry{
							key:       key,
							value:     value,
							acked:     1,
							majority:  uint32(node.Quorum),
							condition: make(chan struct{}),
							lock:      &sync.Mutex{},
						}
						node.Log.Lock.Lock()
						node.Log.Entries[slot] = entry
						//fmt.Printf("Placed entry into slot: %d\n", slot)
						node.Log.Lock.Unlock()

						go func() {

							//etcdWrite(key, value)
							response := make([]byte, 5)
							response[0] = OpAck
							binary.LittleEndian.PutUint32(response[1:], slot)
							client := node.Clients[index]
							client.mutex.Lock()
							err = client.Write(response)
							client.mutex.Unlock()
							if err != nil {
								panic(err)
							}
							//fmt.Printf("Acked back to: %d\n", index)
						}()
					} else if op == OpForward {
						//fmt.Printf("Got forward from: %d\n", index)
						err := reader.Read(buffer[:8])
						if err != nil {
							panic(err)
						}
						keySize := binary.LittleEndian.Uint32(buffer[:4])
						valueSize := binary.LittleEndian.Uint32(buffer[4:8])
						required := int(keySize + valueSize)
						if len(buffer) < required {
							buffer = append(buffer, make([]byte, required-len(buffer))...)
						}
						err = reader.Read(buffer[:(keySize + valueSize)])
						key := make([]byte, keySize)
						value := make([]byte, valueSize)
						copy(key, buffer[:keySize])
						copy(value, buffer[keySize:(keySize+valueSize)])
						go func() {
							node.Write(key, value, false)
							//println("Finished forwarding write from leader!")
						}()
					} else if op == OpAck {
						err = reader.Read(buffer[:4])
						if err != nil {
							panic(err)
						}
						slot := binary.LittleEndian.Uint32(buffer[:4])
						fmt.Printf("\nGot ack from %d for %d\n", index, slot)
						//go func() {
						node.Log.Lock.Lock()
						entry, exists := node.Log.Entries[slot]
						node.Log.Lock.Unlock()

						if exists {
							entry.lock.Lock()
							entry.acked += 1
							majority := entry.acked == entry.majority
							entry.lock.Unlock()
							if majority {
								CommitLock.Lock()
								start := CommitIndex
								for {
									next := CommitIndex + 1
									node.Log.Lock.Lock()
									nextEntry, nextEntryExists := node.Log.Entries[next]
									node.Log.Lock.Unlock()
									if !nextEntryExists {
										fmt.Printf("It does not exist for %d\n", next)
										break
									}
									fmt.Printf("Exists for %d\n", next)
									nextEntry.lock.Lock()
									nextMajority := nextEntry.acked >= nextEntry.majority
									nextEntry.lock.Unlock()
									if nextMajority {
										CommitIndex = next
										if nextEntry.condition != nil {
											fmt.Printf("Closing entry condition in ack\n")
											close(nextEntry.condition)
										}
										node.Log.Lock.Lock()
										delete(node.Log.Entries, next)
										node.Log.Lock.Unlock()
									} else {
										break
									}

								}
								if start == CommitIndex {
									CommitLock.Unlock()
									return
								}

								commitBuffer := make([]byte, 5)
								commitBuffer[0] = OpCommit
								binary.LittleEndian.PutUint32(commitBuffer[1:5], CommitIndex)
								fmt.Printf("Committing up to %d\n", CommitIndex)
								for i := 0; i < node.Total; i++ {
									if i == node.Index {
										continue
									}
									client := node.Clients[i]
									//go func(index int, client Client) {
									client.mutex.Lock()
									err := client.Write(commitBuffer)
									if err != nil {
										panic("error writing!")
										return
									}
									client.mutex.Unlock()
									//}(i, node.Clients[i])
								}

								fmt.Printf("Finished writing up to %d\n", CommitIndex)

								CommitLock.Unlock()
							}
						}
					} else if op == OpCommit {
						//println("Got commit")
						commitBuffer := make([]byte, 4)
						err = reader.Read(commitBuffer)
						if err != nil {
							panic(err)
						}

						next := binary.LittleEndian.Uint32(commitBuffer[:4])
						//fmt.Printf("Going to commit up to %d\n", next)

						go func() {
							for {
								fmt.Printf("Trying commit lock\n")
								CommitLock.Lock()
								fmt.Printf("Inside commit loop\n")
								current := CommitIndex + 1
								//fmt.Printf("Looping %d up to %d\n", current, next)
								if current > next {
									fmt.Printf("Too big\n")
									break
								}
								//

								node.Log.Lock.Lock()
								//fmt.Printf("Looking for log entry %d\n", current)
								entry, exists := node.Log.Entries[current]
								delete(node.Log.Entries, current)
								node.Log.Lock.Unlock()
								//
								//if exists && !atomic.CompareAndSwapUint32(&CommitIndex, current-1, current) {
								//	continue
								//}
								CommitIndex = current

								if !exists {
									//fmt.Printf("Couldn't find entry %d\n", current)
									panic("major problem")
								}

								//etcdWrite(entry.key, entry.value)
								if entry.condition != nil {
									//fmt.Printf("Closing condition in commit\n")
									close(entry.condition)
								}

								node.RequestLock.Lock()
								keyString := string(entry.key)
								channel := node.RequestWaiter[keyString]
								if channel != nil {
									//fmt.Printf("Closing request in commit\n")
									close(channel)
								}
								delete(node.RequestWaiter, keyString)
								node.RequestLock.Unlock()

								//fmt.Printf("i=%d vs current=%d\n", i, current)
							}

							CommitLock.Unlock()

							fmt.Printf("We commited up to %d\n", atomic.LoadUint32(&CommitIndex))
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
		//println("Forwarded to leader!")
		<-channel
	} else {
		//println("Leader got request!")
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
		lock:      &sync.Mutex{},
	}
	node.Log.Lock.Lock()
	node.Log.Entries[appliedIndex] = entry
	node.Log.Lock.Unlock()

	for i := 0; i < node.Total; i++ {
		//fmt.Printf("Writing to index: %d\n", i)
		if i == node.Index {
			//println("Skipping")
			continue
		}
		go func(index int, client Client) {
			shard := segments[index]
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
	//println("passed condition!")
}
