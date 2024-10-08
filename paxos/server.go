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
var RequestId uint32

type Node struct {
	Clients       []Client
	RequestLock   *sync.Mutex
	RequestWaiter map[uint32]chan struct{}
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
	requestId uint32
}

type ProposePacket struct {
	Slot      uint32
	Key       []byte
	Value     []byte
	RequestId uint32
}

type CommitPacket struct {
	RequestIds []uint32
	Next       uint32
}

func GetProposePacket(buffer []byte, hasSlot bool) ProposePacket {
	requestId := binary.LittleEndian.Uint32(buffer[:4])
	keySize := binary.LittleEndian.Uint32(buffer[4:8])
	valueSize := binary.LittleEndian.Uint32(buffer[8:12])
	slot := binary.LittleEndian.Uint32(buffer[12:16])
	keyEnd := 16 + keySize
	keyStart := 16
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	copy(key, buffer[keyStart:keyEnd])
	copy(value, buffer[keyEnd:keyEnd+valueSize])
	return ProposePacket{
		Slot:      slot,
		Key:       key,
		Value:     value,
		RequestId: requestId,
	}
}

func (client Client) WriteProposePacket(packet ProposePacket, op uint8) {
	size := 17 + len(packet.Key) + len(packet.Value)
	buffer := make([]byte, size)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size+4))
	buffer[5] = op

	//buffer[0] = OpForward
	binary.LittleEndian.PutUint32(buffer[5:9], packet.RequestId)
	binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(packet.Key)))
	binary.LittleEndian.PutUint32(buffer[13:17], uint32(len(packet.Value)))
	binary.LittleEndian.PutUint32(buffer[17:21], packet.Slot)
	//binary.LittleEndian.PutUint32(buffer[9:13], requestId)
	//var keyIndex = 13 + len(key)
	var keyEnd = 21 + len(packet.Key)
	copy(buffer[21:keyEnd], packet.Key)
	copy(buffer[keyEnd:keyEnd+len(packet.Value)], packet.Value)
	client.mutex.Lock()
	err := client.Write(buffer)
	if err != nil {
		panic("error forwarding to leader!")
	}
	client.mutex.Unlock()
}

func (client Client) WriteCommitPacket(packet CommitPacket) {
	size := 9 + (4 * len(packet.RequestIds))
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[5] = OpCommit
	binary.LittleEndian.PutUint32(buffer[5:9], packet.Next)
	binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(packet.RequestIds)))
	for i, requestId := range packet.RequestIds {
		binary.LittleEndian.PutUint32(buffer[13+(i*4):], requestId)
	}
	client.mutex.Lock()
	err := client.Write(buffer)
	if err != nil {
		panic("error forwarding to leader!")
	}
	client.mutex.Unlock()
}

func GetCommitPacket(buffer []byte) CommitPacket {
	next := binary.LittleEndian.Uint32(buffer[:4])
	totalRequestIds := binary.LittleEndian.Uint32(buffer[4:8])
	requestIds := make([]uint32, totalRequestIds)
	for i := uint32(0); i < totalRequestIds; i++ {
		requestIds[i] = binary.LittleEndian.Uint32(buffer[8+(i*4) : 8+(i+1)*4])
	}

	return CommitPacket{
		Next:       next,
		RequestIds: requestIds,
	}
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
				sizeBuffer := make([]byte, 4)
				buffer := make([]byte, 65535)
				for {
					err := reader.Read(sizeBuffer)
					if err != nil {
						panic(err)
					}
					packetSize := binary.LittleEndian.Uint32(buffer[:4])

					if packetSize > uint32(len(buffer)) {
						buffer = append(buffer, make([]byte, packetSize-uint32(len(buffer)))...)
					}

					err = reader.Read(buffer)
					if err != nil {
						panic(err)
					}

					op := buffer[0]
					if op == OpPropose {
						proposal := GetProposePacket(buffer[1:], true)
						entry := &Entry{
							key:       proposal.Key,
							value:     proposal.Value,
							acked:     1,
							majority:  uint32(node.Quorum),
							condition: make(chan struct{}),
							requestId: proposal.RequestId,
						}
						node.Log.Lock.Lock()
						node.Log.Entries[proposal.Slot] = entry
						node.Log.Lock.Unlock()

						go func() {
							response := make([]byte, 9)
							binary.LittleEndian.PutUint32(response[:4], 5)
							response[5] = OpAck
							binary.LittleEndian.PutUint32(response[5:], proposal.Slot)
							client := node.Clients[index]
							client.mutex.Lock()
							err = client.Write(response)
							client.mutex.Unlock()
							if err != nil {
								panic(err)
							}
							fmt.Printf("Acked back for node=%d slot=%d\n", index, proposal.Slot)
						}()
					} else if op == OpForward {
						fmt.Printf("Got forward from: %d\n", index)
						forward := GetProposePacket(buffer[1:], false)
						go func() {
							node.Write(forward.Key, forward.Value, false, forward.RequestId)
						}()
					} else if op == OpAck {
						slot := binary.LittleEndian.Uint32(buffer[1:5])
						fmt.Printf("\nGot ack from %d for %d\n", index, slot)
						go func() {
							node.Log.Lock.Lock()
							entry, exists := node.Log.Entries[slot]
							node.Log.Lock.Unlock()

							if exists && atomic.AddUint32(&entry.acked, 1) == entry.majority {
								fmt.Printf("Got first majority for node=%d slot=%d\n", index, slot)
								var requestsIds []uint32
								CommitLock.Lock()
								start := CommitIndex
								for {
									next := CommitIndex + 1

									node.Log.Lock.Lock()
									nextEntry, nextEntryExists := node.Log.Entries[next]
									node.Log.Lock.Unlock()

									if !nextEntryExists {
										fmt.Printf("Does not exist for node=%d slot=%d next=%d\n", index, slot, next)
										break
									}

									if atomic.LoadUint32(&nextEntry.acked) >= nextEntry.majority {
										fmt.Printf("Got next majority for node=%d slot=%d next=%d\n", index, slot, next)
										CommitIndex = next
										requestsIds = append(requestsIds, nextEntry.requestId)
										etcdWrite(nextEntry.key, nextEntry.value)
										if nextEntry.condition != nil {
											fmt.Printf("Closing entry for node=%d slot=%d next=%d\n", index, slot, next)
											close(nextEntry.condition)
										}
										node.Log.Lock.Lock()
										delete(node.Log.Entries, next)
										node.Log.Lock.Unlock()
									} else {
										fmt.Printf("Didn't get majority for node=%d slot=%d next=%d\n", index, slot, next)
										break
									}

								}
								if start == CommitIndex {
									fmt.Printf("Start is the same for node=%d slot=%d start=%d commitIndex=%d\n", index, slot, start, CommitIndex)
									CommitLock.Unlock()
								} else {

									packet := CommitPacket{
										RequestIds: requestsIds,
										Next:       CommitIndex,
									}

									CommitLock.Unlock()

									fmt.Printf("Committing for node=%d slot=%d commitIndex=%d\n", index, slot, CommitIndex)
									for i := 0; i < node.Total; i++ {
										if i == node.Index {
											continue
										}
										go func(index int, client Client) {
											client.WriteCommitPacket(packet)
										}(i, node.Clients[i])
									}
								}
							}
						}()
					} else if op == OpCommit {
						commitPacket := GetCommitPacket(buffer[1:])

						go func() {
							CommitLock.Lock()
							for {
								fmt.Printf("Committing up to %d from %d\n", commitPacket.Next, CommitIndex)
								current := CommitIndex + 1
								//fmt.Printf("Looping %d up to %d\n", current, next)
								if current > commitPacket.Next {
									fmt.Printf("Too big\n")
									break
								}
								//

								node.Log.Lock.Lock()
								entry, exists := node.Log.Entries[current]
								delete(node.Log.Entries, current)
								node.Log.Lock.Unlock()
								//
								//if exists && !atomic.CompareAndSwapUint32(&CommitIndex, current-1, current) {
								//	continue
								//}
								CommitIndex = current

								if !exists {
									panic("major problem")
								}

								etcdWrite(entry.key, entry.value)
								if entry.condition != nil {
									close(entry.condition)
								}

								fmt.Printf("Which index did we get?: current=%d, next=%d, total=%d", int32(current), int32(commitPacket.Next), len(commitPacket.RequestIds))

								requestIndex := (int32(current) - (int32(commitPacket.Next) - int32(len(commitPacket.RequestIds)))) - 1
								fmt.Printf("Request index: %d\n", requestIndex)
								if requestIndex >= 0 {
									node.RequestLock.Lock()
									fmt.Printf("Total in there: %d\n", len(node.RequestWaiter))
									channel := node.RequestWaiter[commitPacket.RequestIds[requestIndex]]
									if channel != nil {
										fmt.Printf("We closed the channel for current=%d\n", current)
										close(channel)
									}
									delete(node.RequestWaiter, commitPacket.RequestIds[requestIndex])
									node.RequestLock.Unlock()
								}
							}

							fmt.Printf("We commited up to %d\n", atomic.LoadUint32(&CommitIndex))
							CommitLock.Unlock()

							//fmt.Printf("We commited up to %d\n", atomic.LoadUint32(&CommitIndex))

						}()

					}
				}
			}()
		}
	}
}

func (node *Node) ForwardRead(
	key []byte,
	etcdRead func(key []byte) []byte,
) []byte {
	if node.Index != node.Leader {
		// forward
	}

	return etcdRead(key)
}

func (node *Node) ForwardWrite(
	key []byte,
	value []byte,
) {
	// create requestId
	requestId := uint32(node.Index<<6 | int(atomic.AddUint32(&RequestId, 1)))
	if node.Index != node.Leader {
		println("Leader didnt get request forwarding!")
		packet := ProposePacket{
			Slot:      0,
			RequestId: requestId,
			Key:       key,
			Value:     value,
		}

		node.Clients[node.Leader].WriteProposePacket(packet, OpForward)
		channel := make(chan struct{})
		node.RequestLock.Lock()
		_, exists := node.RequestWaiter[requestId]
		if exists {
			println("IT ALREADY EXISTS IN THERE AND WE ARE OVERWRITING IT")
		}
		node.RequestWaiter[requestId] = channel
		node.RequestLock.Unlock()
		<-channel
	} else {
		node.Write(key, value, true, requestId)
	}
}

func (node *Node) Write(
	key []byte,
	value []byte,
	wait bool,
	requestId uint32,
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
		requestId: requestId,
	}
	node.Log.Lock.Lock()
	node.Log.Entries[appliedIndex] = entry
	node.Log.Lock.Unlock()

	for i := 0; i < node.Total; i++ {
		if i == node.Index {
			continue
		}
		go func(index int, client Client) {
			client.WriteProposePacket(ProposePacket{
				Key:       key,
				Value:     segments[index],
				Slot:      appliedIndex,
				RequestId: requestId,
			}, OpPropose)
		}(i, node.Clients[i])
	}

	if wait {
		<-entry.condition
	}
}
