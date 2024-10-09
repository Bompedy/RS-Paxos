package paxos

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
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

// var CommitLock sync.Mutex
var CommitIndex uint32
var AppliedIndex uint32

type Node struct {
	Clients       []Client
	RequestWaiter sync.Map
	RequestIds    sync.Map
	LogWaiter     sync.Map
	Total         int
	Encoder       reedsolomon.Encoder
	Entries       sync.Map
	Quorum        int
	Parity        int
	Segments      int
	Index         int
	Leader        int
}

type Entry struct {
	key       []byte
	value     []byte
	acked     uint32
	majority  uint32
	requestId uuid.UUID
}

type ProposePacket struct {
	Slot      uint32
	Key       []byte
	Value     []byte
	RequestId uuid.UUID
}

func GetProposePacket(buffer []byte) ProposePacket {
	//requestId := binary.LittleEndian.Uint32(buffer[:4])
	var requestId uuid.UUID
	copy(requestId[:], buffer[:16])
	keySize := binary.LittleEndian.Uint32(buffer[16:20])
	valueSize := binary.LittleEndian.Uint32(buffer[20:24])
	slot := binary.LittleEndian.Uint32(buffer[24:28])
	keyEnd := 28 + keySize
	keyStart := 28
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
	//println("Reading proposal")
	size := 29 + len(packet.Key) + len(packet.Value)
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = op

	//buffer[0] = OpForward
	copy(buffer[5:21], packet.RequestId[:])
	binary.LittleEndian.PutUint32(buffer[21:25], uint32(len(packet.Key)))
	binary.LittleEndian.PutUint32(buffer[25:29], uint32(len(packet.Value)))
	binary.LittleEndian.PutUint32(buffer[29:33], packet.Slot)
	//binary.LittleEndian.PutUint32(buffer[9:13], requestId)
	//var keyIndex = 13 + len(key)
	var keyEnd = 33 + len(packet.Key)
	copy(buffer[33:keyEnd], packet.Key)
	copy(buffer[keyEnd:keyEnd+len(packet.Value)], packet.Value)
	client.mutex.Lock()
	err := client.Write(buffer)
	if err != nil {
		panic("error forwarding to leader!")
	}
	client.mutex.Unlock()
	//println("Done reading proposal")
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

			commitChannel := make(chan uint32, 4096)

			go func() {
				for next := range commitChannel {
					//println("Got commit!")
					if next > CommitIndex+2048 {
						panic("GOT TOO FAR APART")
						next = CommitIndex + 2048
					}
					for {
						current := CommitIndex + 1
						if current > next {
							break
						}
						//var entry *Entry
						for {
							_, exists := node.Entries.LoadAndDelete(current)
							if exists {
								//entry = value.(*Entry)
								break
							} else {
								logWaiterValue, logWaiterExists := node.LogWaiter.Load(current)
								if logWaiterExists {
									<-logWaiterValue.(chan struct{})
								} else {
									node.LogWaiter.Store(current, make(chan struct{}))
								}
								fmt.Printf("we are so stuck on %d\n", current)
							}
						}
						node.LogWaiter.Delete(current)
						//etcdWrite(entry.key, entry.value)
						CommitIndex = current

						var requestId uuid.UUID
						value, exists := node.RequestIds.LoadAndDelete(current)
						if !exists {
							panic("BIG PROBLEM CAN'T FIND requestID")
						}

						requestId = value.(uuid.UUID)
						waiterValue, exists := node.RequestWaiter.LoadAndDelete(requestId)
						if exists {
							channel := waiterValue.(chan struct{})
							close(channel)
						}
					}
				}
			}()

			go func() {
				sizeBuffer := make([]byte, 4)
				buffer := make([]byte, 65535)
				for {
					err := reader.Read(sizeBuffer)
					if err != nil {
						panic(err)
					}
					packetSize := binary.LittleEndian.Uint32(sizeBuffer[:4])
					if packetSize > uint32(len(buffer)) {
						buffer = append(buffer, make([]byte, packetSize-uint32(len(buffer)))...)
					}

					err = reader.Read(buffer[:packetSize])
					if err != nil {
						panic(err)
					}

					op := buffer[0]
					if op == OpPropose {
						proposal := GetProposePacket(buffer[1:])
						entry := &Entry{
							key:       proposal.Key,
							value:     proposal.Value,
							acked:     1,
							majority:  uint32(node.Quorum),
							requestId: proposal.RequestId,
						}
						node.RequestIds.Store(proposal.Slot, entry.requestId)
						node.Entries.Store(proposal.Slot, entry)

						value, exists := node.LogWaiter.LoadAndDelete(proposal.Slot) // channel, close(channel)
						if exists {
							close(value.(chan struct{}))
						}

						go func() {
							// ack
							response := make([]byte, 9)
							binary.LittleEndian.PutUint32(response[:4], 5)
							response[4] = OpAck
							binary.LittleEndian.PutUint32(response[5:], proposal.Slot)
							client := node.Clients[index]
							client.mutex.Lock()
							err = client.Write(response)
							client.mutex.Unlock()
							if err != nil {
								panic(err)
							}
						}()
					} else if op == OpForward {
						forward := GetProposePacket(buffer[1:])
						go func() {
							node.Write(forward.Key, forward.Value, false, forward.RequestId)
						}()
					} else if op == OpAck {
						slot := binary.LittleEndian.Uint32(buffer[1:])
						fmt.Printf("Got ack for %d\n", slot)
						go func() {
							//CommitLock.Lock()
							value, exists := node.Entries.Load(slot)

							//commitIndex = 0
							//50
							//commitIndex = 5
							//10
							//100
							//commitIndex = 25
							//50

							if exists {
								//fmt.Printf("Exists: %d\n", slot)
								entry := value.(*Entry)
								if atomic.AddUint32(&entry.acked, 1) == entry.majority { // 4, 2
									start := atomic.LoadUint32(&CommitIndex)
									next := start + 1
									for {
										//next := start + 1
										nextValue, nextEntryExists := node.Entries.Load(next)
										if !nextEntryExists {
											break
										}

										nextEntry := nextValue.(*Entry)

										if atomic.LoadUint32(&nextEntry.acked) >= nextEntry.majority {
											if !atomic.CompareAndSwapUint32(&CommitIndex, next-1, next) {
												next -= 1
												break
											}

											next += 1
											//CommitIndex = next
											//etcdWrite(nextEntry.key, nextEntry.value)
											waiterValue, waiterExists := node.RequestWaiter.LoadAndDelete(nextEntry.requestId)
											if waiterExists {
												channel := waiterValue.(chan struct{})
												close(channel)
											} else {
												panic("LEADER DIDNT HAVE REQUEST ID")
											}

											node.Entries.Delete(next)
										} else {
											break
										}
									}

									if start != next {
										//fmt.Printf("Committing up to %d\n", CommitIndex)
										commitBuffer := make([]byte, 9)
										binary.LittleEndian.PutUint32(commitBuffer[:4], 5)
										commitBuffer[4] = OpCommit
										binary.LittleEndian.PutUint32(commitBuffer[5:9], next)

										for i := 0; i < node.Total; i++ {
											if i == node.Index {
												continue
											}
											client := node.Clients[i]
											client.mutex.Lock()
											err := client.Write(commitBuffer)
											if err != nil {
												panic("error forwarding to leader!")
											}
											client.mutex.Unlock()
										}
									}
								}
							}
							//fmt.Printf("Releasing lock: %d\n", slot)
							//CommitLock.Unlock()
						}()
					} else if op == OpCommit {
						slot := binary.LittleEndian.Uint32(buffer[1:])
						commitChannel <- slot
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
	//println("Forward to leader")
	requestId := uuid.New()
	if node.Index != node.Leader {
		packet := ProposePacket{
			Slot:      0,
			RequestId: requestId,
			Key:       key,
			Value:     value,
		}

		channel := make(chan struct{})
		node.RequestWaiter.Store(requestId, channel)
		node.Clients[node.Leader].WriteProposePacket(packet, OpForward)
		<-channel
	} else {
		node.Write(key, value, true, requestId)
	}
}

func (node *Node) Write(
	key []byte,
	value []byte,
	wait bool,
	requestId uuid.UUID,
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
	//fmt.Printf("Got forward: %d\n", appliedIndex)
	entry := &Entry{
		key:       key,
		value:     value,
		acked:     1,
		majority:  uint32(node.Quorum),
		requestId: requestId,
	}
	node.Entries.Store(appliedIndex, entry)
	channel := make(chan struct{})
	node.RequestWaiter.Store(requestId, channel)
	//
	//node.Log.Lock.Lock()
	//node.Log.Entries[appliedIndex] = entry
	//node.Log.Lock.Unlock()

	for i := 0; i < node.Total; i++ {
		if i == node.Index {
			continue
		}
		i := i
		go func(index int, client Client) {
			//fmt.Printf("Sending proposal %d to %d\n", appliedIndex, i)
			client.WriteProposePacket(ProposePacket{
				Key:       key,
				Value:     segments[i],
				Slot:      appliedIndex,
				RequestId: requestId,
			}, OpPropose)
		}(i, node.Clients[i])
	}

	if wait {
		<-channel
	}
}
