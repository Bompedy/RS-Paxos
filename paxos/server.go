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
var OpRead = uint8(4)

var ReadType = uint8(0)
var WriteType = uint8(1)

// var CommitLock sync.Mutex
var CommitIndex uint32
var AppliedIndex uint32

type Node struct {
	Clients            []Client
	ReadRequestWaiter  sync.Map
	ReadSenders        sync.Map
	WriteRequestWaiter sync.Map
	RequestIds         sync.Map
	LogWaiter          sync.Map
	Total              int
	Encoder            reedsolomon.Encoder
	Entries            sync.Map
	Quorum             int
	Parity             int
	Segments           int
	Index              int
	Leader             int
}

type Entry struct {
	key       []byte
	value     []byte
	acked     uint32
	majority  uint32
	Type      uint8
	requestId uuid.UUID
}

type ProposePacket struct {
	Type      uint8
	Slot      uint32
	Key       []byte
	Value     []byte
	RequestId uuid.UUID
	Sender    uint8
}

type ReadResult struct {
	requestId uuid.UUID
	value     []byte
}

func GetProposePacket(buffer []byte) ProposePacket {
	//requestId := binary.LittleEndian.Uint32(buffer[:4])
	var requestId uuid.UUID
	copy(requestId[:], buffer[:16])
	keySize := binary.LittleEndian.Uint32(buffer[16:20])
	valueSize := binary.LittleEndian.Uint32(buffer[20:24])
	slot := binary.LittleEndian.Uint32(buffer[24:28])
	packetType := buffer[28]
	sender := buffer[29]
	keyEnd := 30 + keySize
	keyStart := 30
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	copy(key, buffer[keyStart:keyEnd])
	if packetType != ReadType {
		copy(value, buffer[keyEnd:keyEnd+valueSize])
	}
	return ProposePacket{
		Slot:      slot,
		Key:       key,
		Value:     value,
		RequestId: requestId,
		Type:      packetType,
		Sender:    sender,
	}
}

func (client Client) WriteProposePacket(packet ProposePacket, op uint8) {
	//println("Reading proposal")
	size := 31 + len(packet.Key) + len(packet.Value)
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = op

	//buffer[0] = OpForward
	copy(buffer[5:21], packet.RequestId[:])
	binary.LittleEndian.PutUint32(buffer[21:25], uint32(len(packet.Key)))
	binary.LittleEndian.PutUint32(buffer[25:29], uint32(len(packet.Value)))
	binary.LittleEndian.PutUint32(buffer[29:33], packet.Slot)
	buffer[33] = packet.Type
	buffer[34] = packet.Sender
	//binary.LittleEndian.PutUint32(buffer[9:13], requestId)
	//var keyIndex = 13 + len(key)
	var keyEnd = 35 + len(packet.Key)
	copy(buffer[35:keyEnd], packet.Key)
	if packet.Type != ReadType {
		copy(buffer[keyEnd:keyEnd+len(packet.Value)], packet.Value)
	}
	client.mutex.Lock()
	err := client.Write(buffer)
	if err != nil {
		panic("error forwarding to leader!")
	}
	client.mutex.Unlock()
	//println("Done reading proposal")
}

func (client Client) WriteReadPacket(value []byte, requestId uuid.UUID) {
	size := 17 + len(value)
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = OpRead
	copy(buffer[5:21], requestId[:])
	copy(buffer[21:21+len(value)], value)
	client.mutex.Lock()
	err := client.Write(buffer)
	if err != nil {
		panic("error forwarding to leader!")
	}
	client.mutex.Unlock()
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
	etcdRead func(key []byte) []byte,
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
			readChannel := make(chan ReadResult, 4096)

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
						var entry *Entry
						for {
							value, exists := node.Entries.LoadAndDelete(current)
							if exists {
								entry = value.(*Entry)
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
							panic("BIG PROBLEM CAN'T FIND WRITE requestID")
						}

						if entry.Type == WriteType {
							etcdWrite(entry.key, entry.value)
							requestId = value.(uuid.UUID)
							waiterValue, exists := node.WriteRequestWaiter.LoadAndDelete(requestId)
							if exists {
								channel := waiterValue.(chan struct{})
								close(channel)
							}
						}
					}
				}
			}()

			go func() {
				for readResult := range readChannel {
					value, exists := node.ReadRequestWaiter.LoadAndDelete(readResult.requestId)
					if !exists {
						panic("BIG PROBLEM CAN'T FIND READ requestID")
					}
					channel := value.(chan []byte)
					channel <- readResult.value
					close(channel)
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
							Type:      proposal.Type,
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
							if forward.Type == ReadType {
								node.Read(forward.Key, false, forward.RequestId, forward.Sender)
							} else {
								node.Write(forward.Key, forward.Value, false, forward.RequestId)
							}
						}()
					} else if op == OpAck {
						slot := binary.LittleEndian.Uint32(buffer[1:])
						fmt.Printf("Got ack for slot=%d node=%d\n", slot, index)
						go func() {
							value, exists := node.Entries.Load(slot)

							if exists {
								entry := value.(*Entry)
								if atomic.AddUint32(&entry.acked, 1) == entry.majority { // 4, 2
									start := atomic.LoadUint32(&CommitIndex)
									next := start + 1
									for {
										nextValue, nextEntryExists := node.Entries.Load(next)
										if !nextEntryExists {
											next -= 1
											break
										}

										nextEntry := nextValue.(*Entry)

										if atomic.LoadUint32(&nextEntry.acked) >= nextEntry.majority {
											if !atomic.CompareAndSwapUint32(&CommitIndex, next-1, next) {
												next -= 1
												break
											}

											if nextEntry.Type == ReadType {
												fmt.Printf("Got a read type slot=%d id=%s\n", next, nextEntry.requestId)
												bytes := etcdRead(nextEntry.key)
												senderValue, senderExists := node.ReadSenders.Load(nextEntry.requestId)
												waiterValue, waiterExists := node.ReadRequestWaiter.LoadAndDelete(nextEntry.requestId)
												if senderExists && waiterExists {
													senderIndex := senderValue.(uint8)
													if senderValue.(uint8) == uint8(node.Index) {
														channel := waiterValue.(chan []byte)
														channel <- bytes
														close(channel)
													} else {
														node.Clients[senderIndex].WriteReadPacket(bytes, nextEntry.requestId)
													}
												} else {
													panic("LEADER DIDNT HAVE READ SENDER or WAITER")
												}

											} else {
												etcdWrite(nextEntry.key, nextEntry.value)
												waiterValue, waiterExists := node.WriteRequestWaiter.LoadAndDelete(nextEntry.requestId)
												if waiterExists {
													channel := waiterValue.(chan struct{})
													close(channel)
												} else {
													panic("LEADER DIDNT HAVE WRITE REQUEST ID")
												}
											}

											node.Entries.Delete(next)
											next += 1
										} else {
											break
										}
									}

									if start != next {
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
						}()
					} else if op == OpCommit {
						slot := binary.LittleEndian.Uint32(buffer[1:])
						commitChannel <- slot
					} else if op == OpRead {
						var requestId uuid.UUID
						value := make([]byte, packetSize-17)
						copy(buffer[1:17], requestId[:])
						copy(value, buffer[17:packetSize-17])
						readChannel <- ReadResult{requestId: requestId, value: value}
					}
				}
			}()
		}
	}
}

func (node *Node) ForwardRead(
	key []byte,
) []byte {
	requestId := uuid.New()
	if node.Index != node.Leader {
		channel := make(chan []byte)
		node.ReadRequestWaiter.Store(requestId, channel)
		packet := ProposePacket{
			Slot:      0,
			RequestId: requestId,
			Key:       key,
			Value:     make([]byte, 0),
			Type:      ReadType,
			Sender:    uint8(node.Index),
		}
		node.Clients[node.Leader].WriteProposePacket(packet, OpForward)
		return <-channel
	}

	return node.Read(key, true, requestId, uint8(node.Index))
}

func (node *Node) Read(
	key []byte,
	wait bool,
	requestId uuid.UUID,
	sender uint8,
) []byte {
	channel := make(chan []byte)
	node.ReadRequestWaiter.Store(requestId, channel)
	node.ReadSenders.Store(requestId, sender)
	packet := ProposePacket{
		Slot:      0,
		RequestId: requestId,
		Key:       key,
		Value:     make([]byte, 0),
		Type:      ReadType,
		Sender:    sender,
	}

	appliedIndex := atomic.AddUint32(&AppliedIndex, 1)
	entry := &Entry{
		key:       key,
		value:     make([]byte, 0),
		acked:     1,
		majority:  uint32(node.Quorum),
		requestId: requestId,
	}

	node.Entries.Store(appliedIndex, entry)
	for i := 0; i < node.Total; i++ {
		if i == node.Index {
			continue
		}
		i := i
		go func(index int, client Client) {
			client.WriteProposePacket(packet, OpPropose)
		}(i, node.Clients[i])
	}

	if wait {
		fmt.Printf("Waiting on read for %s\n", requestId.String())
		return <-channel
	}

	return make([]byte, 0)
}

func (node *Node) ForwardWrite(
	key []byte,
	value []byte,
) {
	requestId := uuid.New()
	if node.Index != node.Leader {
		packet := ProposePacket{
			Slot:      0,
			RequestId: requestId,
			Key:       key,
			Value:     value,
			Type:      WriteType,
			Sender:    uint8(node.Index),
		}

		channel := make(chan struct{})
		node.WriteRequestWaiter.Store(requestId, channel)
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
	entry := &Entry{
		key:       key,
		value:     value,
		acked:     1,
		majority:  uint32(node.Quorum),
		requestId: requestId,
		Type:      WriteType,
	}
	node.Entries.Store(appliedIndex, entry)
	channel := make(chan struct{})
	node.WriteRequestWaiter.Store(requestId, channel)

	for i := 0; i < node.Total; i++ {
		if i == node.Index {
			continue
		}
		i := i
		go func(index int, client Client) {
			client.WriteProposePacket(ProposePacket{
				Key:       key,
				Value:     segments[i],
				Slot:      appliedIndex,
				RequestId: requestId,
				Type:      WriteType,
				Sender:    uint8(node.Index),
			}, OpPropose)
		}(i, node.Clients[i])
	}

	if wait {
		<-channel
	}
}
