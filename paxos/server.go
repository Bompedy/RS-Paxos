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
	"time"
)

var OpPropose = uint8(0)
var OpCommit = uint8(1)
var OpForward = uint8(2)
var OpAck = uint8(3)
var OpRead = uint8(4)
var OpElection = uint8(5)
var OpVote = uint8(6)

// var OpElectionResponse = uint8(5)
var OpHeartbeat = uint8(6)

var ReadType = uint8(0)
var WriteType = uint8(1)

var CommitIndex uint32
var AppliedIndex uint32

type Node struct {
	Clients            []Client
	Failed             []bool
	Keys               sync.Map
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

func (node *Node) WriteProposePacket(client Client, packet ProposePacket, op uint8) {
	size := 31 + len(packet.Key) + len(packet.Value)
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = op
	copy(buffer[5:21], packet.RequestId[:])
	binary.LittleEndian.PutUint32(buffer[21:25], uint32(len(packet.Key)))
	binary.LittleEndian.PutUint32(buffer[25:29], uint32(len(packet.Value)))
	binary.LittleEndian.PutUint32(buffer[29:33], packet.Slot)
	buffer[33] = packet.Type
	buffer[34] = packet.Sender
	var keyEnd = 35 + len(packet.Key)
	copy(buffer[35:keyEnd], packet.Key)
	if packet.Type != ReadType {
		copy(buffer[keyEnd:keyEnd+len(packet.Value)], packet.Value)
	}
	client.mutex.Lock()
	err := client.Write(buffer)
	client.mutex.Unlock()
	if err != nil {
		if client.index == uint32(node.Leader) {
			println("setting new leader!")
			node.Leader = 1
		}
		node.Failed[client.index] = true
		//panic("error forwarding to leader!")
	}
}

func (node *Node) WriteReadPacket(client Client, value []byte, requestId uuid.UUID) {
	size := 17 + len(value)
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = OpRead
	copy(buffer[5:21], requestId[:])
	if size > 17 {
		copy(buffer[21:21+len(value)], value)
	}
	client.mutex.Lock()
	err := client.Write(buffer)
	client.mutex.Unlock()
	if err != nil {
		if client.index == uint32(node.Leader) {
			println("setting new leader!")
			node.Leader = 1
		}
		node.Failed[client.index] = true
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
				index:      uint32(i),
			}
			indexBuffer := make([]byte, 1)
			indexBuffer[0] = uint8(node.Index)
			err = client.Write(indexBuffer)
			if err != nil {
				panic("Error writing index!")
			}
			fmt.Printf("Appending: %d\n", i)
			node.Clients[i] = client
			node.Failed[i] = false
		}()
	}

	waiter.Wait()

	//go func() {
	//	for {
	//		buffer := make([]byte, 9)
	//		binary.LittleEndian.PutUint32(buffer[:4], 9)
	//		buffer[4] = OpHeartbeat
	//		binary.LittleEndian.PutUint32(buffer[5:9], atomic.LoadUint32(&CommitIndex))
	//
	//		for i := range node.Clients {
	//			client := node.Clients[i]
	//			client.mutex.Lock()
	//			err := client.Write(buffer)
	//			client.mutex.Unlock()
	//			if err != nil {
	//				node.Failed[i] = true
	//				break
	//			}
	//		}
	//
	//		time.Sleep(time.Duration(rand.Intn(75)+100) * time.Millisecond)
	//	}
	//}()
	//
	//go func() {
	//	buffer := make([]byte, 9)
	//	for i := range node.Clients {
	//		i := i
	//		go func(client Client) {
	//			for {
	//				err := client.Read(buffer)
	//				if err != nil {
	//					node.Failed[i] = true
	//					break
	//				}
	//
	//				commitIndex := binary.LittleEndian.Uint32(buffer[5:9])
	//				if node.Failed[node.Leader] && commitIndex < atomic.LoadUint32(&CommitIndex) {
	//					node.Leader = i
	//				}
	//			}
	//		}(node.Clients[i])
	//	}
	//}()

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
						CommitIndex = current

						var requestId uuid.UUID
						value, exists := node.RequestIds.LoadAndDelete(current)
						if !exists {
							fmt.Printf("CANT FIND ID FOR: %d\n", current)
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
						if index == uint32(node.Leader) {
							println("setting new leader!")
							node.Leader = 1
						}
						node.Failed[index] = true
					}
					packetSize := binary.LittleEndian.Uint32(sizeBuffer[:4])
					if packetSize > uint32(len(buffer)) {
						buffer = append(buffer, make([]byte, packetSize-uint32(len(buffer)))...)
					}

					err = reader.Read(buffer[:packetSize])
					if err != nil {
						if index == uint32(node.Leader) {
							println("setting new leader!")
							node.Leader = 1
						}
						node.Failed[index] = true
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
						node.Keys.Store(string(entry.key), 0)

						value, exists := node.LogWaiter.LoadAndDelete(proposal.Slot)
						if exists {
							close(value.(chan struct{}))
						}

						go func() {
							response := make([]byte, 9)
							binary.LittleEndian.PutUint32(response[:4], 5)
							response[4] = OpAck
							binary.LittleEndian.PutUint32(response[5:], proposal.Slot)
							client := node.Clients[index]
							client.mutex.Lock()
							err = client.Write(response)
							client.mutex.Unlock()
							if err != nil {
								if index == uint32(node.Leader) {
									fmt.Printf("LEADER IS DOWN")
								} else {
									node.Failed[index] = true
									fmt.Printf("Follower went down")
								}
								//panic(err)
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
						go func() {
							value, exists := node.Entries.Load(slot)

							if exists {
								entry := value.(*Entry)
								if atomic.AddUint32(&entry.acked, 1) == entry.majority {
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
														node.WriteReadPacket(node.Clients[senderIndex], bytes, nextEntry.requestId)
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
											if i == node.Index || node.Failed[i] {
												continue
											}
											client := node.Clients[i]
											client.mutex.Lock()
											err := client.Write(commitBuffer)
											client.mutex.Unlock()
											if err != nil {
												if i == node.Leader {
													println("setting new leader!")
													node.Leader = 1
												}

												node.Failed[i] = true
											}
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
						copy(requestId[:], buffer[1:17])
						if packetSize == 17 {
							readChannel <- ReadResult{requestId: requestId, value: make([]byte, 0)}
						} else {
							value := make([]byte, packetSize-17)
							copy(value, buffer[17:packetSize])
							readChannel <- ReadResult{requestId: requestId, value: value}
						}
					} else if op == OpElection {
						//current := atomic.LoadUint32(&CommitIndex)
						//commitIndex := binary.LittleEndian.Uint32(buffer[1:])
						//electionBuffer := make([]byte, 9)
						//binary.LittleEndian.PutUint32(electionBuffer[:4], 5)
						//electionBuffer[4] = OpElection
						//binary.LittleEndian.PutUint32(electionBuffer[5:9], current)
						//
						//// 5
						//// 4
						//// 5
						//// 3
						//
						//for i := 0; i < node.Total; i++ {
						//	if i == node.Index || node.Failed[i] {
						//		continue
						//	}
						//	client := node.Clients[i]
						//	client.mutex.Lock()
						//	err := client.Write(commitBuffer)
						//	client.mutex.Unlock()
						//	if err != nil {
						//		node.Failed[i] = true
						//		if i == node.Leader {
						//			node.Leader = 1
						//		}
						//	}
						//}
					} else if op == OpVote {

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
		node.WriteProposePacket(node.Clients[node.Leader], packet, OpForward)
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

	appliedIndex := atomic.AddUint32(&AppliedIndex, 1)
	entry := &Entry{
		key:       key,
		value:     make([]byte, 0),
		acked:     1,
		majority:  uint32(node.Quorum),
		requestId: requestId,
	}

	packet := ProposePacket{
		Slot:      appliedIndex,
		RequestId: requestId,
		Key:       key,
		Value:     make([]byte, 0),
		Type:      ReadType,
		Sender:    sender,
	}

	node.Entries.Store(appliedIndex, entry)
	for i := 0; i < node.Total; i++ {
		if i == node.Index || node.Failed[i] {
			continue
		}
		i := i
		go func(client Client) {
			node.WriteProposePacket(client, packet, OpPropose)
		}(node.Clients[i])
	}

	if wait {
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
		node.WriteProposePacket(node.Clients[node.Leader], packet, OpForward)
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
		if i == node.Index || node.Failed[i] {
			continue
		}
		i := i
		go func(client Client) {
			node.WriteProposePacket(client, ProposePacket{
				Key:       key,
				Value:     segments[client.index],
				Slot:      appliedIndex,
				RequestId: requestId,
				Type:      WriteType,
				Sender:    uint8(node.Index),
			}, OpPropose)
		}(node.Clients[i])
	}

	if wait {
		<-channel
	}
}
