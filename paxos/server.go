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
var OpSegment = uint8(5)
var OpSegmentResponse = uint8(6)
var OpReceivedFailSlot = uint8(7)

//var OpElection = uint8(5)
//var OpElectionResponse = uint8(6)

var ReadType = uint8(0)
var WriteType = uint8(1)

var ReconstructionWaiter = make(chan struct{})

var CommitIndex uint32
var AppliedIndex uint32

var KeyCount uint32
var ReconstructCount uint32

//var Segments [][]byte
//var SegmentResponses uint32
//
//var Encoder reedsolomon.Encoder

// var Failures = true
var Encoding = true
var FailSlotAcks = uint32(0)

type Node struct {
	FailSlot           uint32
	Failures           bool
	Clients            []Client
	Failed             []bool
	Keys               sync.Map
	ReadRequestWaiter  sync.Map
	ReadSenders        sync.Map
	WriteRequestWaiter sync.Map
	RequestIds         sync.Map
	LogWaiter          sync.Map
	Total              uint32
	Encoder            reedsolomon.Encoder
	Entries            sync.Map
	Quorum             int
	Parity             int
	Segments           int
	Index              uint32
	Leader             uint32
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

func (node *Node) BroadcastWrite(buffer []byte) {
	for i := uint32(0); i < node.Total; i++ {
		if i == node.Index || node.Failed[i] {
			continue
		}
		go func(client Client) {
			err := client.Write(buffer)
			if err != nil {
				panic("SHUTDOWN")
			}
		}(node.Clients[i])
	}
}

func (node *Node) Broadcast(block func(uint32, Client)) {
	for i := uint32(0); i < node.Total; i++ {
		if i == node.Index || node.Failed[i] {
			continue
		}
		block(i, node.Clients[i])
	}
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
		panic("SHUTDOWN")
	}
}

//func (node *Node) RepairAndReconstruct() {
//	current := atomic.LoadUint32(&CommitIndex)
//	if current != FailSlot {
//		panic("it was different than the fialed slot!!!")
//	}
//
//	//if current > HighestIndex {
//	//	HighestIndex = current
//	//}
//	//if current < LowestIndex {
//	//	LowestIndex = current
//	//}
//	//
//	//if atomic.LoadUint32(&CommitIndex) != current {
//	//	panic("SOMEHOW CHANGED?")
//	//}
//	//
//	//for _, client := range node.Clients {
//	//	if ElectionResponses[clientIndex] == -1 {
//	//		continue
//	//	}
//	//	for slot := uint32(ElectionResponses[clientIndex] + 1); slot < HighestIndex; slot++ {
//	//		buf := make([]byte, 9)
//	//		binary.LittleEndian.PutUint32(buf[:4], uint32(5))
//	//		buf[4] = OpSegment
//	//		binary.LittleEndian.PutUint32(buf[5:], slot)
//	//		client.mutex.Lock()
//	//		err = client.Write(buf)
//	//		client.mutex.Unlock()
//	//		if err != nil {
//	//			node.Fail(client)
//	//		}
//	//	}
//	//}
//
//}

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
		panic("SHUTDOWN")
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
	return nil
}

type CommitResult struct {
	Type  uint8
	Index uint32
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

		commitChannel := make(chan uint32, 4096)
		readChannel := make(chan ReadResult, 4096)
		var segmentMap = make([]sync.Map, node.Total)
		var reconstructed = sync.Map{}
		for i := uint32(0); i < node.Total; i++ {
			segmentMap[i] = sync.Map{}
		}

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

					if node.Failures && CommitIndex == node.FailSlot {
						fmt.Println("Reached fail slot")
						buf := make([]byte, 5)
						binary.LittleEndian.PutUint32(buf[:4], 1)
						buf[4] = OpReceivedFailSlot
						err := node.Clients[node.Leader].Write(buf)
						if err != nil {
							panic("shouldnt error on write")
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
			//
			//commitChannel := make(chan CommitResult, 4096)
			//readChannel := make(chan ReadResult, 4096)

			go func() {
				sizeBuffer := make([]byte, 4)
				buffer := make([]byte, 65535)
				for {
					err := reader.Read(sizeBuffer)
					if err != nil {
						panic("SHUTDOWN")
					}
					packetSize := binary.LittleEndian.Uint32(sizeBuffer[:4])
					if packetSize > uint32(len(buffer)) {
						buffer = append(buffer, make([]byte, packetSize-uint32(len(buffer)))...)
					}

					err = reader.Read(buffer[:packetSize])
					if err != nil {
						panic("SHUTDOWN")
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
						node.Keys.Store(string(entry.key), entry.value)

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
								panic("SHUTDOWN")
							}
						}()
					} else if op == OpForward {
						if node.Index != atomic.LoadUint32(&node.Leader) {
							panic("NON LEADER GOT FORWARD")
						}
						forward := GetProposePacket(buffer[1:])
						go func() {
							if forward.Type == ReadType {
								node.Read(forward.Key, false, forward.RequestId, forward.Sender)
							} else {
								node.Write(forward.Key, forward.Value, false, forward.RequestId)
							}
						}()
					} else if op == OpAck {
						if node.Index != atomic.LoadUint32(&node.Leader) {
							panic("NON LEADER GOT ACK")
						}

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
										node.BroadcastWrite(commitBuffer)
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
					} else if op == OpSegment {
						fmt.Println("Got op segment")
						keyCount := uint32(0)
						node.Keys.Range(func(keyValue, value interface{}) bool {
							keyString := keyValue.(string)
							key := []byte(keyString)
							shard := value.([]byte)
							//segment := etcdRead(key)
							segment := shard
							buf := make([]byte, 13+len(segment))
							binary.LittleEndian.PutUint32(buf[:4], uint32(9+len(segment)))
							buf[4] = OpSegmentResponse
							binary.LittleEndian.PutUint32(buf[5:], uint32(len(key)))
							copy(buf[9:], key)
							copy(buf[9+len(key):], segment)
							err := node.Clients[node.Leader].Write(buf)
							if err != nil {
								panic("ERROR SENDING SEGMENT BACK TO LEADER")
							}
							keyCount++
							return true
						})
						atomic.StoreUint32(&KeyCount, keyCount)
					} else if op == OpSegmentResponse {
						fmt.Println("Got op segment response")
						length := binary.LittleEndian.Uint32(buffer[1:5])
						key := buffer[5 : 5+length]
						keyString := string(key)
						segment := buffer[5+length:]
						segmentMap[index].Store(keyString, segment)
						count := 0
						for i := uint32(1); i < node.Total; i++ {
							_, ok := segmentMap[i].Load(keyString)
							if ok {
								count++
							}
						}
						if count < node.Segments {
							continue
						}
						_, loaded := reconstructed.LoadOrStore(keyString, 0)
						if loaded {
							continue
						}
						segmentSize := len(segment)
						fullValueValue, _ := node.Keys.Load(keyString)
						fullValue := fullValueValue.([]byte)
						fullSize := len(fullValue)
						segments := make([][]byte, node.Segments+node.Parity)
						for i := uint32(1); i < node.Total; i++ {
							value, ok := segmentMap[i].Load(keyString)
							if ok {
								segments[i] = value.([]byte)
							}
						}
						err := node.Encoder.Reconstruct(segments)
						if err != nil {
							panic("Couldnt reconstruct")
						}
						value := make([]byte, fullSize)
						startIndex := 0
						for i := range segments[:node.Segments] {
							endIndex := startIndex + segmentSize
							if endIndex > len(value) {
								endIndex = len(value)
							}
							copy(value[startIndex:endIndex], segments[i])
							startIndex = endIndex
						}
						if string(fullValue) != string(value) {
							fmt.Printf("Full Value = %s\n", fullValue)
							fmt.Printf("Value = %s\n", value)
							panic("Reconstructed wrong value")
						}
						etcdWrite(key, value)
						completed := atomic.AddUint32(&ReconstructCount, 1)
						if completed == KeyCount {
							close(ReconstructionWaiter)
						}
						if completed > KeyCount {
							panic("WHY DID WE HAVE MORE THAN KEYCOUNT")
						}
					} else if op == OpReceivedFailSlot {
						if !node.Failures {
							panic("WHY DID WE GET A FAIL SLOT")
						}
						if !Encoding {
							panic("WE ONLY SUPPORT FAILURES WITH ENCODING")
						}
						fmt.Println("Received fail slot")
						if atomic.AddUint32(&FailSlotAcks, 1) == (node.Total - 1) {
							buf := make([]byte, 9)
							binary.LittleEndian.PutUint32(buf[:4], 5)
							buf[4] = OpSegment
							node.BroadcastWrite(buf)
						}
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
	leader := atomic.LoadUint32(&node.Leader)
	if node.Index != leader {
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

		node.WriteProposePacket(node.Clients[leader], packet, OpForward)
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

	appliedIndex := atomic.AddUint32(&AppliedIndex, 1)
	if node.Failures && appliedIndex > node.FailSlot && node.Index == node.Leader {
		<-ReconstructionWaiter
	}

	channel := make(chan []byte)
	node.ReadRequestWaiter.Store(requestId, channel)
	node.ReadSenders.Store(requestId, sender)

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

	node.Broadcast(func(i uint32, client Client) {
		go func(client Client) {
			node.WriteProposePacket(client, packet, OpPropose)
		}(node.Clients[i])
	})

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
	leader := atomic.LoadUint32(&node.Leader)
	if node.Index != leader {
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

		node.WriteProposePacket(node.Clients[leader], packet, OpForward)
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

	appliedIndex := atomic.AddUint32(&AppliedIndex, 1)

	if node.Failures && appliedIndex > node.FailSlot && node.Index == node.Leader {
		<-ReconstructionWaiter
	}

	entry := &Entry{
		key:       key,
		value:     value,
		acked:     1,
		majority:  uint32(node.Quorum),
		requestId: requestId,
		Type:      WriteType,
	}
	node.Keys.Store(string(key), value)
	node.Entries.Store(appliedIndex, entry)
	channel := make(chan struct{})
	node.WriteRequestWaiter.Store(requestId, channel)

	if Encoding {
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

		segments[0] = nil
		segments[1] = nil

		err = node.Encoder.Reconstruct(segments)
		if err != nil {
			panic(err)
		}

		restore := make([]byte, segmentSize*(node.Segments+node.Parity))
		startIndex = 0

		for i := range segments[:node.Segments] {
			endIndex := startIndex + segmentSize
			if endIndex > len(restore) {
				endIndex = len(restore)
			}
			copy(restore[startIndex:endIndex], segments[i])
			startIndex = endIndex
		}

		if string(restore) != string(value) {

			panic(fmt.Errorf("they were different!\n%d=%s\n%d=%s", len(restore), string(restore), len(value), string(value)))
		}

		node.Broadcast(func(i uint32, client Client) {
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
		})
	} else {
		node.Broadcast(func(i uint32, client Client) {
			go func(client Client) {
				node.WriteProposePacket(client, ProposePacket{
					Key:       key,
					Value:     value,
					Slot:      appliedIndex,
					RequestId: requestId,
					Type:      WriteType,
					Sender:    uint8(node.Index),
				}, OpPropose)
			}(node.Clients[i])
		})
	}

	if wait {
		<-channel
	}
}
