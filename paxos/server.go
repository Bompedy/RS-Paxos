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

var CommitLock sync.Mutex
var CommitIndex uint32
var AppliedIndex uint32

type Node struct {
	Clients       []Client
	RequestLock   *sync.Mutex
	RequestWaiter map[uuid.UUID]chan struct{}
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
	requestId uuid.UUID
}

type ProposePacket struct {
	Slot      uint32
	Key       []byte
	Value     []byte
	RequestId uuid.UUID
}

type CommitPacket struct {
	RequestIds []uuid.UUID
	Next       uint32
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

func (client Client) WriteCommitPacket(packet CommitPacket) {
	size := 9 + (16 * len(packet.RequestIds))
	buffer := make([]byte, size+4)
	binary.LittleEndian.PutUint32(buffer[:4], uint32(size))
	buffer[4] = OpCommit
	binary.LittleEndian.PutUint32(buffer[5:9], packet.Next)
	binary.LittleEndian.PutUint32(buffer[9:13], uint32(len(packet.RequestIds)))
	for i, requestId := range packet.RequestIds {
		copy(buffer[13+(i*16):], requestId[:])
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
	requestIds := make([]uuid.UUID, totalRequestIds)
	for i := uint32(0); i < totalRequestIds; i++ {

		copy(requestIds[i][:], buffer[8+(i*16):8+(i*16)+16])
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

			commitChannel := make(chan CommitPacket)
			//commitChannel <- commitPacket
			go func() {
				for commit := range commitChannel {
					for {
						current := CommitIndex + 1
						if current > commit.Next {
							break
						}

						fmt.Printf("Is someone stuck?: %d\n", current)

						var entry Entry
						// wait for it to get in our log i guess :3
						for {
							//fmt.Printf("grabbing lock?: %d\n", current)
							node.Log.Lock.Lock()
							mapEntry, exists := node.Log.Entries[current]
							if exists {
								entry = *mapEntry
								delete(node.Log.Entries, current)
								node.Log.Lock.Unlock()
								break
							} else {
								fmt.Printf("we are so stuck on %d\n", current)
							}

							node.Log.Lock.Unlock()
							//fmt.Printf("releasing lock?: %d\n", current)
						}
						//
						CommitIndex = current

						//etcdWrite(entry.key, entry.value)
						if entry.condition != nil {
							close(entry.condition)
						}

						//CommitIndex = current

						// leader - 6-10          uuids[0] uuids[1] uuids[2] uuids[3]
						// follower - 6

						// (7 - (10 - 4)) - 1

						// current=4
						//                  0           1           2
						// next=7  requests[5] requests[6] requests[7]
						// requestIds=3

						// next - current = 4
						// (4) - 3 - 1 = 0

						//3 - (7 - 4) = 0
						//3 - (7 - 5) = 1
						//3 - (7 - 6) = 2

						//1 - (7-6) - 1 = -1
						// 1 - (7-7) - 1 = 0

						// requests current=1 next=1 totalIds=1 requestIndex=1
						// requests current=1 next=3 totalIds=3 requestIndex=3

						// (3 - (3 - 1)) =
						//len(3) -> 0, 1, 2

						requestIndex := int32(len(commit.RequestIds)) - (int32(commit.Next) - int32(current)) - 1
						fmt.Printf("requests current=%d next=%d totalIds=%d requestIndex=%d\n!.", current, commit.Next, len(commit.RequestIds), requestIndex)

						//requestIndex := (int32(current) - (int32(commit.Next) - int32(len(commit.RequestIds)))) - 1
						if requestIndex >= 0 {
							fmt.Printf("TAKE REQUEST LOCK %d\n!", current)
							node.RequestLock.Lock()
							channel, exists := node.RequestWaiter[commit.RequestIds[requestIndex]]
							fmt.Printf("Request Lock size before: %d\n", len(node.RequestWaiter))
							if exists {
								if channel != nil {
									fmt.Printf("Released channel: current=%d id=%s\n", current, commit.RequestIds[requestIndex].String())
									close(channel)
								}
								delete(node.RequestWaiter, commit.RequestIds[requestIndex])
							} else {
								println("Didn't find request!")
							}
							fmt.Printf("Request Lock size: %d\n", len(node.RequestWaiter))
							node.RequestLock.Unlock()
							fmt.Printf("RELEASED REQUEST LOCK %d\n!.", current)
						} else {
							fmt.Printf("No requests current=%d next=%d totalIds=%d requestIndex=%d\n!.", current, commit.Next, len(commit.RequestIds), requestIndex)
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
						fmt.Printf("Got proposal for %d\n", proposal.Slot)
						entry := &Entry{
							key:       proposal.Key,
							value:     proposal.Value,
							acked:     1,
							majority:  uint32(node.Quorum),
							condition: make(chan struct{}),
							requestId: proposal.RequestId,
						}
						fmt.Printf("Aquiring lock for %d\n", proposal.Slot)
						node.Log.Lock.Lock()
						fmt.Printf("Got lock for %d\n", proposal.Slot)
						node.Log.Entries[proposal.Slot] = entry
						node.Log.Lock.Unlock()

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
						go func() {
							CommitLock.Lock()
							node.Log.Lock.Lock()
							entry, exists := node.Log.Entries[slot]
							node.Log.Lock.Unlock()

							if exists && atomic.AddUint32(&entry.acked, 1) == entry.majority {
								var requestsIds []uuid.UUID
								start := CommitIndex
								for {
									next := CommitIndex + 1

									node.Log.Lock.Lock()
									nextEntry, nextEntryExists := node.Log.Entries[next]
									node.Log.Lock.Unlock()

									if !nextEntryExists {
										break
									}

									if atomic.LoadUint32(&nextEntry.acked) >= nextEntry.majority {
										CommitIndex = next
										requestsIds = append(requestsIds, nextEntry.requestId)
										//etcdWrite(nextEntry.key, nextEntry.value)
										if nextEntry.condition != nil {
											close(nextEntry.condition)
										}
										node.Log.Lock.Lock()
										delete(node.Log.Entries, next)
										node.Log.Lock.Unlock()
									} else {
										break
									}
								}

								if start != CommitIndex {
									packet := CommitPacket{
										RequestIds: requestsIds,
										Next:       CommitIndex,
									}

									fmt.Printf("Commiting up to: %d\n", packet.Next)

									for i := 0; i < node.Total; i++ {
										if i == node.Index {
											continue
										}
										node.Clients[i].WriteCommitPacket(packet)
									}
								}
							}
							CommitLock.Unlock()
						}()
					} else if op == OpCommit {
						commitPacket := GetCommitPacket(buffer[1:])
						fmt.Printf("Commiting up to: %d\n", commitPacket.Next)
						commitChannel <- commitPacket
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
	requestId := uuid.New()
	if node.Index != node.Leader {
		packet := ProposePacket{
			Slot:      0,
			RequestId: requestId,
			Key:       key,
			Value:     value,
		}

		channel := make(chan struct{})
		node.RequestLock.Lock()
		_, exists := node.RequestWaiter[requestId]
		if exists {
			println("IT ALREADY EXISTS IN THERE AND WE ARE OVERWRITING IT")
		}
		node.RequestWaiter[requestId] = channel
		node.RequestLock.Unlock()
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
	fmt.Printf("Got forward: %d\n", appliedIndex)
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
		i := i
		go func(index int, client Client) {
			client.WriteProposePacket(ProposePacket{
				Key:       key,
				Value:     segments[i],
				Slot:      appliedIndex,
				RequestId: requestId,
			}, OpPropose)
		}(i, node.Clients[i])
	}

	if wait {
		<-entry.condition
	}
}
