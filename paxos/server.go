package paxos

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/klauspost/reedsolomon"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"math"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

var OpWrite = uint8(0)
var OpCommit = uint8(1)

type Node struct {
	Clients []Client
	Storage Storage
	Lock    sync.Mutex
	Total   int
	Encoder reedsolomon.Encoder
}

func (node *Node) Connect(nodes []string) error {
	defer node.Lock.Unlock()
	node.Lock.Lock()

	var waiter sync.WaitGroup
	for _, address := range nodes {
		waiter.Add(1)
		address := address
		go func() {
			defer waiter.Done()
			client, err := net.Dial("tcp", address)
			if err != nil {
				panic(fmt.Sprintf("Can't connect to address: %s", address))
			}
			index, err := strconv.Atoi(string(address[len(address)-3]))
			if err != nil {
				panic(fmt.Sprintf("Can't parse address to index: %s", address))
			}
			node.Clients = append(node.Clients, Client{
				connection: client,
				index:      uint8(index),
				//buffer: make([]byte, 65535),
			})
		}()
	}

	waiter.Wait()
	sort.Slice(node.Clients, func(i, j int) bool {
		return node.Clients[i].index < node.Clients[j].index
	})
	return nil
}

func (node *Node) Accept(etcd *etcdserver.EtcdServer, address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	for {
		client, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		connection := Client{
			connection: client,
			index:      0,
		}

		buffer := make([]byte, 65535)
		go func() {
			err := connection.Read(buffer[:1])
			if err != nil {
				panic(err)
			}

			op := buffer[0]
			if op == OpWrite {
				err := connection.Read(buffer[:8])
				if err != nil {
					panic(err)
				}
				keySize := binary.LittleEndian.Uint32(buffer[:4])
				valueSize := binary.LittleEndian.Uint32(buffer[4:8])
				err = connection.Read(buffer[:(keySize + valueSize)])
				if err != nil {
					panic(err)
				}
				trace := traceutil.Get(context.Background())
				var write = etcd.KV().Write(trace)
				write.Put(buffer[:keySize], buffer[keySize:(keySize+valueSize)], 0)
				write.End()
				err = connection.Write(buffer[:1])
				if err != nil {
					panic(err)
				}
			} else if op == OpCommit {
				err = connection.Write(buffer[:1])
				if err != nil {
					panic(err)
				}
			}
		}()
	}
}

func (node *Node) Write(etcd *etcdserver.EtcdServer, key []byte, value []byte) error {
	trace := traceutil.Get(context.Background())
	var write = etcd.KV().Write(trace)
	write.Put(key, value, 0)
	write.End()

	const numSegments = 3
	const parity = 2
	var segmentSize = int(math.Ceil(float64(len(value)) / float64(numSegments)))
	var segments = reedsolomon.AllocAligned(numSegments+parity, segmentSize)
	var startIndex = 0
	for i := range segments[:numSegments] {
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

	return node.quorum(func(index int, client Client) error {
		// Add 1 since DS1 is the leaders segment
		shard := segments[index+1]
		buffer := make([]byte, 9+len(key)+len(shard))
		buffer[0] = OpWrite
		binary.LittleEndian.PutUint32(buffer[1:5], uint32(len(key)))
		binary.LittleEndian.PutUint32(buffer[5:9], uint32(len(shard)))
		keyIndex := 9 + len(key)
		copy(buffer[9:keyIndex], key)
		copy(buffer[keyIndex:keyIndex+len(shard)], shard)
		err := client.Write(buffer)
		if err != nil {
			panic(err)
		}
		return client.Read(buffer[:1])
	})
}

func (node *Node) quorum(
	block func(index int, client Client) error,
) error {
	var waiter sync.WaitGroup
	waiter.Add(node.Total - 1)
	var count = uint32(0)

	for i := range node.Clients {
		go func(index int, client Client) {
			err := block(index, client)
			if err != nil {
				panic(err)
			}
			if atomic.AddUint32(&count, 1) <= uint32(node.Total-1) {
				waiter.Done()
			}
		}(i, node.Clients[i])
	}

	waiter.Wait()
	return nil
}

func (node *Node) Read(etcd *etcdserver.EtcdServer, key []byte) ([]byte, error) {
	var options = mvcc.RangeOptions{}
	trace := traceutil.Get(context.Background())
	var read = etcd.KV().Read(mvcc.ConcurrentReadTxMode, trace)
	defer read.End()
	result, err := read.Range(context.Background(), key, nil, options)
	if err != nil {
		panic(err)
	}
	return result.KVs[0].Value, nil
}
