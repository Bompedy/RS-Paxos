package paxos

// Storage Move to disk
type Storage struct {
	Data map[uint64]byte
}
