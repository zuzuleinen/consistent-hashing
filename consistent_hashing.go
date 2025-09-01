package consistent_hashing

import (
	"errors"
	"hash/fnv"
	"slices"
	"sync"
)

var (
	ErrNoHostsAvailable = errors.New("there are no hosts available")
)

type ConsistentHashing struct {
	mu           sync.RWMutex
	hashToHost   map[uint32]string
	sortedHashes []uint32
}

func NewConsistentHashing() *ConsistentHashing {
	return &ConsistentHashing{
		hashToHost:   make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
	}
}

// Add a host to the ring
func (ch *ConsistentHashing) Add(host string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	hash := ch.Hash(host)

	ch.hashToHost[hash] = host

	idx, found := slices.BinarySearch(ch.sortedHashes, hash)
	if !found {
		ch.sortedHashes = slices.Insert(ch.sortedHashes, idx, hash)
	}
}

// Get returns the host for a key using consistent hashing.
// Returns the first host clockwise from the key's position on the ring.
// If there are no hosts on the ring, it returns ErrNoHostsAvailable error
func (ch *ConsistentHashing) Get(key string) (string, error) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.sortedHashes) == 0 {
		return "", ErrNoHostsAvailable
	}

	hash := ch.Hash(key)

	// BinarySearch returns (idx, found). If not found, idx is the insertion point.
	idx, _ := slices.BinarySearch(ch.sortedHashes, hash)
	if idx == len(ch.sortedHashes) {
		idx = 0 // wrap around
	}

	host := ch.hashToHost[ch.sortedHashes[idx]]

	return host, nil
}

// Hash returns an uint32 hash based for a key
func (ch *ConsistentHashing) Hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
