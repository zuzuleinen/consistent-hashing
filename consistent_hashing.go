package consistent_hashing

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrNoHostsAvailable = errors.New("there are no hosts available")
)

// HashFunc hashes a key to a value on the ring
type HashFunc func(key string) uint32

type ConsistentHashing struct {
	mu           sync.RWMutex
	hashToHost   map[uint32]string
	sortedHashes []uint32
	hash         HashFunc
}

func NewConsistentHashing(hash HashFunc) *ConsistentHashing {
	return &ConsistentHashing{
		hashToHost:   make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		hash:         hash,
	}
}

// Add a host to the ring
func (ch *ConsistentHashing) Add(host string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	hash := ch.hash(host)

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

	hash := ch.hash(key)

	// BinarySearch returns (idx, found). If not found, idx is the insertion point.
	idx, _ := slices.BinarySearch(ch.sortedHashes, hash)
	if idx == len(ch.sortedHashes) {
		idx = 0 // wrap around
	}

	host := ch.hashToHost[ch.sortedHashes[idx]]

	return host, nil
}
