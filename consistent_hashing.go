package consistent_hashing

import (
	"errors"
	"hash/fnv"
	"slices"
)

var (
	ErrNoHostsAvailable = errors.New("there are no hosts available")
)

type ConsistentHashing struct {
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
	hash := ch.Hash(host)

	ch.hashToHost[hash] = host

	// keep hashes sorted for quick find
	ch.sortedHashes = append(ch.sortedHashes, hash)
	slices.Sort(ch.sortedHashes)
}

// Get returns the correct host to handle a key
// If no hosts are available, a ErrNoHostsAvailable is returned
func (ch *ConsistentHashing) Get(key string) (string, error) {
	if len(ch.sortedHashes) == 0 {
		return "", ErrNoHostsAvailable
	}

	hash := ch.Hash(key)

	idx, found := slices.BinarySearch(ch.sortedHashes, hash)
	if !found {
		idx = 0
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
