package consistent_hashing

import (
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"sync"
)

var (
	ErrNoHostsAvailable = errors.New("there are no hosts available")
)

// HashFunc hashes a key to a value on the ring
type HashFunc func(key string) uint32

// Option configures a ConsistentHashing instance
type Option func(c *ConsistentHashing)

// WithHashFunc allows you to pass a custom hash function
func WithHashFunc(f HashFunc) Option {
	return func(c *ConsistentHashing) {
		c.hash = f
	}
}

// WithReplicationFactor allows you to pass a replication factor
// When R > 1 a key will be saved in R distinct nodes instead of just one
func WithReplicationFactor(r int) Option {
	return func(c *ConsistentHashing) {
		c.replicationFactor = r
	}
}

// WithVirtualNodes allows you to specify count of virtual nodes to be added
// Virtual nodes distribute data more evenly across the hash ring by giving each physical host
// multiple positions, which reduces hotspots and improves load balancing
func WithVirtualNodes(n int) Option {
	return func(c *ConsistentHashing) {
		c.virtualNodesCount = n
	}
}

type ConsistentHashing struct {
	mu                sync.RWMutex
	hashToHost        map[uint32]string
	sortedHashes      []uint32
	hash              HashFunc
	replicationFactor int
	virtualNodesCount int
	// primaryNodes is a unique list of main hosts names
	primaryNodes map[string]bool
}

// NewConsistentHashing creates a new *ConsistentHashing
//
// If no custom hash function is set via WithHashFunc, 32-bit FNV-1a is used by default.
func NewConsistentHashing(opts ...Option) *ConsistentHashing {
	c := &ConsistentHashing{
		hashToHost:   make(map[uint32]string),
		sortedHashes: make([]uint32, 0),
		primaryNodes: make(map[string]bool),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.hash == nil {
		c.hash = func(key string) uint32 {
			h := fnv.New32a()
			h.Write([]byte(key))
			return h.Sum32()
		}
	}

	return c
}

// Add a host to the ring
func (ch *ConsistentHashing) Add(host string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	hash := ch.hash(host)
	ch.addNodeToRing(hash, host)
	ch.primaryNodes[host] = true

	// add virtual nodes
	for i := range ch.virtualNodesCount {
		virtualNodeHash := ch.hash(fmt.Sprintf("%s:%d", host, i))
		ch.addNodeToRing(virtualNodeHash, host)
	}
}

// Remove removes a host from the ring including its virtual nodes
func (ch *ConsistentHashing) Remove(host string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	hash := ch.hash(host)
	ch.removeNodeFromRing(hash)

	delete(ch.primaryNodes, host)

	for i := range ch.virtualNodesCount {
		virtualNodeHash := ch.hash(fmt.Sprintf("%s:%d", host, i))
		ch.removeNodeFromRing(virtualNodeHash)
	}
}

func (ch *ConsistentHashing) addNodeToRing(hash uint32, host string) {
	ch.hashToHost[hash] = host

	idx, found := slices.BinarySearch(ch.sortedHashes, hash)
	if !found {
		ch.sortedHashes = slices.Insert(ch.sortedHashes, idx, hash)
	}
}

func (ch *ConsistentHashing) removeNodeFromRing(hash uint32) {
	delete(ch.hashToHost, hash)

	ch.sortedHashes = slices.DeleteFunc(ch.sortedHashes, func(u uint32) bool {
		return u == hash
	})
}

// HostsCount returns the number hosts on the ring
func (ch *ConsistentHashing) HostsCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return len(ch.sortedHashes)
}

// Get returns the host for a key using consistent hashing.
// Returns the first host clockwise from the key's position on the ring.
// If there are no hosts on the ring, it returns ErrNoHostsAvailable error
func (ch *ConsistentHashing) Get(key string) ([]string, error) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.sortedHashes) == 0 {
		return nil, ErrNoHostsAvailable
	}

	hash := ch.hash(key)

	// BinarySearch returns (idx, found). If not found, idx is the insertion point.
	idx, _ := slices.BinarySearch(ch.sortedHashes, hash)
	if idx == len(ch.sortedHashes) {
		idx = 0 // wrap around
	}

	host := ch.hashToHost[ch.sortedHashes[idx]]

	addedHosts := make(map[string]bool)

	matchedHosts := []string{host}
	addedHosts[host] = true

	if ch.replicationFactor > 1 {
		replicationFactor := min(ch.replicationFactor, len(ch.primaryNodes)) // cap replication to len of distinct nodes

		for len(addedHosts) != replicationFactor {
			idx = (idx + 1) % len(ch.sortedHashes)

			hostToAdd := ch.hashToHost[ch.sortedHashes[idx]]

			if _, ok := addedHosts[hostToAdd]; !ok {
				matchedHosts = append(matchedHosts, hostToAdd)
				addedHosts[hostToAdd] = true
			}
		}
	}

	return matchedHosts, nil
}
