package consistent_hashing_test

import (
	"errors"
	"hash/fnv"
	"testing"

	consistenthashing "consistent-hashing"
)

func Hash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func TestNewConsistentHashing(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(Hash)
	if ch == nil {
		t.Errorf("ch should not be nil")
	}
}

func TestAdd(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(Hash)

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")
}

func TestGetNoHostsAvailable(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(Hash)

	host, err := ch.Get("customer-id-1")

	if host != "" {
		t.Errorf("host should be empty in case of error. got %s", host)
	}
	if !errors.Is(err, consistenthashing.ErrNoHostsAvailable) {
		t.Errorf("error should be: %v", consistenthashing.ErrNoHostsAvailable)
	}
}

func TestGet(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(Hash)

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")

	matchedHost, err := ch.Get("customer-id-1")

	if err != nil {
		t.Errorf("err should be nil. got: %v", err)
	}
	if matchedHost != "host-2" {
		t.Errorf("matched host expected %s. got %s", "host-2", matchedHost)
	}
}
