package consistent_hashing_test

import (
	"errors"
	"fmt"
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
	ch := consistenthashing.NewConsistentHashing(
		consistenthashing.WithHashFunc(Hash),
	)
	if ch == nil {
		t.Errorf("ch should not be nil")
	}
}

func TestAdd(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(
		consistenthashing.WithHashFunc(Hash),
	)

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")
}

func TestGetNoHostsAvailable(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(
		consistenthashing.WithHashFunc(Hash),
	)

	host, err := ch.Get("customer-id-1")

	if host != "" {
		t.Errorf("host should be empty in case of error. got %s", host)
	}
	if !errors.Is(err, consistenthashing.ErrNoHostsAvailable) {
		t.Errorf("error should be: %v", consistenthashing.ErrNoHostsAvailable)
	}
}

func TestGet(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing(
		consistenthashing.WithHashFunc(Hash),
	)

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

func TestNewWithDefaults(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing()

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

func TestWithReplicationFactor(t *testing.T) {
	tests := []struct {
		desc               string
		replicationFactor  int
		totalPrimaryHosts  int
		expectedHostsCount int
	}{
		{
			desc:               "by default, hosts count should match number of added hosts",
			replicationFactor:  0,
			totalPrimaryHosts:  3,
			expectedHostsCount: 3,
		},
		{
			desc:               "when replication factor > 0, each primary node gets R - 1 replicas",
			replicationFactor:  2,
			totalPrimaryHosts:  3,
			expectedHostsCount: 2 * 3,
		},
	}

	for _, tc := range tests {
		t.Log(tc.desc)

		ch := consistenthashing.NewConsistentHashing(
			consistenthashing.WithReplicationFactor(tc.replicationFactor),
		)
		for i := range tc.totalPrimaryHosts {
			ch.Add(fmt.Sprintf("host-%d", i))
		}

		if ch.HostsCount() != tc.expectedHostsCount {
			t.Errorf("hosts counts should be %d. got %d", tc.expectedHostsCount, ch.HostsCount())
		}
	}
}
