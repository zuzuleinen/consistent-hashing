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

	hosts, err := ch.Get("customer-id-1")

	if len(hosts) > 0 {
		t.Errorf("hosts should be empty in case of error. got %d", len(hosts))
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

	matchedHosts, err := ch.Get("customer-id-1")

	if len(matchedHosts) != 1 {
		t.Errorf("count of macthed hosts should be %d. got %d", 1, len(matchedHosts))
	}
	if err != nil {
		t.Errorf("err should be nil. got: %v", err)
	}

	matchedHost := matchedHosts[0]
	if matchedHost != "host-2" {
		t.Errorf("matched host expected %s. got %s", "host-2", matchedHost)
	}
}

func TestNewWithDefaults(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing()

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")

	matchedHosts, err := ch.Get("customer-id-1")

	if err != nil {
		t.Errorf("err should be nil. got: %v", err)
	}

	matchedHost := matchedHosts[0]
	if matchedHost != "host-2" {
		t.Errorf("matched host expected %s. got %s", "host-2", matchedHost)
	}
}

func TestWithVirtualNodes(t *testing.T) {
	tests := []struct {
		name               string
		virtualNodesCount  int
		totalPrimaryHosts  int
		expectedHostsCount int
	}{
		{
			name:               "without virtual nodes",
			virtualNodesCount:  0,
			totalPrimaryHosts:  3,
			expectedHostsCount: 3,
		},
		{
			name:               "with virtual nodes",
			virtualNodesCount:  2,
			totalPrimaryHosts:  3,
			expectedHostsCount: 3 + (3 * 2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ch := consistenthashing.NewConsistentHashing(
				consistenthashing.WithVirtualNodes(tc.virtualNodesCount),
			)

			t.Logf("when adding %d hosts", tc.totalPrimaryHosts)
			for i := range tc.totalPrimaryHosts {
				ch.Add(fmt.Sprintf("host-%d", i))
			}

			if ch.HostsCount() != tc.expectedHostsCount {
				t.Errorf("hosts count(including replicas) should be %d. got %d", tc.expectedHostsCount, ch.HostsCount())
			}
		})
	}
}

func TestWithReplication(t *testing.T) {
	tests := []struct {
		name                 string
		replicationFactor    int
		totalPrimaryHosts    int
		expectedMatchedHosts int
	}{
		{
			name:                 "with replication factor = primary hosts count",
			replicationFactor:    3,
			totalPrimaryHosts:    3,
			expectedMatchedHosts: 3,
		},
		{
			name:                 "with replication factor > primary hosts count",
			replicationFactor:    100,
			totalPrimaryHosts:    3,
			expectedMatchedHosts: 3,
		},
		{
			name:                 "with replication factor < primary hosts count",
			replicationFactor:    3,
			totalPrimaryHosts:    7,
			expectedMatchedHosts: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ch := consistenthashing.NewConsistentHashing(consistenthashing.WithReplicationFactor(tc.replicationFactor))

			for i := range tc.totalPrimaryHosts {
				ch.Add(fmt.Sprintf("host-%d", i))
			}

			matchedHosts, err := ch.Get("some-key")
			if err != nil {
				t.Errorf("err should be nil. got: %v", err)
			}

			expectedMatchedHosts := tc.expectedMatchedHosts
			if len(matchedHosts) != expectedMatchedHosts {
				t.Errorf("matched hosts count should be %d. got %d", expectedMatchedHosts, len(matchedHosts))
			}
		})
	}
}

func TestRemoveHost(t *testing.T) {
	ch := consistenthashing.NewConsistentHashing()
	initialHostsCount := 3

	for i := range initialHostsCount {
		ch.Add(fmt.Sprintf("host-%d", i))
	}
	if ch.HostsCount() != initialHostsCount {
		t.Errorf("host count should be %d. got %d", initialHostsCount, ch.HostsCount())
	}

	ch.Remove("host-0")
	if ch.HostsCount() != initialHostsCount-1 {
		t.Errorf("host count should be %d. got %d", initialHostsCount-1, ch.HostsCount())
	}
}
