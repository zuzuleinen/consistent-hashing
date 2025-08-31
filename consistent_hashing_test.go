package consistent_hashing

import (
	"errors"
	"testing"
)

func TestNewConsistentHashing(t *testing.T) {
	ch := NewConsistentHashing()
	if ch == nil {
		t.Errorf("ch should not be nil")
	}
}

func TestAdd(t *testing.T) {
	ch := NewConsistentHashing()

	ch.Add("host-1")
	ch.Add("host-2")
	ch.Add("host-3")
}

func TestGetNoHostsAvailable(t *testing.T) {
	ch := NewConsistentHashing()

	host, err := ch.Get("customer-id-1")

	if host != "" {
		t.Errorf("host should be empty in case of error. got %s", host)
	}
	if !errors.Is(err, ErrNoHostsAvailable) {
		t.Errorf("error should be: %v", ErrNoHostsAvailable)
	}
}

func TestGet(t *testing.T) {
	ch := NewConsistentHashing()

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
