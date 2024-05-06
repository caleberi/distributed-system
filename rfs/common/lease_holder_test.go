package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLeaseHolder(t *testing.T) {
	// Create a LeaseHolder with capacity 1
	lh := NewLeaseHolder(1)

	// Define Lease with expiration time of 1 second
	ls := &Lease{
		Expire:      time.Now().Add(2 * time.Second),
		InUse:       false,
		Primary:     "primary",
		Secondaries: []ServerAddr{"secondary1"},
	}

	// Enqueue a Lease
	assert.True(t, lh.Enqueue(ls), "Failed to enqueue a lease")

	// Acquire the Lease
	acquiredLease, ok := lh.Acquire()
	assert.True(t, ok, "Failed to acquire a lease")
	assert.NotNil(t, acquiredLease, "Acquired lease is nil")
	assert.True(t, acquiredLease.InUse, "Acquired lease is not marked as in use")

	// Wait for the Lease to expire
	time.Sleep(time.Second * 2)

	// Assert that Lease has expired
	assert.False(t, lh.HoldsLease(), "Lease holder should not hold lease after expiration")

	// Release the Lease
	releasedLease, ok := lh.Release()
	assert.False(t, ok, "Failed to release a lease since it should have expired")
	assert.Nil(t, releasedLease, "Released lease is nil")
	lh.Wait()
}
