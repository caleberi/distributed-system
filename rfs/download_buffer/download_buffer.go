// Package downloadbuffer provides a thread-safe buffer implementation for temporarily
// storing and managing data with expiration times in a distributed system. It is
// designed to handle data identified by a unique common.BufferId, supporting
// concurrent access through a read-write mutex and automatic cleanup of expired
// items via a background goroutine.
//
// The primary type, DownloadBuffer, implements the Buffer interface, which defines
// methods for setting, getting, deleting, and fetching data with automatic expiration.
// The buffer is particularly useful in scenarios where data needs to be temporarily
// cached, such as in distributed file systems or data transfer protocols, with
// configurable expiration and cleanup intervals.
//
// Example usage:
//
//	// Create a new DownloadBuffer with a 1-second tick and 10-second expiration.
//	buffer, err := NewDownloadBuffer(time.Second, 10*time.Second)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer buffer.Done()
//
//	// Store data in the buffer.
//	id := NewDownloadBufferId(common.ChunkHandle("chunk1"))
//	buffer.Set(id, []byte("example data"))
//
//	// Retrieve data.
//	data, ok := buffer.Get(id)
//	if ok {
//	    fmt.Printf("Retrieved: %s\n", data)
//	}
package downloadbuffer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
)

// Buffer defines the interface for a thread-safe buffer that stores and manages
// data with expiration times in a distributed system.
type Buffer interface {
	// Set stores data in the buffer with the specified BufferId.
	// The data is associated with an expiration time based on the buffer's configuration.
	Set(bufId common.BufferId, data []byte)
	// Get retrieves data for the given BufferId and updates its expiration time if found.
	// Returns the data and a boolean indicating whether the data was found.
	Get(bufId common.BufferId) ([]byte, bool)
	// Delete removes the data associated with the given BufferId from the buffer.
	Delete(bufId common.BufferId)
	// FetchAndDelete retrieves and removes the data associated with the given BufferId.
	// Returns the data and an error if the BufferId is not found.
	FetchAndDelete(bufId common.BufferId) ([]byte, error)
	// Done signals the buffer to stop its background cleanup routine.
	Done()
}

// BufferedItem represents an item stored in the buffer, containing the data
// and its expiration time.
type BufferedItem struct {
	data   []byte    // The stored data.
	expire time.Time // The time at which the item expires.
}

// DownloadBuffer is a thread-safe implementation of the Buffer interface.
// It stores data in a map with BufferId keys and runs a background goroutine
// to clean up expired items based on a configurable tick duration.
type DownloadBuffer struct {
	buffer       map[common.BufferId]*BufferedItem // The map storing buffer items.
	expire, tick time.Duration                     // Durations for expiration and cleanup interval.
	mu           sync.RWMutex                      // Read-write mutex for thread-safe access.
	done         chan bool                         // Channel to signal cleanup routine termination.
}

// NewDownloadBuffer creates a new DownloadBuffer with the specified tick and expire durations.
// The tick duration determines how often the buffer checks for expired items,
// and the expire duration sets how long items remain in the buffer before expiration.
// It starts a background goroutine to clean up expired items.
// Returns an error if tick or expire durations are not positive.
func NewDownloadBuffer(tick, expire time.Duration) (*DownloadBuffer, error) {
	if tick <= 0 || expire <= 0 {
		return nil, errors.New("tick and expire durations must be positive")
	}
	downloadBuffer := &DownloadBuffer{
		buffer: make(map[common.BufferId]*BufferedItem),
		done:   make(chan bool, 1),
		expire: expire,
		tick:   tick,
	}

	// clearBuffer removes expired items from the buffer.
	clearBuffer := func(downloadBuffer *DownloadBuffer) {
		var expiredKeys []common.BufferId
		downloadBuffer.mu.RLock()
		for id, item := range downloadBuffer.buffer {
			if item.expire.Before(time.Now()) {
				expiredKeys = append(expiredKeys, id)
			}
		}
		downloadBuffer.mu.RUnlock()

		if len(expiredKeys) > 0 {
			downloadBuffer.mu.Lock()
			for _, key := range expiredKeys {
				delete(downloadBuffer.buffer, key)
			}
			downloadBuffer.mu.Unlock()
		}
	}
	// Start a background goroutine to periodically clean up expired items.
	go func(downloadBuffer *DownloadBuffer) {
		ticker := time.NewTicker(downloadBuffer.tick)
		for {
			select {
			case <-ticker.C:
				clearBuffer(downloadBuffer)
			case <-downloadBuffer.done:
				close(downloadBuffer.done)
				ticker.Stop()
				return
			}
		}
	}(downloadBuffer)

	return downloadBuffer, nil
}

// NewDownloadBufferId creates a new BufferId using the provided ChunkHandle
// and the current timestamp.
func NewDownloadBufferId(id common.ChunkHandle) common.BufferId {
	return common.BufferId{
		Handle:    id,
		Timestamp: time.Now().UnixNano(),
	}
}

// Len returns the number of items currently stored in the buffer.
// It is thread-safe and uses a read lock to access the buffer.
func (downloadBuffer *DownloadBuffer) Len() int {
	downloadBuffer.mu.RLock()
	defer downloadBuffer.mu.RUnlock()
	return len(downloadBuffer.buffer)
}

// Delete removes the data associated with the specified BufferId from the buffer.
// It is thread-safe and uses a write lock to modify the buffer.
func (downloadBuffer *DownloadBuffer) Delete(bufId common.BufferId) {
	downloadBuffer.mu.Lock()
	defer downloadBuffer.mu.Unlock()
	delete(downloadBuffer.buffer, bufId)
}

// Set stores the provided data in the buffer with the specified BufferId.
// The data is associated with an expiration time based on the buffer's expire duration.
// It is thread-safe and uses a write lock to modify the buffer.
func (downloadBuffer *DownloadBuffer) Set(bufId common.BufferId, data []byte) {
	downloadBuffer.mu.Lock()
	defer downloadBuffer.mu.Unlock()
	downloadBuffer.buffer[bufId] = &BufferedItem{
		data:   data,
		expire: time.Now().Add(downloadBuffer.expire),
	}
}

// Get retrieves the data associated with the specified BufferId and updates its expiration time.
// Returns the data and a boolean indicating whether the data was found.
// It is thread-safe and uses a write lock to update the expiration time.
func (downloadBuffer *DownloadBuffer) Get(bufId common.BufferId) ([]byte, bool) {
	downloadBuffer.mu.Lock()
	defer downloadBuffer.mu.Unlock()

	item, ok := downloadBuffer.buffer[bufId]
	if !ok {
		return nil, ok
	}

	item.expire = time.Now().Add(downloadBuffer.expire)
	return item.data, ok
}

// FetchAndDelete retrieves and removes the data associated with the specified BufferId.
// Returns the data and an error if the BufferId is not found.
// It is thread-safe and uses a write lock to modify the buffer.
func (downloadBuffer *DownloadBuffer) FetchAndDelete(bufId common.BufferId) ([]byte, error) {
	downloadBuffer.mu.Lock()
	defer downloadBuffer.mu.Unlock()

	item, ok := downloadBuffer.buffer[bufId]
	if !ok {
		return nil, fmt.Errorf("cannot find id [%v] in buffer", bufId)
	}

	delete(downloadBuffer.buffer, bufId)
	return item.data, nil
}

// Done signals the buffer to stop its background cleanup routine.
// It sends a signal to the done channel, causing the cleanup goroutine to exit gracefully.
func (downloadBuffer *DownloadBuffer) Done() {
	select {
	case downloadBuffer.done <- true:
	default:
	}
}
