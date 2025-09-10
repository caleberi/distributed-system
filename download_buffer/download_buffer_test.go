package downloadbuffer

import (
	"sync"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadBuffer(t *testing.T) {
	const (
		tick   = 100 * time.Millisecond
		expire = 500 * time.Millisecond
	)

	t.Run("NewDownloadBuffer", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err, "NewDownloadBuffer should not return an error")
		require.NotNil(t, buffer, "Buffer should not be nil")
		defer buffer.Done()

		_, err = NewDownloadBuffer(0, expire)
		assert.Error(t, err, "NewDownloadBuffer with zero tick should return an error")
		_, err = NewDownloadBuffer(tick, 0)
		assert.Error(t, err, "NewDownloadBuffer with zero expire should return an error")
	})

	t.Run("SetAndGet", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)
		defer buffer.Done()

		id := NewDownloadBufferId(common.ChunkHandle(1))
		data := []byte("test data")
		buffer.Set(id, data)

		retrieved, ok := buffer.Get(id)
		assert.True(t, ok, "Get should find the item")
		assert.Equal(t, data, retrieved, "Retrieved data should match set data")
		assert.Equal(t, 1, buffer.Len(), "Buffer length should be 1")

		nonExistentId := NewDownloadBufferId(common.ChunkHandle(2))
		_, ok = buffer.Get(nonExistentId)
		assert.False(t, ok, "Get should return false for non-existent ID")
	})

	t.Run("Delete", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)
		defer buffer.Done()

		id := NewDownloadBufferId(common.ChunkHandle(1))
		data := []byte("test data")
		buffer.Set(id, data)

		buffer.Delete(id)
		_, ok := buffer.Get(id)
		assert.False(t, ok, "Get should return false after Delete")
		assert.Equal(t, 0, buffer.Len(), "Buffer length should be 0 after Delete")
	})

	t.Run("FetchAndDelete", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)
		defer buffer.Done()

		id := NewDownloadBufferId(common.ChunkHandle(1))
		data := []byte("test data")
		buffer.Set(id, data)

		retrieved, err := buffer.FetchAndDelete(id)
		assert.NoError(t, err, "FetchAndDelete should not return an error")
		assert.Equal(t, data, retrieved, "Fetched data should match set data")
		assert.Equal(t, 0, buffer.Len(), "Buffer length should be 0 after FetchAndDelete")

		nonExistentId := NewDownloadBufferId(common.ChunkHandle(2))
		_, err = buffer.FetchAndDelete(nonExistentId)
		assert.Error(t, err, "FetchAndDelete should return an error for non-existent ID")
	})

	t.Run("Expiration", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)
		defer buffer.Done()

		for i := range 10 {
			id := NewDownloadBufferId(common.ChunkHandle(i))
			buffer.Set(id, []byte("test data"))
		}
		assert.Equal(t, 10, buffer.Len(), "Buffer should contain 10 items initially")

		time.Sleep(expire + 2*tick)
		assert.Equal(t, 0, buffer.Len(), "Buffer should be empty after expiration")
	})

	t.Run("Concurrency", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)
		defer buffer.Done()

		var wg sync.WaitGroup
		numGoroutines := 100
		wg.Add(numGoroutines)

		for i := range numGoroutines {
			go func(i int) {
				defer wg.Done()
				id := NewDownloadBufferId(common.ChunkHandle(i))
				data := []byte("test data")
				buffer.Set(id, data)
				retrieved, ok := buffer.Get(id)
				if ok {
					assert.Equal(t, data, retrieved, "Retrieved data should match set data")
				}
			}(i)
		}
		wg.Wait()
		assert.LessOrEqual(t, buffer.Len(), numGoroutines, "Buffer length should not exceed number of goroutines")
	})

	t.Run("Done", func(t *testing.T) {
		buffer, err := NewDownloadBuffer(tick, expire)
		require.NoError(t, err)

		id := NewDownloadBufferId(common.ChunkHandle(1))
		buffer.Set(id, []byte("test data"))
		assert.Equal(t, 1, buffer.Len(), "Buffer should contain 1 item")

		buffer.Done()
		time.Sleep(2 * tick)
		buffer.Set(id, []byte("new data"))
		assert.Equal(t, 1, buffer.Len(), "Buffer should still function after Done")
	})
}
