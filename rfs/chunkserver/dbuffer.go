package chunkserver

import (
	"fmt"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/rs/zerolog/log"
)

// TODO: optimize bufferedItem by packing bit together
// using encoding/binary
type bufferedItem struct {
	data   []byte    // actual binary representation of data using a buffer(byte)
	expire time.Time // the time when this data item expires
}

type dbuffer struct {
	mu     sync.RWMutex
	done   chan bool                         // need this to prevent memory leaks
	buffer map[common.BufferId]*bufferedItem // cache all buffered item
	expire time.Duration
	tick   time.Duration // track this buffer expiry time for clean up
}

// NewDBuffer this create a download buffer and returns it
// but it creates a routine for cleaning up expired buffer items
func NewDBuffer(tick, expire time.Duration) *dbuffer {
	dbuf := &dbuffer{
		done:   make(chan bool, 1),
		buffer: make(map[common.BufferId]*bufferedItem),
		expire: expire,
		tick:   tick,
	}

	go func() {
		ticker := time.NewTicker(dbuf.tick)
		for {
			select {
			case <-ticker.C:
				for key, item := range dbuf.buffer {
					if item.expire.Before(time.Now()) {
						dbuf.mu.Lock()
						delete(dbuf.buffer, key)
						dbuf.mu.Unlock()
					}
				}

			case <-dbuf.done:
				log.Info().Msg("Closing download buffer")
				ticker.Stop()
				close(dbuf.done)
				return
			}
		}
	}()

	return dbuf
}

func NewDBufferId(id common.ChunkHandle) common.BufferId {
	tstamp := time.Now().Nanosecond() + time.Now().Minute()*60*1000 + time.Now().Second()*60 //  random nanosec + minute + second time allocation
	return common.BufferId{Handle: id, Timestamp: tstamp}
}

func (dbuf *dbuffer) Delete(bufId common.BufferId) {
	dbuf.mu.Lock()
	defer dbuf.mu.Unlock()
	delete(dbuf.buffer, bufId)
}

func (dbuf *dbuffer) Set(bufId common.BufferId, data []byte) {
	dbuf.mu.Lock()
	defer dbuf.mu.Unlock()
	dbuf.buffer[bufId] = &bufferedItem{
		data:   data,
		expire: time.Now().Add(dbuf.expire),
	}
}

func (dbuf *dbuffer) Get(bufId common.BufferId) ([]byte, bool) {
	dbuf.mu.Lock()
	defer dbuf.mu.Unlock()

	item, ok := dbuf.buffer[bufId]
	if item != nil {
		item.expire = time.Now().Add(dbuf.expire)
		return item.data, ok
	}
	return nil, ok
}

func (dbuf *dbuffer) FetchAndDelete(bufId common.BufferId) ([]byte, error) {
	dbuf.mu.Lock()
	defer dbuf.mu.Unlock()

	item, ok := dbuf.buffer[bufId]
	if !ok {
		return nil, fmt.Errorf("cannot find id [%v] in buffer", bufId)
	}

	delete(dbuf.buffer, bufId)
	return item.data, nil
}

func (dbuffer *dbuffer) Done() {
	dbuffer.done <- true
}
