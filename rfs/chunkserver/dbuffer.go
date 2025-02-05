package chunkserver

import (
	"fmt"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/caleberi/distributed-system/rfs/utils"
	"github.com/rs/zerolog/log"
)

type bufferedItem struct {
	data   []byte
	expire time.Time
}

type dbuffer struct {
	mu     sync.RWMutex
	done   chan bool
	buffer map[common.BufferId]*bufferedItem
	expire time.Duration
	tick   time.Duration
}

// NewDBuffer create a download buffer and returns it and
// runs a routine for cleaning up expired buffer items
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
				action := func(key common.BufferId, item *bufferedItem) {
					if !item.expire.Before(time.Now()) {
						return
					}
					dbuf.mu.Lock()
					delete(dbuf.buffer, key)
					dbuf.mu.Unlock()
				}
				utils.IterateOverMap(dbuf.buffer, action)
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
