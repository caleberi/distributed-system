package chunkserver

import (
	"log"
	"testing"
	"time"

	"github.com/caleberi/distributed-system/rfs/common"
	"github.com/stretchr/testify/assert"
	"github.com/tjarratt/babble"
)

func TestDownloadBuffer(t *testing.T) {

	var (
		tick            = 1 * time.Second
		expire          = 5 * time.Second
		buffer *dbuffer = NewDBuffer(tick, expire)
	)

	// The download buffer get data for a particular amount of time
	// some of the data are expected to expire after a particular amount of time
	// how to verify that  data has expired after some particular ticks
	ids := []common.BufferId{}

	for i := 0; i < 100; i++ {
		id := NewDBufferId(common.ChunkHandle(i))
		ids = append(ids, id)
		babbler := babble.NewBabbler()
		babbler.Separator = " "
		word := babbler.Babble()
		log.Printf(" writing -> [%v] to buffer", word)
		buffer.Set(id, []byte(word))
		if i%2 == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	// we have populated the items in the array .
	//  Now we need to create a stat of expired ones to unexpired ones

	expired := 0
	for _, id := range ids {
		if _, ok := buffer.Get(id); !ok {
			expired++
		}
	}
	buffer.Done() // exit go rountine and  stop ticker

	assert.Less(t, expired, len(ids)-expired) // hopefully the expired ones are lesser than the unexpired ones

}
