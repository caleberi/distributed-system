package common

import (
	"log"
	"sync"
	"time"
)

type Lease struct {
	Handle      ChunkHandle // each lease acquired should be associated to a particular chunkhandle
	Expire      time.Time
	InUse       bool
	Primary     ServerAddr   // the current chunk server (as primary)
	Secondaries []ServerAddr // to notice all secondary  chunk server
}

type LeaseHolder struct {
	queue          chan *Lease
	current        *Lease
	mu             sync.Mutex
	active         bool
	expirationTime time.Time
	done           chan bool
	wg             *sync.WaitGroup
	capacity       int
}

func NewLeaseHolder(capacity int) *LeaseHolder {
	return &LeaseHolder{
		capacity: capacity,
		queue:    make(chan *Lease, capacity),
		done:     make(chan bool),
		wg:       &sync.WaitGroup{},
	}
}

func (lh *LeaseHolder) HoldsLease() bool {
	lh.mu.Lock()
	defer lh.mu.Unlock()
	return len(lh.queue) != 0 && lh.active
}

func (lh *LeaseHolder) Wait() {
	lh.wg.Wait()
}

func (lh *LeaseHolder) Enqueue(ls *Lease) bool {
	lh.mu.Lock()
	defer lh.mu.Unlock()
	if len(lh.queue) < lh.capacity {
		lh.queue <- ls
		return true
	}
	return false
}

func (lh *LeaseHolder) Acquire() (*Lease, bool) {
	lh.mu.Lock()
	defer lh.mu.Unlock()
	if !lh.active {
		ls := <-lh.queue
		if ls == nil {
			lh.active = false
			return ls, false
		}
		lh.current = ls
		if !ls.InUse {
			ls.InUse = !ls.InUse
		}
		lh.active = ls.InUse
		lh.expirationTime = ls.Expire
		lh.wg.Add(1)
		go lh.monitorExpiration()
		return ls, true
	}
	return nil, false
}

func (lh *LeaseHolder) Release() (*Lease, bool) {
	lh.mu.Lock()
	defer lh.mu.Unlock()
	if len(lh.queue) == 0 && lh.current == nil {
		return nil, false
	}
	ls := lh.current
	ls.InUse, lh.active = false, false
	lh.done <- true
	return ls, true
}

func (lh *LeaseHolder) ReleaseAll() []*Lease {
	ret := make([]*Lease, 0)
	for len(lh.queue) > 0 {
		if !lh.active {
			l := <-lh.queue
			ret = append(ret, l)
		} else {
			current, ok := lh.Release()
			if ok {
				ret = append(ret, current)
			}
		}
	}
	return ret
}

func (lh *LeaseHolder) monitorExpiration() {
	defer lh.wg.Done()
	for {
		// handle extension
		select {
		case <-lh.done:
			lh.mu.Lock()
			log.Printf("releasing %v\n", lh.current)
			lh.active = false
			lh.current = nil
			lh.mu.Unlock()
			return
		case <-time.After(time.Duration(lh.expirationTime.Second())):
			lh.mu.Lock()
			log.Printf("expired %v\n", lh.current)
			if lh.active {
				lh.active = false
				lh.current = nil
				lh.mu.Unlock()
				return // Lease expired
			}
			lh.mu.Unlock()
		}
	}
}
