package utils

import "sync"

type BQueue[T comparable] struct {
	mu       sync.Mutex
	c        sync.Cond
	data     []T
	capacity int
}

func NewBlockingQueue[T comparable](capacity int) *BQueue[T] {
	q := new(BQueue[T])
	q.c = sync.Cond{L: &q.mu}
	q.capacity = capacity
	return q
}

func (q *BQueue[T]) Put(item T) {
	q.c.L.Lock()
	defer q.c.L.Unlock()

	for q.isFull() {
		q.c.Wait() // blocks until queue is not full (the current lock holder has to wait)
	}
	q.data = append(q.data, item)
	q.c.Signal()
}

func (q *BQueue[T]) Take() T {
	q.c.L.Lock()
	defer q.c.L.Unlock()

	for q.isEmpty() {
		q.c.Wait()
	}

	item := q.data[0]
	q.data = q.data[1:len(q.data)]
	q.c.Signal()
	return item
}

func (q *BQueue[T]) isFull() bool {
	return len(q.data) == q.capacity
}

func (q *BQueue[T]) isEmpty() bool {
	return len(q.data) == 0
}
