package utils

import (
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestQueueInFIFOSequence(t *testing.T) {
	type testcase struct {
		name  string
		input []interface{}
		want  []interface{}
	}
	testcases := []testcase{
		{
			name: "integer queue",
			input: []interface{}{
				1, 3, 4, 5, 45, 234, 45, 34, 3,
			},
			want: []interface{}{
				1, 3, 4, 5, 45, 234, 45, 34, 3,
			},
		},
		{
			name: "string queue",
			input: []interface{}{
				'1', "3", "4", "5", "45", "234", "45", "34", "3",
			},
			want: []interface{}{
				'1', "3", "4", "5", "45", "234", "45", "34", "3",
			},
		},
		{
			name: "boolean queue",
			input: []interface{}{
				true, false, true, false, false, true,
			},
			want: []interface{}{
				true, false, true, false, false, true,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			queue := Deque[interface{}]{}
			ret := []interface{}{}
			for _, v := range tc.input {
				queue.PushBack(v)
			}
			for queue.Length() != 0 {
				ret = append(ret, queue.PopFront())
			}

			if !reflect.DeepEqual(ret, tc.input) {
				t.Fatalf("expected %v , got %v", ret, tc.input)
			}
		})
	}

}

func TestQueueWithProducerAndConsumer(t *testing.T) {
	wg := sync.WaitGroup{}
	queue := Deque[int]{}
	want := []int{6, 5, 1, 4, 3}
	got := []int{}
	done := make(chan bool, 1)

	wg.Add(1)
	// create a producer
	go func(queue *Deque[int]) {
		defer wg.Done()
		queue.PushFront(1)
		queue.PushBack(2)
		queue.PushBack(3)
		queue.PushBack(4)
		queue.PushFront(5)
		queue.PushBack(6)
		done <- true
	}(&queue)

	wg.Add(1)
	// create a consumer
	go func(queue *Deque[int]) {
		defer wg.Done()
		<-done
		got = append(got, queue.PopBack())
		got = append(got, queue.PopFront())
		got = append(got, queue.PopFront())
		got = append(got, queue.PopBack())
		got = append(got, queue.PopBack())
		close(done)
	}(&queue)

	wg.Wait()

	queue.Print(os.Stdout)

	if len(want) != len(got) {
		t.Fatalf("expected length of %d got %d", len(want), len(got))
	}

	if queue.Length() != 1 {
		t.Fatalf("expected length of queue to be %d got %d", 1, queue.Length())
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected %v , got %v", want, got)
	}

}
