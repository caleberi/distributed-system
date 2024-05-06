package utils

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBlockingQueue(t *testing.T) {
	bq := NewBlockingQueue[string](1)
	done := make(chan bool)

	items := []string{"Abacus", "Banner", "Life"}
	results := []string{}
	go func(items []string) {
		for _, item := range items {
			bq.Put(item)
			time.Sleep(100 * time.Millisecond)
		}
	}(items)

	go func(d int) {
		for i := 0; i < d; i++ {
			item := bq.Take()
			time.Sleep(100 * time.Millisecond)
			results = append(results, item)
		}
		done <- true
	}(len(items))

	<-done

	if !reflect.DeepEqual(items, results) {
		t.Errorf("expected result to be [%s] got [%s]", strings.Join(results, ","), strings.Join(items, ","))
	}
}
