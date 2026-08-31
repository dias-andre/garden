package garden_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dias-andre/garden"
)

func TestQueue_PushPop(t *testing.T) {
	q := garden.NewQueue[int](func(item int) error { return nil }, 1)
	_ = q.Push(1)
	_ = q.Push(2)
	_ = q.Push(3)
	got, ok := q.Pop()
	if !ok || got != 1 {
		t.Fatalf("expected (1, true), got (%d, %v)", got, ok)
	}
}

func TestQueue_NoDuplicateProcessing(t *testing.T) {
	const numItems = 10_000
	const numWorkers = 8

	var mu sync.Mutex
	seen := make(map[int]int)
	q := garden.NewQueue[int](func(item int) error {
		mu.Lock()
		seen[item]++
		mu.Unlock()
		return nil
	}, numWorkers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Serve(ctx)

	for i := 0; i < numItems; i++ {
		_ = q.Push(i)
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		count := len(seen)
		mu.Unlock()
		if count == numItems {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timeout: only %d of %d items were processed", count, numItems)
		case <-time.After(10 * time.Millisecond):

		}
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < numItems; i++ {
		if seen[i] != 1 {
			t.Errorf("item %d was processed %d times (expected 1)", i, seen[i])
		}
	}
}
