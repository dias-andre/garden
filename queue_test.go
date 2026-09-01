package garden_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

func TestQueue_GracefulShutdown(t *testing.T) {
	var processed int64

	q := garden.NewQueue(func(item int) error {
		atomic.AddInt64(&processed, 1)
		time.Sleep(5 * time.Millisecond)
		return nil
	}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	q.Serve(ctx)

	for i := 0; i < 50; i++ {
		_ = q.Push(i)
	}
	time.Sleep(20 * time.Millisecond) // let some items process
	cancel()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	if err := q.Shutdown(shutdownCtx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Shutdown() did not return - possible deadlock or goroutine leak")
		}
		t.Fatalf("Failed to run shutdown: %v", err)
	}

	t.Logf("items processed before shutdown: %d", atomic.LoadInt64(&processed))
}
