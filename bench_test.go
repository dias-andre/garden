package garden_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dias-andre/garden"
)

func BenchmarkQueueMemory(b *testing.B) {
	var processed atomic.Uint64

	queue := garden.NewQueue[int](func(n int) error {
		processed.Add(1)
		return nil
	}, 10)

	ctx := context.Background()
	queue.Serve(ctx)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := queue.Push(i); err != nil {
			b.Fatalf("failed to push item %d, err: %v", i, err)
		}
	}

	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := queue.DrainAndShutdown(drainCtx); err != nil {
		b.Fatalf("failed to wait queue shutdown: %v", err)
	}
}

func BenchmarkQueueParallel(b *testing.B) {
	var processed atomic.Uint64

	queue := garden.NewQueue[int](func(n int) error {
		processed.Add(1)
		return nil
	}, 10)

	ctx := context.Background()
	queue.Serve(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = queue.Push(1)
		}
	})

	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := queue.DrainAndShutdown(drainCtx); err != nil {
		b.Fatalf("failed to wait queue shutdown: %v", err)
	}
}
