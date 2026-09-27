package garden_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dias-andre/garden"
)

func mockCommitPending(_ context.Context, _ int) error { return nil }
func mockCommitSuccess(_ context.Context, _ int) error { return nil }

func TestSourcedQueue_PushPop(t *testing.T) {
	sq, err := garden.NewSourcedQueue(func(i int) error {
		return nil
	}, 2, garden.SourcedHooks[int]{
		CommitPending: mockCommitPending,
		CommitSuccess: mockCommitSuccess,
	})

	if err != nil {
		t.Fatalf("failed to create SourcedQueue, err: %v", err)
	}

	_ = sq.Push(1)
	_ = sq.Push(2)
	_ = sq.Push(3)

	got, ok := sq.Pop(t.Context())
	if !ok || got != 1 {
		t.Fatalf("expected (1, true), got (%d, %v)", got, ok)
	}
}

func TestSourcedQueue_Processing(t *testing.T) {
	numWorkers := 8
	numItems := 10_000
	var processed atomic.Int64
	sq, err := garden.NewSourcedQueue(func(i int) error {
		processed.Add(1)
		return nil
	}, numWorkers, garden.SourcedHooks[int]{
		CommitPending: mockCommitPending,
		CommitSuccess: mockCommitSuccess,
	})

	if err != nil {
		t.Fatalf("failed to create SourcedQueue, err: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	sq.Serve(ctx)
	for i := range numItems {
		_ = sq.Push(i)
	}

	if err := sq.DrainAndShutdown(ctx); err != nil {
		t.Errorf("shutdown err: %v", err)
	}

	if processed.Load() != 10_000 {
		t.Fatalf("only %d of %d item were processed", processed.Load(), numItems)
	}
}
