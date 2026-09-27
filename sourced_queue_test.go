package garden_test

import (
	"context"
	"testing"

	"github.com/dias-andre/garden"
)

func mockCommitPending(_ context.Context, _ int) error { return nil }
func mockCommitSuccess(_ context.Context, _ int) error { return nil }

func TestSourcedQueue_PushPop(t *testing.T) {
	sq, err := garden.NewSourcedQueue[int](func(i int) error {
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
