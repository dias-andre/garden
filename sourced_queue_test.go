package garden_test

import (
	"testing"

	"github.com/dias-andre/garden"
)

func TestSourcedQueue_PushPop(t *testing.T) {
	sq := garden.NewSourcedQueue(func(i int) error {
		return nil
	}, 2)

	_ = sq.Push(1)
	_ = sq.Push(2)
	_ = sq.Push(3)

	got, ok := sq.Pop(t.Context())
	if !ok || got != 1 {
		t.Fatalf("expected (1, true), got (%d, %v)", got, ok)
	}
}
