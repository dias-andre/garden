package lib

import "sync"

type Fifo[T any] struct {
	mu           sync.Mutex
	items        []T
	minShrinkCap int
}

func NewFifo[T any]() *Fifo[T] {
	var newFifo Fifo[T]
	newFifo.minShrinkCap = 100
	return &newFifo
}

func (f *Fifo[T]) WithMinShrinkCap(s int) *Fifo[T] {
	f.minShrinkCap = s
	return f
}

func (f *Fifo[T]) Push(item T) {
	f.mu.Lock()
	f.items = append(f.items, item)
	f.mu.Unlock()
}

func (f *Fifo[T]) Pop() (T, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var zero T
	if len(f.items) == 0 {
		return zero, false
	}

	item := f.items[0]
	f.items[0] = zero
	f.items = f.items[1:]
	if cap(f.items) > f.minShrinkCap && len(f.items) <= cap(f.items)/4 {
		newItems := make([]T, len(f.items), len(f.items)*2)
		copy(newItems, f.items)
		f.items = newItems
	}
	return item, true
}

func (f *Fifo[T]) Size() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}
