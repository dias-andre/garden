// Package garden: the root library code
package garden

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrQueueClosed   = errors.New("queue closed")
	ErrQueueDraining = errors.New("queue draining")
)

type WorkerFn[T any] func(item T) error

type Queue[T any] struct {
	mu              sync.Mutex
	cond            *sync.Cond
	wg              sync.WaitGroup
	dispatchedItems chan T

	closed   bool
	draining bool

	items       []T
	workerFn    WorkerFn[T]
	workerCount int
}

func NewQueue[T any](fn WorkerFn[T], count int) *Queue[T] {
	q := &Queue[T]{}
	q.cond = sync.NewCond(&q.mu)
	q.workerFn = fn
	q.workerCount = count
	return q
}

func (q *Queue[T]) Push(item T) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.Join(errors.New("cannot push"), ErrQueueClosed)
	}
	if q.draining {
		q.mu.Unlock()
		return ErrQueueDraining
	}
	q.items = append(q.items, item)
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}

func (q *Queue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	for len(q.items) == 0 && q.closed {
		var zero T
		return zero, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *Queue[T]) Serve(ctx context.Context) {
	q.dispatchedItems = make(chan T, 100)
	for i := 0; i < q.workerCount; i++ {
		q.wg.Add(1)
		go func(id int, items <-chan T) {
			defer q.wg.Done()
			for job := range items {
				// TODO: manage error
				_ = q.workerFn(job)
			}
		}(i, q.dispatchedItems)
	}

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		defer close(q.dispatchedItems)
		for {
			item, ok := q.Pop()
			if !ok {
				return
			}
			select {
			case q.dispatchedItems <- item:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		q.Close()
	}()
}

func (q *Queue[T]) Shutdown(ctx context.Context) error {
	q.Close()
	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *Queue[T]) DrainAndShutdown(ctx context.Context) error {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}

	q.draining = true
	q.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		q.wg.Wait()
		done <- nil
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		q.Close()
		return ctx.Err()
	}
}
