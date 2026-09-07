// Package garden: the root library code
package garden

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueClosed   = errors.New("queue closed")
	ErrQueueDraining = errors.New("queue draining")
)

type WorkerFn[T any] func(item T) error

type queueJob[T any] struct {
	item      T
	attempt   int
	lastErr   error
	lastRetry time.Time
}

type Queue[T any] struct {
	mu              sync.Mutex
	cond            *sync.Cond
	wg              sync.WaitGroup
	dispatchedItems chan queueJob[T]

	closed   bool
	draining bool

	jobs        []queueJob[T]
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
	newJob := queueJob[T]{
		item:    item,
		attempt: 1,
		lastErr: nil,
	}
	q.jobs = append(q.jobs, newJob)
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}

func (q *Queue[T]) pop() (queueJob[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.jobs) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.jobs) == 0 {
		var zero queueJob[T]
		return zero, false
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job, true
}

func (q *Queue[T]) PopItem() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.jobs) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.jobs) == 0 {
		var zero T
		return zero, false
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	return job.item, true
}

func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *Queue[T]) startWorkers() {
	for i := 0; i < q.workerCount; i++ {
		q.wg.Add(1)
		go func(id int, items <-chan queueJob[T]) {
			defer q.wg.Done()
			for job := range items {
				// TODO: manage error
				_ = q.workerFn(job.item)
			}
		}(i, q.dispatchedItems)
	}
}

func (q *Queue[T]) startDispatcher(ctx context.Context) {
	q.wg.Go(func() {
		defer close(q.dispatchedItems)
		for {
			item, ok := q.pop()
			if !ok {
				return
			}
			select {
			case q.dispatchedItems <- item:
			case <-ctx.Done():
				return
			}
		}
	})
}

func (q *Queue[T]) Serve(ctx context.Context) {
	q.dispatchedItems = make(chan queueJob[T], 100)
	q.startWorkers()
	q.startDispatcher(ctx)

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
	q.cond.Broadcast()

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
