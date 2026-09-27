package garden

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dias-andre/garden/internal/lib"
	"github.com/dias-andre/garden/internal/scheds"
)

type sourceWithRetry[T any] struct {
	mu   sync.Mutex
	fifo *lib.Fifo[queueJob[T]]

	maxRetries int

	closed   bool
	draining bool
}

func (s *sourceWithRetry[T]) pushJob(job queueJob[T]) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrQueueClosed
	}
	if s.draining && job.attempts == 0 {
		s.mu.Unlock()
		return ErrQueueDraining
	}
	s.mu.Unlock()
	s.fifo.Push(job)
	return nil
}

func (s *sourceWithRetry[T]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.fifo.Broadcast()
}

func (s *sourceWithRetry[T]) enableDraining() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrQueueClosed
	}
	s.draining = true
	s.fifo.Broadcast()
	s.mu.Unlock()
	return nil
}

func (s *sourceWithRetry[T]) Next(_ context.Context) (queueJob[T], bool) {
	return s.fifo.Pop()
}

func (s *sourceWithRetry[T]) CanRetry(job queueJob[T], _ error) bool {
	if s.maxRetries > 0 && job.attempts < s.maxRetries {
		return true
	}
	return false
}

func (s *sourceWithRetry[T]) EnqueueRetry(_ context.Context, job queueJob[T], err error) error {
	job.attempts++
	job.lastErr = err
	job.lastRetry = time.Now()
	return s.pushJob(job)
}

type SourcedHooks[T any] struct {
	CommitPending func(ctx context.Context, item T) error
	CommitSuccess func(ctx context.Context, item T) error

	CommitFailed func(ctx context.Context, item T, err error, attempts int) error
}

type SourcedQueue[T any] struct {
	workerFn  func(T) error
	source    *sourceWithRetry[T]
	scheduler *scheds.Scheduler[queueJob[T]]

	hooks SourcedHooks[T]

	ctx       context.Context
	cancelCtx context.CancelFunc
}

func NewSourcedQueue[T any](fn func(T) error, count int, h SourcedHooks[T]) (*SourcedQueue[T], error) {
	if h.CommitPending == nil {
		return nil, errors.New("CommitPending is required")
	}
	if h.CommitSuccess == nil {
		return nil, errors.New("CommitSuccess is required")
	}

	fifo := lib.NewFifo[queueJob[T]]()
	sq := &SourcedQueue[T]{
		workerFn: fn,
		source: &sourceWithRetry[T]{
			fifo: fifo,
		},
		scheduler: &scheds.Scheduler[queueJob[T]]{
			WorkerFn: func(job queueJob[T]) error {
				return fn(job.item)
			},
			WorkerCount: count,
		},
	}
	sq.scheduler.Source = sq.source
	sq.hooks = h

	sq.scheduler.OnSuccessHook = func(ctx context.Context, qj queueJob[T]) {
		sq.hooks.CommitSuccess(ctx, qj.item)
	}

	if sq.hooks.CommitFailed != nil {
		sq.scheduler.OnFailureHook = func(ctx context.Context, qj queueJob[T]) {
			sq.hooks.CommitFailed(ctx, qj.item, qj.lastErr, qj.attempts)
		}
	}

	sq.ctx, sq.cancelCtx = context.WithCancel(context.Background())

	return sq, nil
}

func (sq *SourcedQueue[T]) Push(item T) error {
	if commitErr := sq.hooks.CommitPending(sq.ctx, item); commitErr != nil {
		return commitErr
	}
	if err := sq.source.pushJob(queueJob[T]{
		item:     item,
		attempts: 0,
	}); err != nil {
		return err
	}
	sq.scheduler.IncPendingJobs()
	return nil
}

func (sq *SourcedQueue[T]) Pop(ctx context.Context) (T, bool) {
	job, status := sq.source.Next(ctx)
	return job.item, status
}

func (sq *SourcedQueue[T]) Serve(ctx context.Context) {
	sq.ctx, sq.cancelCtx = context.WithCancel(ctx)
	sq.scheduler.Start(ctx)
}

func (sq *SourcedQueue[T]) Shutdown(ctx context.Context) error {
	sq.source.close()
	done := make(chan struct{})
	go func() {
		sq.scheduler.WaitRoutines()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (sq *SourcedQueue[T]) DrainAndShutdown(ctx context.Context) error {
	if drainErr := sq.source.enableDraining(); drainErr != nil {
		return drainErr
	}
	done := make(chan struct{})
	go func() {
		sq.scheduler.WaitRoutines()
		sq.cancelCtx()
		done <- struct{}{}
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		sq.source.close()
		return ctx.Err()
	}
}

func (sq *SourcedQueue[T]) WithRetries(r int) *SourcedQueue[T] {
	sq.source.maxRetries = r
	return sq
}

func (sq *SourcedQueue[T]) WithRateLimit(items int, interval time.Duration) *SourcedQueue[T] {
	sq.scheduler.WithRateLimit(items, interval)
	return sq
}

func (sq *SourcedQueue[T]) WithBackoff(b func(int) time.Duration) *SourcedQueue[T] {
	sq.scheduler.OnBackoffHook = func(qj queueJob[T], ctx context.Context) {
		backoff := b(qj.attempts)
		if backoff > 0 {
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
		}
	}
	return sq
}
