// Package scheds provides scheduler definitions
package scheds

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dias-andre/garden/internal/lib"
)

type SchedulerSource[T any] interface {
	Next(context.Context) (T, bool)
}

type SchedulerSourceWithRetry[T any] interface {
	Next(context.Context) (T, bool)
	CanRetry(T, error) bool
	EnqueueRetry(ctx context.Context, j T, e error) error
}

type Scheduler[T any] struct {
	WorkerCount int
	Source      SchedulerSource[T]
	WorkerFn    func(T) error

	UseRateLimit bool
	Limiter      *lib.RateLimiter

	OnFailureHook func(context.Context, T)
	OnSuccessHook func(context.Context, T)
	OnBackoffHook func(T, context.Context)
	MaxRetries    int

	wg  sync.WaitGroup
	ctx context.Context

	pendingJobs    atomic.Int64
	dispatchedJobs chan T
}

func (s *Scheduler[T]) onSuccess(j T) {
	if s.OnSuccessHook != nil {
		s.OnSuccessHook(s.ctx, j)
	}
	s.pendingJobs.Add(-1)
}

func (s *Scheduler[T]) onFailure(j T) {
	if s.OnFailureHook != nil {
		s.OnFailureHook(s.ctx, j)
	}
	s.pendingJobs.Add(-1)
}

func (s *Scheduler[T]) handleBackoff(job T) {
	if s.OnBackoffHook != nil {
		s.OnBackoffHook(job, s.ctx)
	}
}

func (s *Scheduler[T]) processJob(job T) {
	err := s.WorkerFn(job)

	if err != nil {
		source, ok := s.Source.(SchedulerSourceWithRetry[T])
		// supports retry
		if ok && source.CanRetry(job, err) {
			s.handleBackoff(job)
			if retryErr := source.EnqueueRetry(s.ctx, job, err); retryErr != nil {
				s.onFailure(job)
			}
			return
		}
		s.onFailure(job)
	}
	s.onSuccess(job)
}

func (s *Scheduler[T]) GetPendingJobs() int64 {
	return s.pendingJobs.Load()
}

func (s *Scheduler[T]) IncPendingJobs() int64 {
	return s.pendingJobs.Add(1)
}

func (s *Scheduler[T]) WithRateLimit(items int, t time.Duration) *Scheduler[T] {
	s.UseRateLimit = true
	s.Limiter = lib.NewRateLimiter(items, t)
	return s
}

func (s *Scheduler[T]) WaitRoutines() {
	s.wg.Wait()
}

func (s *Scheduler[T]) Start(ctx context.Context) {
	s.ctx = ctx
	s.dispatchedJobs = make(chan T, s.WorkerCount)
	s.wg.Go(func() {
		defer close(s.dispatchedJobs)
		for {
			item, ok := s.Source.Next(ctx)
			if !ok {
				if s.pendingJobs.Load() == 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}
			select {
			case s.dispatchedJobs <- item:
			case <-s.ctx.Done():
				return
			}
		}
	})

	for i := 0; i < s.WorkerCount; i++ {
		s.wg.Add(1)
		go func(id int, items <-chan T) {
			defer s.wg.Done()
			for item := range items {
				if s.UseRateLimit {
					if err := s.Limiter.Wait(ctx); err != nil {
						return
					}
				}
				s.processJob(item)
			}
		}(i, s.dispatchedJobs)
	}
}
