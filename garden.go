// Package garden provides a small, generic worker pool for processing items
// with built-in support for retries and graceful shutdown.
//
// It's designed for small to medium workloads where you need concurrent
// processing without the overhead of a full message broker.
package garden

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	// ErrQueueClosed is returned when trying to push to closed queue.
	ErrQueueClosed = errors.New("queue closed")
	// ErrQueueDraining is returned when trying to push during drain shutdown.
	ErrQueueDraining = errors.New("queue draining")
)

type (
	// WorkerFn is the function type that processes items from queue.
	// It should return an error if processing fails (which may trigger retries).
	WorkerFn[T any]     func(item T) error
	DeadLetterFn[T any] func(item T, err error, attempts int)
	BackoffFn           func(int) time.Duration
)

type queueJob[T any] struct {
	item      T
	attempts  int
	lastErr   error
	lastRetry time.Time
}

// Queue is a generic worker pool that processes items concurrently.
// It supports automatic retries with exponential backoff and graceful shutdown.
// It uses an internal slice (protected by mutex) as an unbounded input buffer
// and a fixed-size channel to distribute work to workers. This hybrid approach
// allows producers to push items without blocking while preventing workers
// from being overwhelmed.
type Queue[T any] struct {
	mu              sync.Mutex
	cond            *sync.Cond
	workersWG       sync.WaitGroup
	dispatchedItems chan queueJob[T]

	closed   bool
	draining bool

	internalCtx       context.Context
	internalCtxCancel context.CancelFunc

	jobs        []queueJob[T]
	workerFn    func(item T) error
	workerCount int

	maxRetries     int
	backoffFn      func(int) time.Duration
	onDeadLetterFn func(item T, err error, attempts int)

	// rate limit
	useRateLimit bool
	rateLimiter  *rateLimiter
}

func (q *Queue[T]) pushJob(newJob queueJob[T]) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.Join(errors.New("cannot push"), ErrQueueClosed)
	}
	if q.draining && newJob.attempts == 0 {
		q.mu.Unlock()
		return errors.Join(errors.New("cannot push"), ErrQueueDraining)
	}
	q.jobs = append(q.jobs, newJob)
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}

func (q *Queue[T]) popJob() (queueJob[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.jobs) == 0 && !q.closed && !q.draining {
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

func (q *Queue[T]) processJob(job queueJob[T]) {
	err := q.workerFn(job.item)
	if err != nil {
		if job.attempts <= q.maxRetries {
			backoff := time.Duration(0)
			job.attempts++
			job.lastErr = err
			job.lastRetry = time.Now()

			if q.backoffFn != nil {
				backoff = q.backoffFn(job.attempts)
			}
			if backoff > 0 {
				select {
				case <-time.After(backoff):
				case <-q.internalCtx.Done():
					return
				}
			}

			time.Sleep(backoff)
			if err := q.pushJob(job); err != nil && q.onDeadLetterFn != nil {
				q.onDeadLetterFn(job.item, err, job.attempts)
			}
			return
		}
		if q.onDeadLetterFn != nil {
			q.onDeadLetterFn(job.item, err, job.attempts)
		}
		return
	}
}

func (q *Queue[T]) startWorkers() {
	for i := 0; i < q.workerCount; i++ {
		q.workersWG.Add(1)
		go func(id int, items <-chan queueJob[T]) {
			defer q.workersWG.Done()
			for job := range items {
				if q.useRateLimit {
					if err := q.rateLimiter.Wait(q.internalCtx); err != nil {
						return
					}
				}
				q.processJob(job)
			}
		}(i, q.dispatchedItems)
	}
}

func (q *Queue[T]) startDispatcher(ctx context.Context) {
	q.dispatchedItems = make(chan queueJob[T], 100)
	q.workersWG.Go(func() {
		defer close(q.dispatchedItems)
		for {
			item, ok := q.popJob()
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

// NewQueue creates a new worker pool with the given worker function and worker count.
//
// The workerFn is called for each item in the queue. If it returns an error,
// the item will be retried (if retries are configured) or sent to the dead letter handler.
//
// Example:
//
//	q := garden.NewQueue[Email](sendEmail, 4)
func NewQueue[T any](fn WorkerFn[T], count int) *Queue[T] {
	q := &Queue[T]{}
	q.cond = sync.NewCond(&q.mu)
	q.workerFn = fn
	q.workerCount = count
	return q
}

// WithRetries configures the maximum number of retry attempts for failed items.
// Default is 0 (no retries). The first attempt counts as attempts 1.
//
// Example:
// q := garden.NewQueue[Email](sendEmail, 4)
//
//	.WithRetries(3)
//	.WithBackoff(exponentialBackoff)
func (q *Queue[T]) WithRetries(r int) *Queue[T] {
	q.maxRetries = r
	return q
}

// WithBackoff configures the backoff function which calculates the delay between retries.
func (q *Queue[T]) WithBackoff(b func(int) time.Duration) *Queue[T] {
	q.backoffFn = b
	return q
}

func (q *Queue[T]) OnDeadLetter(dt func(item T, err error, attempt int)) *Queue[T] {
	q.onDeadLetterFn = dt
	return q
}

func (q *Queue[T]) WithRateLimit(items int, interval time.Duration) *Queue[T] {
	// q.rateLimitItems = items
	// q.rateLimitInterval = interval
	q.rateLimiter = newRateLimiter(items, interval)
	q.useRateLimit = true
	return q
}

// Push adds an item to the queue for processing
//
// Returns ErrQueueClosed if the queue has been closed, or ErrQueueDraining
// if the queue is in draining mode.
func (q *Queue[T]) Push(item T) error {
	newJob := queueJob[T]{
		item:     item,
		attempts: 0, // new job
	}
	return q.pushJob(newJob)
}

// Pop removes and returns the next item from the queue.
//
// It blocks until an item is available, the queue is closed, or draining mode
// is entered with no items remaining.
//
// Pop returns (item, true) if an item was successfully retrieved.
// It returns (zero, false) if the queue is closed or in draining mode with no items left.
func (q *Queue[T]) Pop() (T, bool) {
	job, status := q.popJob()
	return job.item, status
}

// Close closes the queue immediately and stops all workers.
// After Close(), Push() will return ErrQueueClosed.
// Items not yet dispatched to workers are discarded.
//
// For graceful shutdown that processes remaining items, use DrainAndShutdown instead.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
	if q.internalCtxCancel != nil {
		q.internalCtxCancel()
	}
}

// Serve starts the worker pool and begins processing items from the queue.
//
// It starts the configured number of workers in separate goroutines and creates
// a dispatcher that pulls items from the queue and sends them to workers.
// Workers will continue processing until the queue is closed or the context is cancelled.
//
// The context is used to signal graceful shutdown. When ctx.Done() is signalled,
// the dispatcher stops accepting new items but already-queued items may continue
// processing until Close() is called.
//
// Serve is non-blocking and returns immediately. Call Shutdown() or DrainAndShutdown()
// to stop the workers.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	q := garden.NewQueue[Email](sendEmail, 4)
//	q.Serve(ctx)
//
//	// Push items to be processed
//	q.Push(email1)
//	q.Push(email2)
//
//	// Gracefully shutdown when done
//	q.DrainAndShutdown(context.Background())
func (q *Queue[T]) Serve(ctx context.Context) {
	newCtx, newCancel := context.WithCancel(ctx)
	q.internalCtx = newCtx
	q.internalCtxCancel = newCancel

	q.startDispatcher(q.internalCtx)

	q.startWorkers()
	go func() {
		<-ctx.Done()
		q.Close()
	}()
}

// Shutdown closes the queue immediately and stops all workers.
// It waits up to the context deadline for workers to finish.
// Items not yet dispatched to workers are discarded.
// Push() will return ErrQueueClosed after this call.
//
// Returns an error if the context deadline is exceeded before workers stop.
// Use DrainAndShutdown for graceful shutdown that processes remaining items.
//
// Example:
// ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
// defer cancel()
//
//	if err := queue.Shutdown(ctx); err != nil {
//	  log.Printf("shutdown timeout: %v", err)
//	}
func (q *Queue[T]) Shutdown(ctx context.Context) error {
	q.Close()
	done := make(chan struct{})
	go func() {
		q.workersWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DrainAndShutdown gracefully shuts down the queue by processing all remaining items
// before stopping workers.
//
// It enters draining mode, which rejects new Push() calls but continues processing
// items already in the queue. Workers stop only after all items are processed.
//
// Returns an error if the context deadline is exceeded before all items are processed.
// If the deadline is exceeded, Close() is called to force shutdown and return the error.
//
// Example:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	if err := q.DrainAndShutdown(ctx); err == context.DeadlineExceeded {
//		log.Println("drain timeout, some items may be lost")
//	}
func (q *Queue[T]) DrainAndShutdown(ctx context.Context) error {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		return ErrQueueClosed
	}

	q.draining = true
	q.cond.Broadcast()
	q.mu.Unlock()

	done := make(chan error, 1)
	go func() {
		q.workersWG.Wait()
		q.internalCtxCancel()
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
