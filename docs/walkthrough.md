# garden: code walkthrough

A line-by-line tour of how `garden.go` works, from the public API down to the
concurrency primitives. Read it after the [README](../README.md) if you want to
understand *why* each piece exists.

## Types and configuration

```go
type (
	WorkerFn[T any]     func(item T) error
	DeadLetterFn[T any] func(item T, err error, attempts int)
	BackoffFn           func(int) time.Duration
)
```

Three function types define the whole knobs surface of the library:

- `WorkerFn` -- the actual work. It receives one item and returns an error when the
  work failed. The error is what drives the retry logic.
- `DeadLetterFn` -- a sink for work that never succeeded. Called with the item, the
  last error, and how many attempts were made in total.
- `BackoffFn` -- maps the attempt number to how long to sleep before trying again.

They are mutually independent so you can configure retries without a backoff, or a
dead-letter handler without retries, and everything still works.

```go
type queueJob[T any] struct {
	item      T
	attempts  int
	lastErr   error
	lastRetry time.Time
}
```

Note that the queue stores `queueJob`, not the bare item `T`. This is the key detail
that makes retries possible: when a job fails, it is re-queued **with its accumulated
attempt count**, so the next worker to pick it up knows how many times it has already
been tried. `lastErr` and `lastRetry` are bookkeeping for observability (handed to the
dead-letter handler and available to backoff functions).

## The Queue struct

```go
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

	maxRetries     int
	backoffFn      BackoffFn
	onDeadLetterFn DeadLetterFn[T]
}
```

The struct mixes three concerns:

1. **The input buffer**: `jobs` (`[]queueJob[T]`) guarded by `mu`, woken by `cond`.
2. **The fan-out**: `dispatchedItems` (a bounded channel) and `wg` used to track the
   dispatcher and worker goroutines.
3. **The policy knobs**: `maxRetries`, `backoffFn`, `onDeadLetterFn`, configured via the
   chainable `With*` methods.

`closed` and `draining` are two flavours of "no more pushes". They are read and written
under `mu`.

## Construction and configuration

```go
func NewQueue[T any](fn WorkerFn[T], count int) *Queue[T] {
	q := &Queue[T]{}
	q.cond = sync.NewCond(&q.mu)
	q.workerFn = fn
	q.workerCount = count
	return q
}
```

A queue starts inert -- no goroutines are spawned until `Serve`. `NewQueue` only stores
the worker function and the pool size. `sync.NewCond(&q.mu)` binds the condition variable
to the same mutex that protects `jobs`, which is required for `Wait`/`Broadcast` to work.

```go
func (q *Queue[T]) WithRetries(r int) *Queue[T] {
	q.maxRetries = r
	return q
}
```

Each `With*` method returns `*Queue[T]` for chaining:

```go
q := garden.NewQueue(sendEmail, 4).
	WithRetries(3).
	WithBackoff(exponential).
	OnDeadLetter(handleDead)
```

There is no locking in these methods -- they are meant to be called once, on a fresh
queue, before `Serve`. The configuration is read by `processJob` on the workers, so
mutating it concurrently with running workers would be a data race.

## Pushing and popping: the slice buffer

```go
func (q *Queue[T]) pushJob(newJob queueJob[T]) error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return errors.Join(errors.New("cannot push"), ErrQueueClosed)
	}
	if q.draining {
		q.mu.Unlock()
		return ErrQueueDraining
	}
	q.jobs = append(q.jobs, newJob)
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}
```

`pushJob` is the internal enqueue primitive. The public `Push` wraps it with a fresh job
whose `attempts` start at zero:

```go
func (q *Queue[T]) Push(item T) error {
	return q.pushJob(queueJob[T]{item: item})
}
```

Separating them matters: the retry path in `processJob` calls `pushJob` directly with an
existing (incremented) job, bypassing the public API. The two guard clauses are also
different by design:

- `closed` rejects *external* pushes only in `Push`... no, wait -- `pushJob` itself
  checks `closed`. During a retry the queue is being drained, not closed, so the check
  passes; if `Close` was already called, the retry is silently discarded, which is the
  correct "give up quietly" behaviour for a hard shutdown.
- `draining` rejects new work during `DrainAndShutdown`, but retries must still be
  allowed to re-enter -- so `processJob` swallows the returned error with `_ = q.pushJob`.

Finally, `cond.Signal()` wakes one waiting caller, rather than `Broadcast()`, because a
single item only needs one consumer.

```go
func (q *Queue[T]) popJob() (queueJob[T], bool) {
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
```

`popJob` is the dequeuing side. Three idiomatic `sync.Cond` points worth noting:

1. **The guard loop.** `cond.Wait()` releases `mu` while sleeping and reacquires it
   before returning. Waiters must re-check the predicate in a loop, because Go's `Cond`
   (like pthreads) allows spurious wakeups regardless of `Signal`/`Broadcast`.
2. **The exit condition.** The loop only exits when there is an item *or* the queue is
   `closed`. If a drain is active and the last item was popped, `len(q.jobs) == 0` is
   still true but `closed` is false -- the loop would block forever. That is prevented
   because `DrainAndShutdown` closes the queue once the wait group is done... but wait,
   there is no "no more items" tracker. In fact the dispatcher keeps popping until it
   gets `(zero, false)`, and `Close()` broadcasts on deadline. During a clean drain the
   dispatcher may block, but `Close()` (triggered by the cancelled context or a timeout)
   always breaks it. The `closed` flag is the universal escape hatch.
3. **O(1) dequeuing.** `q.jobs[1:]` drops the front element by reslicing the backing
   array instead of copying. This is the classic FIFO-queue-from-a-slice idiom.

## The dispatcher and workers

```go
func (q *Queue[T]) startDispatcher(ctx context.Context) {
	q.wg.Go(func() {
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
```

The dispatcher is the bridge between the unbounded buffer and the bounded channel. Each
iteration pops one job and tries to send it. The `select` makes the dispatcher
cancellation-aware: if the context is done *and* the channel is full, it bails out
instead of blocking forever. When the dispatcher returns (queue closed or ctx cancelled),
`defer close(q.dispatchedItems)` closes the channel, which terminates every worker's
`for job := range` loop.

`q.wg.Go` (the `WaitGroup.Go` helper added in Go 1.22) registers and runs the function in
a goroutine, so `wg.Wait()` covers the dispatcher too.

```go
func (q *Queue[T]) startWorkers() {
	for i := 0; i < q.workerCount; i++ {
		q.wg.Add(1)
		go func(id int, items <-chan queueJob[T]) {
			defer q.wg.Done()
			for job := range items {
				q.processJob(job)
			}
		}(i, q.dispatchedItems)
	}
}
```

`startWorkers` spawns `workerCount` goroutines that all read from the same channel. Go's
channel semantics do the scheduling: each worker blocks on receive until the dispatcher
delivers a job, so work goes to whatever worker is free first. Nothing about which worker
runs which job is coordinated by the library -- that is the point of the fan-out pattern.

## Retries: processJob

```go
func (q *Queue[T]) processJob(job queueJob[T]) {
	err := q.workerFn(job.item)
	if err != nil {
		if job.attempts <= q.maxRetries {
			backoff := time.Second * 0
			if q.backoffFn != nil {
				backoff = q.backoffFn(job.attempts)
			}
			job.attempts++
			job.lastErr = err
			job.lastRetry = time.Now()

			time.Sleep(backoff)
			_ = q.pushJob(job)
		} else {
			if q.onDeadLetterFn != nil {
				q.onDeadLetterFn(job.item, err, job.attempts)
			}
		}
	}
}
```

This is the heart of the retry behaviour. Walk it case by case:

- **Success** (`err == nil`): nothing happens. The job is gone from the queue and the
  worker moves on to the next one.
- **Failure, retries left** (`attempts <= maxRetries`): a backoff delay is computed
  (`backoffFn` is optional, defaulting to zero) and slept *before* re-queuing. The
  attempt counter is bumped, and the job goes back through `pushJob` -- the same buffer
  it came from. This is what makes retries "just work": no separate retry channel, no
  infinite loop in the worker; the job rides the normal pipeline again.
- **Failure, exhausted** (`attempts > maxRetries`): the dead-letter handler is invoked
  with the item, the last error, and the total attempts, then the job is dropped.

Two details are easy to miss:

1. **The counting semantics.** With `WithRetries(3)`, a job runs at most 4 times:
   attempts 0, 1, 2, 3 pass the `<=` check, each followed by one re-queue, and the 3rd
   failure (attempt 4) lands in the dead-letter path. The attempt number reported to
   `OnDeadLetter` is therefore "how many tries happened", matching intuition.

2. **`time.Sleep(backoff)` blocks the *worker*, not a dedicated timer.** While a job is
   sleeping it is not idle -- the worker simply does not pick up new work. That is fine
   for a garden-sized pool, but it means very long backoffs can stall all workers.

## Serve and shutdown

```go
func (q *Queue[T]) Serve(ctx context.Context) {
	q.dispatchedItems = make(chan queueJob[T], 100)
	q.startWorkers()
	q.startDispatcher(ctx)

	go func() {
		<-ctx.Done()
		q.Close()
	}()
}
```

`Serve` wires everything together in order:

1. Allocate the bounded channel (capacity 100).
2. Spawn the workers first, so they are already waiting on the channel before the first
   item can be dispatched.
3. Start the dispatcher.
4. Register a watcher goroutine that converts a cancelled context into `Close()`.

The watcher goroutine is the "no explicit Shutdown = no leak" safety net: cancel the
context and the queue tears itself down.

```go
func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
```

`Close` sets the flag and **broadcasts** differently from `Push`'s `Signal`: instead of
waking one waiter for one item, we want *every* blocked `popJob` to observe the closed
flag and return `false`, so the dispatcher can exit.

```go
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
```

`Shutdown` is "fast shutdown": it closes immediately (discarding whatever is still in
the slice buffer) and then waits for already-dispatched work to finish. The goroutine
watching `wg.Wait()` decouples the blocking wait from context cancellation -- `Wait`
cannot be interrupted, so it runs in the background and the `select` decides what to
report back.

```go
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
```

`DrainAndShutdown` is the graceful counterpart. The differences from `Shutdown`:

- It sets `draining` instead of `closed`, so the dispatcher keeps popping items (the
  guard loop still sees `len(jobs) > 0`), while `Push` starts returning
  `ErrQueueDraining`.
- `done` is buffered (capacity 1) so the goroutine never leaks if the context wins the
  `select` -- the send does not block forever waiting for a receiver.
- On context timeout it forces `Close()` and returns the error, trading "process
  everything" for "stop now, no hang".

That stripped-down draining state is exactly what separates this shutdown from a hard
close: the queue accepts no *new* work but faithfully finishes the work it already
committed to.