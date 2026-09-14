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
	workersWG       sync.WaitGroup
	dispatchedItems chan queueJob[T]

	closed   bool
	draining bool

	internalCtx       context.Context
	internalCtxCancel context.CancelFunc

	jobs        []queueJob[T]
	pendingJobs atomic.Int64
	workerFn    func(item T) error
	workerCount int

	retriesEnabled bool
	maxRetries     int
	backoffFn      func(int) time.Duration
	onDeadLetterFn func(item T, err error, attempts int)

	// rate limit
	useRateLimit bool
	rateLimiter  *rateLimiter
}
```

The struct mixes a few concerns:

1. **The input buffer**: `jobs` (`[]queueJob[T]`) guarded by `mu`, woken by `cond`.
2. **The fan-out**: `dispatchedItems` (a bounded channel) and `workersWG`, which tracks
   both the dispatcher and the worker goroutines.
3. **The policy knobs**: `maxRetries`, `backoffFn`, `onDeadLetterFn`, plus
   `retriesEnabled` and the `useRateLimit`/`rateLimiter` pair, configured via the
   chainable `With*` methods.
4. **The lifecycle plumbing**: `closed`/`draining` (two flavours of "no more pushes",
   read and written under `mu`) and `internalCtx`/`internalCtxCancel`, the queue's own
   context derived from the caller's `Serve(ctx)` context.
5. **The bookkeeping**: `pendingJobs` (`atomic.Int64`), which counts how many *fresh*
   jobs are still outstanding -- this is what lets the dispatcher distinguish "the
   buffer is empty" from "the buffer is empty but retries are about to be re-queued".

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
	q.retriesEnabled = true
	return q
}
```

`WithRetries` does two things: it stores the limit and flips `retriesEnabled`. The flag
matters because `maxRetries` alone is ambiguous -- is retrying disabled because the limit
is 0, or is someone deliberately retrying zero times? With the flag, retries only ever
happen when the caller *asked* for them.

```go
func (q *Queue[T]) WithRateLimit(items int, interval time.Duration) *Queue[T] {
	q.rateLimiter = newRateLimiter(items, interval)
	q.useRateLimit = true
	return q
}
```

`WithRateLimit` installs a token-bucket limiter (see below) and remembers it. Each `With*`
method returns `*Queue[T]` for chaining:

```go
q := garden.NewQueue(sendEmail, 4).
	WithRetries(3).
	WithBackoff(exponential).
	OnDeadLetter(handleDead).
	WithRateLimit(10, time.Second)
```

There is no locking in these methods -- they are meant to be called once, on a fresh
queue, before `Serve`. The configuration is read by the workers, so mutating it
concurrently with running workers would be a data race.

## Pushing and popping: the slice buffer

```go
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
	if newJob.attempts == 0 {
		q.pendingJobs.Add(1)
	}
	q.mu.Unlock()
	q.cond.Signal()
	return nil
}
```

`pushJob` is the internal enqueue primitive. The public `Push` wraps it with a fresh job
whose `attempts` start at zero:

```go
func (q *Queue[T]) Push(item T) error {
	newJob := queueJob[T]{
		item:     item,
		attempts: 0, // new job
	}
	return q.pushJob(newJob)
}
```

Three details are worth stopping on:

- **The draining guard only rejects fresh jobs.** The condition is
  `q.draining && newJob.attempts == 0`. A retry (which carries `attempts > 0`) is still
  accepted during a drain, so in-flight work can be re-queued until the drain truly
  finishes. New external work, meanwhile, is refused with `ErrQueueDraining`.
- **`pendingJobs` counts fresh jobs only.** It is incremented when `attempts == 0` -- a
  brand-new push. Retries never touch it, because a retry is the *same* job still being
  tracked; incrementing again would double-count it.
- **`cond.Signal()` wakes one waiting caller**, not all of them, because a single item
  only needs one consumer.

```go
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
```

`popJob` is the dequeuing side. Three idiomatic `sync.Cond` points worth noting:

1. **The guard loop.** `cond.Wait()` releases `mu` while sleeping and reacquires it
   before returning. Waiters must re-check the predicate in a loop, because Go's `Cond`
   (like pthreads) allows spurious wakeups regardless of `Signal`/`Broadcast`.
2. **The exit condition.** The loop exits when there is an item, or the queue is
   `closed`, **or it is `draining` and the buffer ran out**. The `!q.draining` term is
   what makes a clean drain terminate: once draining starts, the last `popJob` in a
   drained buffer returns `(zero, false)` instead of blocking forever. The `closed` flag
   remains the universal escape hatch for hard shutdowns.
3. **O(1) dequeuing.** `q.jobs[1:]` drops the front element by reslicing the backing
   array instead of copying. This is the classic FIFO-queue-from-a-slice idiom.

## The dispatcher and workers

```go
func (q *Queue[T]) startDispatcher(ctx context.Context) {
	q.dispatchedItems = make(chan queueJob[T], 100)
	q.workersWG.Go(func() {
		defer close(q.dispatchedItems)
		for {
			item, ok := q.popJob()
			if !ok {
				if q.pendingJobs.Load() == 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
				continue
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

The dispatcher is the bridge between the unbounded buffer and the bounded channel. It
also allocates the channel (capacity 100) -- everything downstream depends on that
channel existing, so it is created where it is first used.

The loop is where `pendingJobs` earns its keep. Normally, `!ok` only happens when the
queue is closed, so the dispatcher exits immediately. But during a *drain*, `popJob`
can return `!ok` while retries are still in flight: a failed job is about to call
`pushJob` to re-queue itself, and if the dispatcher exits now, that retry never gets
processed. `pendingJobs` is still non-zero in that window, so instead of exiting the
dispatcher sleeps 10ms and pops again -- it only bails out when the counter hits zero,
meaning no fresh job is either queued, dispatched, or being retried.

The `select` makes the dispatcher cancellation-aware as well: if the context is done
*and* the channel is full, it bails out instead of blocking forever. When the dispatcher
returns, `defer close(q.dispatchedItems)` closes the channel, which terminates every
worker's `for job := range` loop.

`q.workersWG.Go` (the `WaitGroup.Go` helper added in Go 1.22) registers and runs the
function in a goroutine, so `workersWG.Wait()` covers the dispatcher too.

```go
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
```

`startWorkers` spawns `workerCount` goroutines that all read from the same channel. Go's
channel semantics do the scheduling: each worker blocks on receive until the dispatcher
delivers a job, so work goes to whatever worker is free first.

Before each job, the worker optionally consults the rate limiter (see below). If the
internal context is already cancelled, `Wait` returns an error and the worker stops
immediately instead of sleeping through the remaining shutdown sequence.

## Retries: processJob

```go
func (q *Queue[T]) processJob(job queueJob[T]) {
	err := q.workerFn(job.item)
	if err != nil {
		if job.attempts < q.maxRetries && q.retriesEnabled {
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

			if err := q.pushJob(job); err != nil {
				if q.onDeadLetterFn != nil {
					q.onDeadLetterFn(job.item, err, job.attempts)
				}
				q.pendingJobs.Add(-1)
			}
			return
		}
		if q.onDeadLetterFn != nil {
			q.onDeadLetterFn(job.item, err, job.attempts)
		}
		q.pendingJobs.Add(-1)
		return
	}
	q.pendingJobs.Add(-1)
}
```

This is the heart of the retry behaviour. Walk it case by case:

- **Success** (`err == nil`): `pendingJobs--` and the worker moves on. The job is gone
  from the queue.
- **Failure, retries left** (`attempts < maxRetries` **and** `retriesEnabled`): the
  attempt counter is bumped *first*, then a backoff delay is computed (`backoffFn` is
  optional, defaulting to zero) and slept before re-queuing. The job goes back through
  `pushJob` -- the same buffer it came from. That is what makes retries "just work": no
  separate retry channel, no infinite loop in the worker; the job rides the normal
  pipeline again.
- **Failure, exhausted** (`attempts >= maxRetries`, or retries never enabled): the
  dead-letter handler is invoked with the item, the last error, and the total attempts,
  `pendingJobs--`, and the job is dropped.

Four details are easy to miss:

1. **The counting semantics.** With `WithRetries(3)`, a job runs at most 4 times:
   attempts 0, 1, and 2 pass the `< 3` check (each followed by one re-queue), and the
   4th run -- `attempts == 3` -- falls through to the dead-letter path. The attempt
   number reported to `OnDeadLetter` is therefore "how many tries happened", matching
   intuition.
2. **`retriesEnabled` gates the whole path.** Unlike older versions where `maxRetries`
   alone decided, retrying now requires an explicit `WithRetries` call. A job pushed to a
   queue that never called `WithRetries` fails straight into the dead-letter handler.
3. **The backoff sleep is cancellation-aware.** It is not a bare `time.Sleep`: the
   `select` watches the internal context, so a `Close()` (hard shutdown) interrupts a
   long backoff immediately -- the worker returns and never re-queues the job.
4. **`pendingJobs` is decremented exactly once per fresh job**, on its *terminal*
   processing: success, dead letter, or (in the retry branch) a failed re-push. Note the
   failed re-push case: if the queue was closed hard mid-retry, `pushJob` errors, the job
   is dead-lettered with that error, and the counter is corrected so the drain/dispatcher
   can still terminate.

## Rate limiting

```go
type rateLimiter struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	fillRate   float64
	lastUpdate time.Time
}

func newRateLimiter(limit int, interval time.Duration) *rateLimiter {
	rate := float64(limit) / interval.Seconds()
	return &rateLimiter{
		tokens:     float64(limit),
		maxTokens:  float64(limit),
		fillRate:   rate,
		lastUpdate: time.Now(),
	}
}

func (r *rateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()

	now := time.Now()
	elapsed := now.Sub(r.lastUpdate).Seconds()

	if elapsed > 0 {
		r.tokens += elapsed * r.fillRate
		if r.tokens > r.maxTokens {
			r.tokens = r.maxTokens
		}
		r.lastUpdate = now
	}

	r.tokens -= 1.0

	var waitTime time.Duration
	if r.tokens < 0 {
		deficit := -r.tokens
		waitTime = time.Duration((deficit / r.fillRate) * float64(time.Second))
	}

	r.mu.Unlock()

	if waitTime > 0 {
		select {
		case <-time.After(waitTime):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
```

This is a classic **token bucket**, and it is small enough to hold in one head:

- The bucket starts full (`tokens = maxTokens = limit`).
- Before each job, the worker consumes one token. Tokens refill continuously at
  `fillRate = limit / interval` per second, capped at `maxTokens` so the bucket never
  grows beyond a full burst.
- If the bucket is empty the worker must wait `deficit / fillRate` seconds -- the time
  needed to earn the token back. Because every worker in the pool calls `Wait`, the
  *total* throughput is capped: with `WithRateLimit(10, time.Second)`, five workers
  together still start no more than ten jobs per second.
- The wait is cancellation-aware: if the internal context is done, `Wait` returns
  `ctx.Err()` and the worker stops instead of sleeping through the shutdown.

The trade-off is the mirror of the backoff one: while a worker is waiting on the bucket
it is not doing other work, but the cap is exactly what was asked for.

## Serve and shutdown

```go
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
```

`Serve` wires everything together:

1. **Derive the internal context.** The dispatcher, workers, backoff sleeps, and rate
   limiter all observe `internalCtx`, a child of the caller's `ctx`. Cancelling the
   caller's context cancels this one too -- and, as we saw, `Close()` cancels it as well,
   so every sleeping primitive gets a single, uniform "stop now" signal.
2. **Start the dispatcher**, which also allocates the bounded channel (capacity 100).
3. **Start the workers** so they are waiting on the channel.
4. **Register a watcher goroutine** that converts a cancelled context into `Close()`.

The watcher goroutine is the "no explicit Shutdown = no leak" safety net: cancel the
context and the queue tears itself down.

```go
func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
	if q.internalCtxCancel != nil {
		q.internalCtxCancel()
	}
}
```

`Close` does two things. First, it sets the flag and **broadcasts** -- differently from
`Push`'s `Signal`: instead of waking one waiter for one item, we want *every* blocked
`popJob` to observe the closed flag and return `false`, so the dispatcher can exit.
Second, it cancels `internalCtx`, which wakes any sleeping backoff or rate-limit wait.

```go
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
```

`Shutdown` is "fast shutdown": it closes immediately (discarding whatever is still in
the slice buffer) and then waits for already-dispatched work to finish. The goroutine
watching `workersWG.Wait()` decouples the blocking wait from context cancellation --
`Wait` cannot be interrupted, so it runs in the background and the `select` decides what
to report back.

```go
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
```

`DrainAndShutdown` is the graceful counterpart. The differences from `Shutdown`:

- It sets `draining` instead of `closed`, and **broadcasts immediately** so any blocked
  `popJob` wakes up, re-checks its predicate, and -- if the buffer is empty -- returns
  `(zero, false)`. Meanwhile `Push` starts returning `ErrQueueDraining`, but retries
  (attempts > 0) are still allowed through `pushJob`.
- The dispatcher now relies on `pendingJobs`: "buffer empty, counter zero" means the
  drain is genuinely done and it exits; "buffer empty, counter non-zero" means a retry is
  about to be re-queued, so it waits a beat and re-pops.
- `done` is buffered (capacity 1) so the goroutine never leaks if the context wins the
  `select` -- the send does not block forever waiting for a receiver.
- Once `workersWG.Wait()` returns, it cancels `internalCtx` to release any still-sleeping
  primitives, then reports success.
- On context timeout it forces `Close()` and returns the error, trading "process
  everything" for "stop now, no hang".

That stripped-down draining state is exactly what separates this shutdown from a hard
close: the queue accepts no *new* work but faithfully finishes the work it already
committed to -- including anything that fails and is re-queued for another try.