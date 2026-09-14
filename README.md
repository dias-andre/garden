# garden

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)

A small, generic worker pool for Go. The name is a reminder that this is meant for
gardens, not factories -- small, personal environments where you need a handful of
goroutines processing items without the overhead of a full message broker or job queue.

If you are building a distributed system that processes millions of jobs per second,
this is not the tool. If you need a clean way to fan out work across a few workers in
a script, a CLI, a small service, or a side project, garden gets the job done with zero
dependencies.

## Installation

```
go get github.com/dias-andre/garden
```

Requires Go 1.27 or later (generics). No external dependencies.

## Quick start

```go
package main

import (
 "context"
 "fmt"
 "time"

 "github.com/dias-andre/garden"
)

func main() {
 q := garden.NewQueue(func(item string) error {
  fmt.Println("processing:", item)
  time.Sleep(100 * time.Millisecond)
  return nil
 }, 3) // 3 workers

 ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
 defer cancel()

 q.Serve(ctx)

 for i := 0; i < 10; i++ {
  _ = q.Push(fmt.Sprintf("job-%d", i))
 }

 if err := q.Shutdown(ctx); err != nil {
  fmt.Println("shutdown failed:", err)
 }
 fmt.Println("all done")
}
```

### Retries and dead letters

`WorkerFn` may return an error for a failed item. garden can retry it with backoff
and, once the retries are exhausted, hand it to a dead-letter handler:

```go
q := garden.NewQueue(sendEmail, 4). // sendEmail(item Email) error
 WithRetries(3).
 WithBackoff(func(attempt int) time.Duration {
  return time.Duration(1<<attempt) * time.Second // 1s, 2s, 4s
 }).
 OnDeadLetter(func(item Email, err error, attempts int) {
  log.Printf("email %q failed after %d attempts: %v", item.Subject, attempts, err)
 })
```

### Rate limiting

To cap how fast work is processed, register a rate limit with `WithRateLimit`. This is
a global limit across the whole pool, not per worker:

```go
q := garden.NewQueue(fetchURL, 8).
 WithRateLimit(10, time.Second) // at most 10 items per second

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

q.Serve(ctx)

for _, url := range urls {
 _ = q.Push(url)
}
if err := q.DrainAndShutdown(ctx); err != nil {
 log.Fatal("shutdown failed:", err)
}
```

## How it works

The core idea is a two-stage pipeline: an unbounded slice buffer feeds a bounded
channel, which fans out to a fixed pool of workers.

```
Push(item)                Pop()                dispatchedItems            workers
   |                        |                         |                       |
   +--> items []T -------> chan T (cap 100) ------> processJob(item) x N
        (slice + mutex         (dispatcher
         + cond)               goroutine)
```

### Docs

If you want to understand *why* each piece exists -- the choice of `sync.Cond` over a
plain channel, the retry counting semantics, how the drain knows when it is truly
finished -- there is a line-by-line code walkthrough in
[docs/walkthrough.md](docs/walkthrough.md). It mirrors `garden.go` from the public API
down to the concurrency primitives.

### Stage 1: the slice buffer

When you call `Push(item)`, the item is appended to an internal slice (`[]T`). This
slice is protected by a mutex and coordinated with a condition variable (`sync.Cond`).

The important thing here is that **Push never blocks**. You can push as many items as
you want and the producer keeps running. The slice acts as a buffer that absorbs bursts
of work faster than the workers can consume them.

On the other side, `Pop()` blocks (via `cond.Wait()`) when the slice is empty. When
`Push()` adds an item, it calls `cond.Signal()` to wake up a blocked `Pop()`.

Why not just use a channel directly? Because a channel has a fixed capacity. If the
channel is full, the producer blocks. With the slice approach, producers are always
free to push, and the dispatcher goroutine pulls from the slice at whatever rate the
channel allows.

### Stage 2: the dispatcher and workers

`Serve(ctx)` sets up the second stage. A single dispatcher goroutine runs a loop that
calls `Pop()` and sends each item into a buffered channel (`dispatchedItems`, capacity
100). This channel is where the actual concurrency happens.

```go
// simplified dispatcher loop
go func() {
    defer close(q.dispatchedItems)
    for {
        item, ok := q.Pop()
        if !ok {
            if q.pendingJobs.Load() == 0 {
                return // nothing left, no retries in flight
            }
            time.Sleep(10 * time.Millisecond) // wait for re-queued retries
            continue
        }
        q.dispatchedItems <- item // blocks if channel is full
    }
}()
```

The channel is shared among N worker goroutines. Go's channel semantics handle the
load balancing -- whichever worker is free picks up the next item:

```go
// simplified worker
go func() {
    for job := range q.dispatchedItems {
        if q.useRateLimit {
            if err := q.rateLimiter.Wait(q.internalCtx); err != nil {
                return // context cancelled
            }
        }
        q.processJob(job)
    }
}()
```

This is the standard fan-out pattern. The channel capacity (100) controls how many
items can be "in flight" between the dispatcher and the workers at any given time.

### Stage 3: retries and dead letters

Once a worker receives a job, it runs it through `processJob`. If `workerFn` returns an
error and retries are configured, the job is re-queued -- back into the same slice
buffer, right where it started -- with its attempt counter incremented.

```go
// simplified processJob
func processJob(job queueJob) {
    err := workerFn(job.item)
    if err == nil {
        return
    }
    if q.retriesEnabled && job.attempts < maxRetries {
        job.attempts++
        backoff := time.Duration(0)
        if q.backoffFn != nil {
            backoff = q.backoffFn(job.attempts)
        }
        select {
        case <-time.After(backoff): // backoff is cancellation-aware
        case <-q.internalCtx.Done():
            return
        }
        q.pushJob(job) // re-queue for another try
        return
    }
    q.onDeadLetterFn(job.item, err, job.attempts) // give up and report
}
```

Re-queueing is what makes retries work without any extra machinery: the job goes
through the exact same pipeline as a freshly pushed item, so it is naturally processed
by whichever worker finishes next. The `Push`-during-`Close` guard is skipped for
internal re-pushes, so a drain that is still running keeps retrying in-flight work. The
backoff sleep is also interrupted by shutdown, so a hard close never waits out a long
delay.

### Graceful shutdown

When the context passed to `Serve` is cancelled, a background goroutine calls `Close()`
on the queue. This sets a `closed` flag, broadcasts to all blocked `Pop()` calls, and
cancels the queue's internal context (which also aborts any sleeping backoff or
rate-limit wait). The dispatcher sees the queue is closed, finishes sending any
remaining items, closes the channel, and the workers drain whatever is left.

There are two ways to stop a running queue:

- **`Shutdown(ctx)`** closes the queue immediately and waits for in-flight workers to
  finish, discarding items not yet dispatched. It returns an error if the context
  deadline expires first.
- **`DrainAndShutdown(ctx)`** enters draining mode first: new `Push()` calls are
  rejected with `ErrQueueDraining`, but every remaining item -- plus anything re-queued
  by retries -- is processed before the workers stop. A `pendingJobs` counter lets the
  dispatcher tell "empty buffer" from "empty buffer with retries still to be re-queued".
  If the deadline expires, it forces a `Close()` and returns the error.

```go
ctx, cancel := context.WithCancel(context.Background())
q.Serve(ctx)

// push some items...
_ = q.Push("work-1")
_ = q.Push("work-2")

cancel()                             // signal shutdown
if err := q.DrainAndShutdown(context.Background()); err != nil {
    log.Println("drain failed:", err)
}
```

## API

### `NewQueue[T any](fn WorkerFn[T], count int) *Queue[T]`

Creates a new queue. `fn` is the function each worker calls for every item. `count` is
the number of worker goroutines.

### `(*Queue).WithRetries(r int) *Queue[T]`

Sets the maximum number of retries for failed items. Default is `0` (no retries): a
failed item is handed straight to the dead-letter handler. Config methods are chainable
and must be called before `Serve`.

### `(*Queue).WithBackoff(b BackoffFn) *Queue[T]`

Sets the delay function used between retries. `BackoffFn` receives the attempt number
and returns the `time.Duration` to sleep. Defaults to no delay.

### `(*Queue).OnDeadLetter(dt DeadLetterFn[T]) *Queue[T]`

Sets the handler called when an item exhausts all retries. `DeadLetterFn` receives the
item, the last error, and the total number of attempts. No-op by default.

### `(*Queue).WithRateLimit(items int, interval time.Duration) *Queue[T]`

Limits processing to `items` jobs per `interval` across the whole pool. Implemented with
a token-bucket limiter; workers wait on the bucket before each job. Cancellation-aware.

### `Push(item T) error`

Adds an item to the queue. Returns an error wrapping `ErrQueueClosed` if the queue has
been closed, or `ErrQueueDraining` during a drain. Under normal operation, this never
blocks.

### `Pop() (T, bool)`

Removes and returns the next item from the queue. Blocks until an item is available or
the queue is closed/draining. Returns `(zero, false)` when no items remain.

### `Serve(ctx context.Context)`

Starts the dispatcher and worker goroutines. Call this once, after pushing initial items
or before pushing -- it does not matter, since Push never blocks. Cancelling `ctx` closes
the queue.

### `Shutdown(ctx context.Context) error`

Closes the queue immediately and blocks until all in-flight workers finish, up to the
context deadline. Items not yet dispatched are discarded. Returns `ctx.Err()` on timeout.

### `DrainAndShutdown(ctx context.Context) error`

Gracefully processes every remaining item before stopping the workers. New `Push()` calls
are rejected with `ErrQueueDraining`. Returns `ctx.Err()` if the deadline expires before
the drain completes.

## FAQ

**Q: Why the name "garden"?**
Because gardens are small, personal, and self-contained. This library is not meant for
large-scale distributed job processing. It is for the kind of project where you have a
handful of things to do and want a clean way to do them in parallel.

**Q: Can I push items after calling Serve?**
Yes. Push works at any time before Close. The dispatcher will pick up new items as they
appear in the slice.

**Q: What happens if a worker returns an error?**
If `WithRetries` is configured, the item is re-queued and processed again, with a backoff
delay in between. Once the attempt limit is reached, `OnDeadLetter` is called (or the
error is silently dropped if no handler is set). Without retries, the dead-letter handler
runs immediately on the first failure.

**Q: How many times will a job be attempted with `WithRetries(3)`?**
Four: the initial attempt plus three retries. The retry count passed to `WithBackoff` and
`OnDeadLetter` starts at `1` for the first attempt after the initial failure.

**Q: Can I rate limit the workers?**
Yes, with `WithRateLimit(items, interval)`. It raises a global cap -- no more than
`items` jobs start within each `interval`, regardless of how many workers there are. The
limiter is a token bucket and is cancellation-aware.

**Q: What is the channel capacity and can I change it?**
The dispatch channel has a fixed capacity of 100. This is hardcoded. For a garden-sized
tool, 100 items in flight is more than enough. If you need a different value, it would
be a one-line change in `garden.go`.

**Q: Is this safe for concurrent use?**
Yes. `Push` is safe to call from multiple goroutines. `Serve`, `Shutdown`, and
`DrainAndShutdown` should be called once from a single goroutine.

**Q: What happens if I call Shutdown/DrainAndShutdown without Serve?**
They return immediately since no goroutines are tracked in the WaitGroup (or, for a drain,
after closing the queue). No panic, no deadlock.

**Q: What happens if I call Push after Close?**
You get an error wrapping `ErrQueueClosed`.

## Tests

There are five tests in `queue_test.go` covering the main behaviors:

**TestQueue_PushPop** -- Verifies basic FIFO semantics. Pushes three items, pops the
first one, checks that it comes out in order. Simple sanity check.

**TestQueue_NoDuplicateProcessing** -- Pushes 10,000 items to a pool of 8 workers. Every
item is tracked in a map with a mutex, and the test asserts each item was processed
exactly once. This validates that the fan-out dispatch does not duplicate or skip work,
even under heavy load.

**TestQueue_GracefulShutdown** -- Pushes 50 items, lets them process for a bit, then
cancels the context and calls Shutdown. Asserts that Shutdown returns within 2 seconds,
which catches goroutine leaks or deadlocks.

**TestQueue_RateLimit** -- Pushes 20 items to 5 workers limited to 10 per second and
asserts the run takes at least 1 second and every item is processed, verifying the global
rate cap.

**TestQueue_RetryBackoffDeadLetter** -- Pushes a mix of items, some of which always fail,
with retries, backoff, and a dead-letter handler. Asserts failed items are attempted the
expected number of times and land in the dead-letter sink exactly once.

Run the tests with:

```
go test -v -race ./...
```

The `-race` flag is recommended to catch any concurrency issues.

## License

MIT