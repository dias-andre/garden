# garden

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

	q.Shutdown()
	fmt.Println("all done")
}
```

## How it works

The core idea is a two-stage pipeline: an unbounded slice buffer feeds a bounded
channel, which fans out to a fixed pool of workers.

```
Push(item)                Pop()                 dispatchedItems            workers
   |                        |                         |                       |
   +--> items []T -------> chan T (cap 100) ------> workerFn(item) x N
        (slice + mutex         (dispatcher
         + cond)               goroutine)
```

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
            return // queue closed, no more items
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
        q.workerFn(job)
    }
}()
```

This is the standard fan-out pattern. The channel capacity (100) controls how many
items can be "in flight" between the dispatcher and the workers at any given time.

### Graceful shutdown

When the context is cancelled, a background goroutine calls `Close()` on the queue.
This sets a `closed` flag and broadcasts to all blocked `Pop()` calls. The dispatcher
sees the queue is closed, finishes sending any remaining items, closes the channel,
and the workers drain whatever is left.

`Shutdown()` waits for all workers and the dispatcher to finish, so you are guaranteed
that every item pushed before the context was cancelled gets processed.

```go
ctx, cancel := context.WithCancel(context.Background())
q.Serve(ctx)

// push some items...
_ = q.Push("work-1")
_ = q.Push("work-2")

cancel()      // signal shutdown
q.Shutdown()  // block until everything is processed
```

## API

### `NewQueue[T any](fn WorkerFn[T], count int) *Queue[T]`

Creates a new queue. `fn` is the function each worker calls for every item. `count` is
the number of worker goroutines.

### `Push(item T) error`

Adds an item to the queue. Returns an error wrapping `ErrQueueClosed` if the queue has
been closed. Under normal operation, this never blocks.

### `Serve(ctx context.Context)`

Starts the dispatcher and worker goroutines. Call this once, after pushing initial items
or before pushing -- it does not matter, since Push never blocks.

### `Shutdown()`

Blocks until all workers finish. Call this after cancelling the context to ensure a
clean exit.

## FAQ

**Q: Why the name "garden"?**
Because gardens are small, personal, and self-contained. This library is not meant for
large-scale distributed job processing. It is for the kind of project where you have a
handful of things to do and want a clean way to do them in parallel.

**Q: Can I push items after calling Serve?**
Yes. Push works at any time before Close. The dispatcher will pick up new items as they
appear in the slice.

**Q: What happens if a worker returns an error?**
Currently, errors are ignored (the return value of `workerFn` is discarded). This is
intentional for simplicity in small environments. If you need error handling, you can
handle it inside the worker function itself (log, retry, store in a shared variable,
etc.).

**Q: What is the channel capacity and can I change it?**
The dispatch channel has a fixed capacity of 100. This is hardcoded. For a garden-sized
tool, 100 items in flight is more than enough. If you need a different value, it would
be a one-line change in `garden.go`.

**Q: Is this safe for concurrent use?**
Yes. `Push` is safe to call from multiple goroutines. `Pop` is internal and not exported.
`Serve` and `Shutdown` should be called once from a single goroutine.

**Q: What happens if I call Shutdown without Serve?**
It returns immediately since no goroutines are tracked in the WaitGroup. No panic, no
deadlock.

**Q: What happens if I call Push after Close?**
You get an error: `cannot push: queue closed`.

## Tests

There are three tests in `queue_test.go` that cover the main behaviors:

**TestQueue_PushPop** -- Verifies basic FIFO semantics. Pushes three items, pops the
first one, checks that it comes out in order. Simple sanity check.

**TestQueue_NoDuplicateProcessing** -- Pushes 10,000 items to a pool of 8 workers. Every
item is tracked in a map with a mutex, and the test asserts each item was processed
exactly once. This validates that the fan-out dispatch does not duplicate or skip work,
even under heavy load.

**TestQueue_GracefulShutdown** -- Pushes 50 items, lets them process for a bit, then
cancels the context and calls Shutdown. Asserts that Shutdown returns within 2 seconds,
which catches goroutine leaks or deadlocks.

Run the tests with:

```
go test -v -race ./...
```

The `-race` flag is recommended to catch any concurrency issues.

## License

MIT
