# Concurrency

![Go](https://img.shields.io/badge/Go-00ADD8?style=flat&logo=go&logoColor=white)
[![LeetCode](https://img.shields.io/badge/LeetCode-venexene-FFA116?style=flat&logo=leetcode&logoColor=white)](https://leetcode.com/venexene)

Concurrency training in Go - solving problems and taking notes.

## Structure

Each problem lives in its own directory with:

- `solution.go` — the implementation
- `task.md` — problem statement, constraints, and examples
- `notes.md` — solution idea, complexity, and mistakes encountered
- `solution_test.go` — tests for ordering, completion, and data races

The full problem list is in [docs/TASKS.md](./docs/TASKS.md).
See [TESTING_GUIDE.md](./TESTING_GUIDE.md) for a practical guide to testing concurrent code (in Russian).

## Solved

**Solved: 40 problems · 4 topics**

**Solved difficulty: 18 Easy · 17 Medium · 5 Hard**

| Topic | Solved |
|------|--------|
| Goroutines & Execution Order | 10 |
| Channels & Streams | 10 |
| Shared Memory & Locks | 10 |
| Context & Cancellation | 10 |
| Worker Pools & Semaphores | 0 |
| Pipelines | 0 |
| Timers & Scheduling | 0 |
| Atomics & Memory Model | 0 |
| Debugging & Testing | 0 |
| Caches, Actors & Brokers | 0 |

## Goroutines & Execution Order (10)

- [x] [Print in Order](./goroutines/print-in-order/) - channel synchronization, close signals
- [x] [Ping Pong](./goroutines/ping-pong/) - buffered channels, alternating turns
- [x] [Zero Even Odd](./goroutines/zero-even-odd/) - parity routing, broadcast shutdown
- [x] [Concurrent Squares](./goroutines/concurrent-squares/) - per-index channels, WaitGroup
- [x] [Parallel Chunk Sum](./goroutines/parallel-chunk-sum/) - balanced chunks, channel aggregation
- [x] [Broadcast Latch](./goroutines/broadcast-latch/) - closed channel signal, sync.Once
- [x] [Goroutine Ring](./goroutines/goroutine-ring/) - sync.Cond turn-taking, WaitGroup
- [x] [Concurrent FizzBuzz](./goroutines/concurrent-fizz-buzz/) - sync.Cond, role-specific callbacks
- [x] [Building Water Molecules](./goroutines/building-water-molecules/) - sync.Cond, atom slots and group barrier
- [x] [Reusable Barrier](./goroutines/reusable-barrier/) - per-generation channels, mutex-protected arrivals

## Channels & Streams (10)

- [x] [Range Generator](./channel/range-generator/) - unbuffered channel, sequential generation and close
- [x] [Collect Until Close](./channel/collect-until-close/) - channel range, ordered collection
- [x] [Channel Map](./channel/channel-map/) - goroutine-based stream transformation
- [x] [Non-blocking Send](./channel/non-blocking-send/) - select with default for immediate send attempts
- [x] [Non-blocking Receive](./channel/non-blocking-receive/) - select with default and closed-channel state
- [x] [Take First Elements](./channel/take-first-elements/) - bounded reads, leaving the rest of the stream untouched
- [x] [Merge Two Channels](./channel/merge-two-channels/) - select over active inputs, per-source order
- [x] [Reliable Channel Tee](./channel/reliable-channel-tee/) - two unbuffered outputs with cancellation
- [x] [Predicate Partition](./channel/predicate-partition/) - predicate routing with backpressure
- [x] [Bounded Channel Queue](./channel/bounded-channel-queue/) - mutex-protected FIFO, wake-up signals and graceful close

## Shared Memory & Locks (10)

- [x] [Mutex Counter](./mutex/mutex-counter/) - RWMutex-protected updates and reads
- [x] [Concurrent Integer Set](./mutex/concurrent-integer-set/) - RWMutex-protected map operations
- [x] [Atomic Withdrawal](./mutex/atomic-withdrawal/) - lock-protected balance check and withdrawal
- [x] [Word Frequency Snapshot](./mutex/word-frequency-snapshot/) - RWMutex-protected map cloning
- [x] [Account Transfer](./mutex/account-transfer/) - ordered account locks and atomic IDs
- [x] [Lazy Once Initialization](./mutex/lazy-once-initialization/) - sync.Once for shared lazy loading
- [x] [Wait for Threshold](./mutex/wait-for-threshold/) - sync.Cond with predicate checks and broadcast
- [x] [Condition Variable Queue](./mutex/condition-variable-queue/) - bounded ring buffer with condition variables
- [x] [Writer-First Read-Write Lock](./mutex/writer-first-read-write-lock/) - writer tickets and reader admission control
- [x] [Multi-Wallet Transaction](./mutex/multi-wallet-transaction/) - ordered per-wallet locks and consistent snapshots

## Context & Cancellation (10)

- [x] [Cancelable Receive](./context/cancelable-receive/) - select between channel input and context cancellation
- [x] [Cancelable Counter Generator](./context/cancelable-counter-generator/) - unbuffered stream with cancelable sends
- [x] [Parent Deadline Budget](./context/parent-deadline-budget/) - child timeout bounded by the parent deadline
- [x] [Parallel Map with First Error](./context/parallel-map-first-error/) - per-index results, first-error cancellation, WaitGroup
- [x] [First Successful Response](./context/first-successful-response/) - first-success coordination, cancellation, and waiting for all calls
- [x] [Cleanup After Cancellation](./context/cleanup-after-cancellation/) - detached request values, independent timeout, synchronous cleanup
- [x] [Isolated Request Scopes](./context/isolated-request-scopes/) - named branches, cancellation causes, existing and future branch cancellation
- [x] [Shared Call with Independent Cancellation](./context/shared-call-independent-cancellation/) - one shared operation, independent waiters, cached results, concurrent Close
- [x] [Wait for Next Version](./context/wait-for-next-version/) - consistent value-version pairs, sync.Cond, cancelable waits, broadcast shutdown
- [x] [Dynamic Task Tree](./context/dynamic-task-tree/) - dynamic descendants, task accounting, first-error cancellation, waiting for the external cancellation callback

## Testing

Run a single problem while developing:

```sh
go test -race -count=1 -timeout=30s ./context/dynamic-task-tree
```

Run all problems:

```sh
go test -race -count=1 -timeout=60s ./...
```

Tests cover ordering, cancellation, completion, and concurrent access. Passing tests and the race detector do not prove correctness for every possible goroutine schedule; see each problem's notes for the scenarios checked and remaining limitations.

## Worker Pools & Semaphores (0)

No solved problems yet.

## Pipelines (0)

No solved problems yet.

## Timers & Scheduling (0)

No solved problems yet.

## Atomics & Memory Model (0)

No solved problems yet.

## Debugging & Testing (0)

No solved problems yet.

## Caches, Actors & Brokers (0)

No solved problems yet.
