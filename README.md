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

## Solved

**Solved: 20 problems · 2 topics**

**Difficulty: 13 Easy · 6 Medium · 1 Hard**

| Topic | Solved |
|------|--------|
| Goroutines & Execution Order | 10 |
| Channels & Streams | 10 |
| Shared Memory & Locks | 0 |
| Context & Cancellation | 0 |
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

## Shared Memory & Locks (0)

No solved problems yet.

## Context & Cancellation (0)

No solved problems yet.

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
