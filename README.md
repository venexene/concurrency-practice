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

**Solved: 10 problems · 10 topics**

**Difficulty: 7 Easy · 3 Medium · 0 Hard**

| Topic | Solved |
|------|--------|
| Goroutines & Execution Order | 10 |
| Channels & Streams | 0 |
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

## Channels & Streams (0)

No solved problems yet.

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
