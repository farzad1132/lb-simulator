# Server Queue Scheduling (`ms`)

This document describes server-side queue scheduling in the microservice simulator (`ms`). Scheduling is independent of load balancing: load balancers pick *which server* receives work; scheduling picks *which waiting item* that server serves next.

See also: [microservice-simulation.md](microservice-simulation.md) for overall request flow and replica queue semantics; [lb-vs-ms.md](lb-vs-ms.md) for feature comparison with the flat `lb` simulator.

## Overview

Each replica (server) has a local queue. By default, the queue is **FIFO** (`--scheduling fifo`). With **EDF** (Earliest Deadline First, `--scheduling edf`), newly arriving work — both upstream arrivals and downstream returns — is inserted by deadline so the server tends to serve requests closest to their SLO deadline first.

Scheduling applies only to **replica queues**. It does not reorder work at shared `DownstreamBalancer` pull queues used by `--lb-policy centralized`.

## CLI

| Flag | Default | Values | Description |
|------|---------|--------|-------------|
| `--scheduling` | `fifo` | `fifo`, `edf` | Server queue discipline at each replica |

Example:

```bash
./target/release/ms \
  --callgraph tests/chain/3/callgraph.json \
  --load-file tests/chain/3/load.json \
  --scheduling edf \
  --n 10000
```

## Deadlines

When a user request is injected, the simulator assigns a deadline:

```
deadline = arrival_time + slo_ms
```

- `slo_ms` comes from `load.json` for that API (or `--slo-ms` override).
- The deadline is stored on the `Hop` and propagated to all downstream hops and returns of the same request via `clone`.
- Deadlines are used only for EDF enqueue ordering; SLO violation reporting still uses `slo_ms` from the load file.

## Queue item types

Each replica queue holds two kinds of work:

| Kind | Source | What it is |
|------|--------|------------|
| **Upstream** | `EdgeBalancer`, `ReplicaBalancer`, or `DownstreamBalancer` (push path) | A new hop arriving at this server — a user request entering the microservice, or an outbound RPC dispatched from a caller |
| **DownstreamReturn** | Caller replica via `return_outputs` | A continuation after a downstream RPC completed — resumes call-graph traversal (`advance`) at this server |

Both kinds share one queue and both require a free concurrency slot to be dequeued (`in_flight < max_concurrency`). Only **Upstream** items hold a slot once started (`in_flight += 1`); **DownstreamReturn** runs `advance()` without incrementing `in_flight`.

## Scheduling behavior by policy

| | **Upstream** (new arrivals) | **DownstreamReturn** (continuations) |
|---|---------------------------|--------------------------------------|
| **`fifo` (default)** | Appended to the back (`push_back`). Served in strict arrival order. | Appended to the back (`push_back`). Same FIFO discipline. |
| **`edf`** | Inserted by deadline (earliest first). Uses the request deadline set at user arrival. | Inserted by deadline (earliest first). Uses the same propagated request deadline. |

**Dequeue (both policies):** `drain_queue` always `pop_front`. The scheduling policy only affects **enqueue placement**.

**Insertion rule:** scan from the front; insert before the first queue item whose deadline is strictly greater than the new hop's deadline. Existing items are not moved.

**Tie-breaking:** equal deadlines insert after existing items with the same deadline (FIFO among ties).

## EDF insertion example

Suppose a replica is at capacity and its queue holds:

```
front ──▶  [Upstream  req=1  deadline=120ms]
           [Upstream  req=2  deadline=200ms]
           [Return    req=3  deadline=180ms]
back  ──▶
```

### New upstream arrival

**req=4** arrives with `deadline=150ms`.

| Policy | Resulting queue (front → back) |
|--------|-------------------------------|
| `fifo` | `[req=1] [req=2] [req=3] [req=4]` |
| `edf` | `[req=1] [req=4] [req=2] [req=3]` |

Under EDF, req=4 inserts before req=2 (first item with a strictly later deadline). req=3 stays in place — insertion does not reorder existing items.

### New downstream return

**req=5** returns to the same starting queue with `deadline=150ms`:

| Policy | Resulting queue (front → back) |
|--------|-------------------------------|
| `fifo` | `[req=1] [req=2] [req=3] [req=5]` |
| `edf` | `[req=1] [req=5] [req=2] [req=3]` |

Under EDF, returns use the same insertion rule as upstream arrivals. req=5 inserts before req=2 (deadline 200ms) and req=3 (deadline 180ms).

## Independence from load balancing

Scheduling does not change:

- Which server a load balancer selects (`EdgeBalancer`, `ReplicaBalancer`, `DownstreamBalancer`)
- Shared `DownstreamBalancer` pull queue ordering (always FIFO under `centralized`)

Scheduling only changes the order in which a replica dequeues waiting queue items.

## Interaction with LB policies

| LB policy | EDF effect on replica queues |
|-----------|------------------------------|
| Push (`random`, `power-of-two`, `round-robin`, `least-request`, **`cl`**, **`corr`**) | EDF affects every replica queue when servers are saturated |
| **`centralized`** | EDF affects **ingress** (entry) replica queues. Downstream replicas bypass local queuing via `slot_release`, so EDF has no effect on downstream hops |

## Source files

| File | Role |
|------|------|
| [`src/scheduling.rs`](../src/scheduling.rs) | `SchedulingPolicyKind` enum and EDF insert-index helper |
| [`src/microservice/hop.rs`](../src/microservice/hop.rs) | `deadline` field on `Hop` |
| [`src/microservice/simulate.rs`](../src/microservice/simulate.rs) | Deadline assignment at `UserArrival::inject` |
| [`src/microservice/replica.rs`](../src/microservice/replica.rs) | Queue enqueue logic |
| [`src/bin/ms.rs`](../src/bin/ms.rs) | `--scheduling` CLI flag |
