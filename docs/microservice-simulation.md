# Microservice Simulation (`ms`)

This document describes how the microservice simulator works: inputs, internal model, request flow, and metrics. The simulator is implemented as a separate binary (`ms`) and module (`src/microservice/`) that does not modify the flat load-balancer simulator (`lb`).

See also: [lb-vs-ms.md](lb-vs-ms.md) for a feature comparison with the flat load-balancer simulator (including which features are shared, lb-only, or ms-only).

## Overview

The simulator models a microservice application as a directed graph of endpoints. User requests arrive as independent Poisson processes (one per API), enter through a **per-API edge load balancer**, and traverse the callgraph as **nested synchronous RPCs**: each replica performs local work, calls downstream services sequentially (edge order) via its **own replica load balancer**, waits for each subtree to return, then returns directly to its caller replica. Queueing happens at each replica.

```
Poisson sources (per API)
        │
        ▼
  UserArrival ──▶ EdgeBalancer(api) ──▶ entry Replica(s)
        │                                      │
        │                    return (direct)   │
        │                    ◀─────────────────┘
        │                              │
        │              outbound via caller's ReplicaBalancer
        ▼                              ▼
                         downstream Replica(s) ──▶ … ──▶ stats sink
```

## Input files

### `callgraph.json`

Describes the application topology.

**`nodes`** — one entry per actor:

| Node kind | Fields | Notes |
|-----------|--------|-------|
| `USER` | `interfaces` | Synthetic entry point. Not simulated as a service. |
| Service (e.g. `frontend`) | `interfaces`, `cpu`, `replicas` | A deployable microservice. |

Each **interface** (endpoint) on a service has a mean local processing time in **milliseconds**, specified as either:

- `"avg_rt": 0.2` — mean 0.2 ms, or
- `"exponential": { "mean": 0.8 }` — mean 0.8 ms

Both forms define the mean of an **exponential** random variable sampled when that endpoint processes a hop (converted to seconds internally for simulation).

**`cpu`** is the total concurrency of the microservice (shared across all interfaces). **`replicas`** is the number of replica instances. Each replica gets `cpu / replicas` concurrent processing slots.

**`edges`** — directed calls between endpoints:

```json
{ "source": "frontend:f1", "target": "backend1:f2", "api": "f1" }
```

- `source` / `target` — `"<service>:<interface>"` or `"USER"`.
- `api` (optional) — tags which root API this edge belongs to. Omitted on `USER → …` entry edges.

**Entry APIs:** each `USER → <service>:<interface>` edge defines an API named after the interface suffix (e.g. `frontend:f1` → API `"f1"`).

**Edge filtering:** when simulating API `"f1"`, only follow edges whose `api` field equals `"f1"`.

### `load.json`

Maps API name to request rate and SLO latency:

```json
{
    "f1": { "rps": 2000, "slo_ms": 58.0 },
    "g1": { "rps": 500, "slo_ms": 49.0 }
}
```

| Field | Unit | Description |
|-------|------|-------------|
| **rps** | requests/s | Poisson arrival rate for this API |
| **slo_ms** | ms | SLO latency threshold (reported as `slo_latency_ms` in output) |

Every key must match an entry API from the callgraph. Each API gets an independent Poisson arrival process at the given RPS.

## Callgraph navigation

At runtime the simulator uses the **`children`** map (edges grouped by source endpoint, filtered by API). There is no flat precomputed path. Sibling edges are visited **sequentially** in JSON array order; each child call is a nested RPC that must return before the next sibling is dispatched.

**Example** (fanin fixture, API `"f1"`):

```
frontend:f1
  ├─► backend1:f2 ─► shared:f5 ─► (return) ─► (return)
  └─► backend2:f4 ─► shared:f5 ─► (return) ─► (return) ─► CompletedRequest
```

**Example** (API `"g1"`):

```
frontend:g1 ─► backend1:f3 ─► (return) ─► CompletedRequest
```

## Simulation entities

| Entity | Count | Role |
|--------|-------|------|
| **Poisson source** | one per API | Generates user requests at RPS from `load.json` |
| **UserArrival** | 1 | Creates initial `Hop` and injects into the API's edge balancer |
| **EdgeBalancer** | one per API | Picks an entry-service replica for user traffic (true load for power-of-two; local inflight otherwise) |
| **ReplicaBalancer** | one per replica | Outbound only: picks downstream replicas (true load for power-of-two; local inflight otherwise) |
| **Replica** | `replicas` per microservice | Strict FIFO queue, local processing, nested dispatch/return |

### What a microservice models

A callgraph service node becomes:

- `replicas` × `Replica` models, each with `max_concurrency = cpu / replicas`
- `replicas` × `ReplicaBalancer` models (one outbound LB per replica)

All interfaces of a service share the same replica pool. The queue is per-replica, not per-interface.

User ingress is handled separately: one `EdgeBalancer` per API in the callgraph, wired to that API's entry-service replicas.

### Replica subsetting

When `--lb-subset-size k > 0`, each balancer only routes among `min(k, replicas)` targets. Subset assignment is controlled by `--lb-subset-policy` (default `deterministic`):

- **EdgeBalancer:** client id is the API index (APIs sorted lexicographically).
- **ReplicaBalancer:** client id is `replica_idx` within the calling service. Each replica balancer computes its own downstream subsets independently (for both `deterministic` and `random` policies).

See [lb-simulation.md](lb-simulation.md#server-subset) for the deterministic algorithm.

### What is NOT modeled

- Network latency between services
- Per-endpoint concurrency (only per-service `cpu`)
- Parallel fan-out / fork-join among siblings (siblings are sequential)
- Retries, failures, or timeouts

## Request lifecycle

### One user request = one `Hop`

| Field | Set when | Changes? |
|-------|----------|----------|
| `api` | User arrival | Never |
| `endpoint` | Arrival / return | Current endpoint for local work or continuation |
| `sibling_index` | After local work or child return | Next child edge to dispatch at this endpoint |
| `start` | User arrival | Never — used for e2e latency |
| `duration` | Each local processing step | Re-sampled exponential for current endpoint |
| `processing_time` | Each local completion | Running sum of local durations |
| `caller` | Outbound dispatch | `CallerRef` for return routing |

### Nested call/return flow (API `f1`, one request)

1. Request enters **frontend** via `EdgeBalancer(api=f1)`
2. Local processing at **frontend** (`frontend:f1`)
3. **frontend/0's ReplicaBalancer** picks a **backend1 replica**; async wait begins
4. Local processing at **backend1** (`backend1:f2`)
5. **backend1/0's ReplicaBalancer** picks a **shared replica**
6. Local processing at **shared** (`shared:f5`)
7. **shared** returns directly to **backend1** → backend1 FIFO queue
8. **backend1** returns directly to **frontend** → frontend FIFO queue
9. **frontend/0's ReplicaBalancer** picks a **backend2 replica** (next sibling)
10. … second subtree …
11. **frontend** emits `CompletedRequest` and releases the edge balancer slot

While waiting on a downstream RPC, a replica may process other queue items (new requests or other continuations).

### Load balancing

| Direction | Path |
|-----------|------|
| **User ingress** | `UserArrival` → `EdgeBalancer(api)` → entry-service replica |
| **Outbound** (child RPC) | Caller replica → **caller's `ReplicaBalancer`** → downstream replica (true load for power-of-two; local inflight otherwise) |
| **Return** | Callee replica → **specific caller replica** via `CallerRef` (not load-balanced) |

Each replica publishes true load (`in_flight + queue.len()`) to a shared `LoadRegistry` per service. **Power-of-two** balancers read this at routing time; **least-request** and other policies still use each balancer's local outbound inflight counters.

Example: frontend/0 → backend1 uses **ReplicaBalancer(frontend/0)** to pick a backend1 replica. backend1/0 → shared uses **ReplicaBalancer(backend1/0)** to pick a shared replica.

### Replica FIFO queue

Each replica has one strict FIFO queue holding:

| Kind | Handler |
|------|---------|
| **Upstream** | Sample duration → local processing (holds concurrency slot) |
| **DownstreamReturn** | Immediate `advance()` — dispatch next sibling, return up, or complete (no slot held) |

**All items** require a free concurrency slot (`in_flight < max_concurrency`) to be dequeued. Dispatch-only returns do **not** increment `in_flight` once dequeued.

After local processing completes, the first child call runs synchronously in the completion handler (same worker just freed). Subsequent continuations from downstream returns are separate queue items.

### Continuation logic (`advance`)

After local work at endpoint `E`:

1. `sibling_index = 0`
2. If `sibling_index < len(children(E))`: dispatch `children[sibling_index]` via own `ReplicaBalancer`
3. Else if `caller` set: return hop to caller replica (restore endpoint / sibling_index from `CallerRef`)
4. Else: emit `CompletedRequest`

On **DownstreamReturn** dequeued: run `advance()` with restored state (next sibling, return up, or complete).

## Model wiring

```
For each API in callgraph entrypoints:
    Poisson ──▶ UserArrival ──▶ EdgeBalancer(api) ──▶ entry Replica[i]

For each (service, replica_idx):
    ReplicaBalancer(service, replica_idx).outbound ──▶ downstream Replica[j].input
    Replica(service, replica_idx) ──▶ own ReplicaBalancer.outbound
    Replica(service, replica_idx).outbound_release ──▶ own ReplicaBalancer.release_outbound
    Replica(entry, i).edge_release[api] ──▶ EdgeBalancer(api).release
    Replica[*].completed ──▶ stats sink
    return_outputs[(S,j)] ──▶ Replica[j].input     (DownstreamReturn, direct)
```

## Metrics

All latency values in **output** are in **milliseconds**, grouped **per API** under `by_api`.

### Per request (per API)

| Metric | Definition |
|--------|------------|
| **e2e_ms** | `finish − start` in ms — wall-clock from user arrival to final completion (includes all queueing) |
| **processing_time_ms** | Sum of sampled local durations across the nested call tree in ms (excludes queueing) |

Queueing delay per request = `e2e_ms − processing_time_ms` (derivable, not a primary output).

### SLO (per API)

| Metric | Definition |
|--------|------------|
| **unloaded_latency_p99_ms** | p99 of that API's `processing_time_ms` samples |
| **slo_latency_ms** | `slo_ms` from `load.json` for that API |
| **prob_latency_gt_slo** | Fraction of requests with `e2e_ms > slo_latency_ms` |

### Per microservice

| Metric | Definition |
|--------|------------|
| **Utilization** | `busy_time[s] / (observation_time × cpu[s]) × 100` |

`busy_time[s]` is the sum of all sampled local hop durations executed on any replica of service `s`. Visiting the same service twice in one request contributes twice.

### Per replica

| Metric | Definition |
|--------|------------|
| **Utilization** | `busy_time[s][r] / (observation_time × (cpu[s] / replicas[s])) × 100` |

`busy_time[s][r]` is the sum of local hop durations executed on replica `r` of service `s`. Per-replica utilization uses that replica's concurrency slots (`cpu / replicas`) as capacity. When all replicas have equal capacity, the service-level overall utilization equals the average of per-replica utilizations.

## Validation against `lb`

A single-hop callgraph (`USER → server:handle`) is equivalent to the flat `lb` simulator when capacity, service-time mean, arrival rate, and LB policy match.

| Concept | `ms` | `lb` |
|---------|------|------|
| Total capacity | `cpu` | `servers × concurrency` |
| Per-replica concurrency | `cpu / replicas` | `concurrency` |
| Server count | `replicas` | `servers` |
| Service mean | `avg_rt` in ms (exponential) | default 1.0 s |
| Arrival rate | `load.json` `rps` | derived from `--load` |

Load equivalence (with service mean 1 s):

```
rps = load × cpu / service_mean_seconds
```

Fixtures live under `tests/client_server/single_replica/` and `tests/client_server/multi_replica/`. Use the same `--lb-policy` on both simulators (defaults differ).

Run the comparison harness:

```bash
cargo build --release
python compare_lb_ms.py --scenario all --n 200000
```

Automated check: `cargo test lb_ms_equivalence`.

Nested multi-hop metrics (`f1`, etc.) differ from the old flat-path simulator by design. Express lane mode is not available in `ms`; see [lb-vs-ms.md](lb-vs-ms.md).

## CLI and output

```bash
cargo build --release
./target/release/ms \
  --callgraph tests/fanin/single/callgraph.json \
  --load-file tests/fanin/single/load.json \
  --format json \
  --n 100000
```

| Flag | Description |
|------|-------------|
| `--callgraph` | Path to callgraph JSON (required) |
| `--load-file` | Path to per-API load JSON (`rps` + `slo_ms`) (required) |
| `--n` | Total requests, split across APIs proportional to RPS |
| `--lb-policy` | Load-balancing policy: `random`, `power-of-two` (default), `least-request`, or `round-robin` |
| `--lb-subset-size` | Replica subset per balancer (`0` = all) |
| `--lb-subset-policy` | Subset assignment policy: `deterministic` (default) or `random` |
| `--seed` | Optional RNG seed for reproducible runs (uses single-threaded simulation) |
| `--format` | `human` or `json` |
| `--trace` | Emit a human-readable request-flow timeline on stderr |
| `--trace-limit` | Number of user requests to trace (default `5`; only applies with `--trace`) |
| `--scale` | Add this many cores and replicas to every microservice (default `0`) |

### Tracing

Use `--trace` to print a per-request timeline on **stderr** while stats still go to stdout. Each line shows simulation time, request id, entity, and action:

```
[t=0.000449s] req=1 UserArrival api=f1 entry=frontend:f1
[t=0.000449s] req=1 EdgeBalancer(api=f1) -> replica=0
[t=0.000449s] req=1 Replica(frontend/0) enqueue upstream endpoint=frontend:f1 queue=1 inflight=0
[t=0.000449s] req=1 Replica(frontend/0) serve start endpoint=frontend:f1
[t=0.000480s] req=1 Replica(frontend/0) serve done endpoint=frontend:f1 duration_ms=0.031
...
[t=0.004599s] req=1 UserArrival complete api=f1 e2e_ms=4.15 proc_ms=4.15
```

Only the first `--trace-limit` user arrivals are traced. Keep this small when `--n` is large.

```bash
./target/release/ms \
  --callgraph tests/fanin/single/callgraph.json \
  --load-file tests/fanin/single/load.json \
  --n 200 --seed 7 \
  --trace --trace-limit 1 \
  2> trace.log
```

**JSON output:**

```json
{
  "utilization_pct": {
    "frontend": 2.02,
    "backend1": 9.41,
    "backend2": 2.54,
    "shared": 7.46
  },
  "replica_utilization_pct": {
    "frontend": { "0": 2.50, "1": 1.54 },
    "backend1": { "0": 10.2, "1": 9.1, "2": 8.9 },
    "backend2": { "0": 2.80, "1": 2.28 },
    "shared": { "0": 8.1, "1": 7.2, "2": 7.0, "3": 6.5 }
  },
  "by_api": {
    "f1": {
      "e2e_ms": [4.2, 5.1],
      "processing_time_ms": [3.0, 4.1],
      "unloaded_latency_p99_ms": 11.6,
      "slo_latency_ms": 58.0,
      "prob_latency_gt_slo": 0.012
    }
  }
}
```

Human output lists per-replica utilization indented under each microservice.

To plot e2e latency CDFs from `plot_cdfs.py`, see [Plot microservice e2e CDF](../README.md#plot-microservice-e2e-cdf) in the README.
