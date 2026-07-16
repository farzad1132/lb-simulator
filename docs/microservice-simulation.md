# Microservice Simulation (`ms`)

This document describes how the microservice simulator works: inputs, internal model, request flow, and metrics. The simulator is implemented as a separate binary (`ms`) and module (`src/microservice/`) that does not modify the flat load-balancer simulator (`lb`).

See also: [lb-vs-ms.md](lb-vs-ms.md) for a feature comparison with the flat load-balancer simulator (including which features are shared, lb-only, or ms-only). Per-microservice visit distributions are documented in [analyze.md](analyze.md). Server queue scheduling (`fifo` / `edf`) is documented in [scheduling.md](scheduling.md).

## Vocabulary

| Term | Meaning | Code / JSON |
|------|---------|-------------|
| **Server** | A single server with its own queue and concurrency slots (FIFO by default; see [scheduling.md](scheduling.md)) | Rust type `Replica`; `server_idx`; JSON `server_utilization_pct` |
| **Microservice** | A deployable callgraph component backed by one or more servers | `microservice_id`; callgraph node; JSON `by_microservice`, `microservice_utilization_pct` |
| **Service** | The API-level offering; one service per entry API | Conceptual; 1:1 with an entry API in `load.json` |
| **API** | Named user-facing entry point with independent Poisson traffic | Key in `load.json`; JSON `by_api` |

In a linear chain (chain-3), one API (`handle`) belongs to one **service** and traverses three **microservices** (`frontend → backend1 → backend2`). Metrics under `by_microservice` aggregate across all **servers** of each microservice.

## Overview

The simulator models a microservice application as a directed graph of endpoints. User requests arrive as independent Poisson processes (one per API), enter through a **per-API edge load balancer**, and traverse the callgraph as **nested synchronous RPCs**: each server performs local work, calls downstream microservices sequentially (edge order) via its **own replica load balancer**, waits for each subtree to return, then returns directly to its caller server. Queueing happens at each server.

```
Poisson sources (per API)
        │
        ▼
  UserArrival ──▶ EdgeBalancer(api) ──▶ entry server(s)
        │                                      │
        │                    return (direct)   │
        │                    ◀─────────────────┘
        │                              │
        │              outbound via caller's ReplicaBalancer
        ▼                              ▼
                         downstream server(s) ──▶ … ──▶ stats sink
```

## Input files

### `callgraph.json`

Describes the application topology.

**`nodes`** — one entry per actor:

| Node kind | Fields | Notes |
|-----------|--------|-------|
| `USER` | `interfaces` | Synthetic entry point. Not simulated as a microservice. |
| Microservice (e.g. `frontend`) | `interfaces`, `cpu`, `replicas` | A deployable microservice node. |

Each **interface** (endpoint) on a microservice has a mean local processing time in **milliseconds**, specified as either:

- `"avg_rt": 0.2` — mean 0.2 ms, or
- `"exponential": { "mean": 0.8 }` — mean 0.8 ms

Both forms define the mean of an **exponential** random variable sampled when that endpoint processes a hop (converted to seconds internally for simulation).

**`cpu`** is the total concurrency of the microservice (shared across all interfaces). **`replicas`** is the number of server instances. Each server gets `cpu / replicas` concurrent processing slots.

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
| **EdgeBalancer** | one per API | Push routing to entry replicas; honors `--lb-policy` for push policies; always power-of-two for `cl` / `cl-lr` / `centralized` / `corr` / `approx` |
| **ReplicaBalancer** | one per server (default and `approx` policies) | Outbound only: push dispatch (default policies) or decentralized pull-intent queues (`approx`) |
| **DownstreamBalancer** | one per downstream target (`cl`, `cl-lr`, `centralized`, `corr`) | Shared outbound LB: push P2C (`cl`), push least-request (`cl-lr`), pull FCFS (`centralized`), or experimental push (`corr`) |
| **OutboundGateway** | one per server (`cl`, `cl-lr`, `centralized`, `corr`) | Forwards outbound calls/releases to the correct `DownstreamBalancer` |
| **Replica** (server) | `replicas` per microservice | Configurable queue (`fifo` default, `edf` optional; see [scheduling.md](scheduling.md)), local processing, nested dispatch/return |

### What a microservice models

A callgraph microservice node becomes:

- `replicas` × `Replica` (server) models, each with `max_concurrency = cpu / replicas`
- Default push policies and `approx`: `replicas` × `ReplicaBalancer` models (one outbound LB per server)
- `--lb-policy cl`, `cl-lr`, `centralized`, or `corr`: one `DownstreamBalancer` per downstream microservice target, plus `replicas` × `OutboundGateway` forwarders

All interfaces of a microservice share the same server pool. The queue is per-server, not per-interface.

User ingress is handled separately: one `EdgeBalancer` per API in the callgraph, wired to that API's entry-microservice servers.

### Replica subsetting

When `--lb-subset-size k > 0`, each balancer only routes among `min(k, replicas)` targets. Subset assignment is controlled by `--lb-subset-policy` (default `deterministic`):

- **EdgeBalancer:** client id is the API index (APIs sorted lexicographically).
- **ReplicaBalancer:** client id is `server_idx` within the calling microservice. Each server balancer computes its own downstream subsets independently (for both `deterministic` and `random` policies).

**Not supported with `cl`, `cl-lr`, `centralized`, or `corr`:** `--lb-subset-size > 0` is rejected at startup. All shared-layer policies require all replicas (ingress and outbound).

See [lb-simulation.md](lb-simulation.md#server-subset) for the deterministic algorithm.

### What is NOT modeled

- Network latency between services
- Per-endpoint concurrency (only per-microservice `cpu`)
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
| **Outbound** (child RPC) | Caller replica → **caller's `ReplicaBalancer`** → downstream replica (local outbound inflight) |
| **Return** | Callee replica → **specific caller replica** via `CallerRef` (not load-balanced) |

All push policies use each balancer's local inflight counters at routing time. **Power-of-two** samples two random replicas from the subset and picks the lower local inflight; **least-request** scans the full subset for the minimum.

Example: frontend/0 → backend1 uses **ReplicaBalancer(frontend/0)** to pick a backend1 replica. backend1/0 → shared uses **ReplicaBalancer(backend1/0)** to pick a shared replica.

### EdgeBalancer (ingress) policy

The **EdgeBalancer** handles **user ingress only**. It always uses **push** routing via `LoadBalancePolicy::select()` on local ingress inflight — never pull-based dispatch.

| `--lb-policy` | EdgeBalancer algorithm |
|---------------|------------------------|
| `random`, `power-of-two`, `least-request`, `round-robin` | Honors `--lb-policy` |
| `cl`, `cl-lr`, `centralized`, `corr`, `approx` | Always **power-of-two** (the flag changes outbound architecture only) |

Outbound RPC routing and returns are handled separately (see below).

### CL policy (centralized layer)

`--lb-policy cl` changes **outbound** routing only. Ingress stays one `EdgeBalancer` per API with push-based power-of-two.

For each downstream microservice that receives RPCs, the simulator creates one **DownstreamBalancer** shared by all caller replicas. Dispatch is push-on-arrival (no central queue, no server pull). Routing uses power-of-two on a **shared** inflight table — all outstanding RPCs to that target, from every caller, count toward load.

```
User → EdgeBalancer(handle) → frontend/0
                                │
frontend/0 ──▶ OutboundGateway(frontend/0) ──▶ DownstreamBalancer(backend1) ──▶ backend1/*
backend1/* ──▶ OutboundGateway(backend1/i) ──▶ DownstreamBalancer(backend2) ──▶ backend2/*
```

**Chain-3 example** (`tests/chain/3/`): one `EdgeBalancer`, two `DownstreamBalancer` models (`backend1`, `backend2`), plus one `OutboundGateway` per replica that makes outbound calls.

| vs | Difference |
|----|------------|
| Default push policies | Each `ReplicaBalancer` sees only its own replica's outbound inflight |
| `cl` | All callers to the same downstream target share one inflight view (push P2C) |
| `cl-lr` | Same shared topology as `cl`; downstream uses least-request on shared inflight |
| `corr` | Same shared topology as `cl`; experimental outbound routing |
| `centralized` | All callers share one pull queue per downstream target (pull FCFS); see below |

`lb --lb-policy cl` and `lb --lb-policy cl-lr` are rejected at startup.

### CL-LR policy (shared least-request outbound)

`--lb-policy cl-lr` uses the same shared outbound topology as `cl` (`DownstreamBalancer` + `OutboundGateway` per downstream target). Ingress stays push-based power-of-two on `EdgeBalancer` (same as `cl`).

The only difference from `cl` is downstream routing: `DownstreamBalancer` uses **least-request** on the shared inflight table (full scan for minimum load; random tie-break among minima) instead of power-of-two.

| vs | Difference |
|----|------------|
| `cl` | Push-on-arrival P2C on shared inflight |
| `cl-lr` | Push-on-arrival least-request on shared inflight |

### Centralized policy (pull-based layer)

`--lb-policy centralized` uses the same shared outbound topology as `cl` (`DownstreamBalancer` + `OutboundGateway` per downstream target), but dispatch is **pull-based** like lb `centralized`: outbound calls queue at the shared balancer; downstream replicas pull when they have spare capacity; FCFS matching (no `select()`).

Ingress stays push-based power-of-two on `EdgeBalancer` (same as `cl`).

```
User → EdgeBalancer(handle) → frontend/0
                                │
frontend/0 ──▶ OutboundGateway(frontend/0) ──▶ DownstreamBalancer(backend1) ──pull──▶ backend1/*
backend1/* ──▶ OutboundGateway(backend1/i) ──▶ DownstreamBalancer(backend2) ──pull──▶ backend2/*
```

| vs | Difference |
|----|------------|
| `cl` | Push-on-arrival P2C; inflight released on return to caller |
| `centralized` | Pull FCFS queue; inflight released after local service complete at assigned replica (before nested child dispatch) |
| lb `centralized` | One global flat pool; ms uses one pull queue **per downstream target** |

`--lb-subset-size > 0` is not supported with `cl`, `cl-lr`, `centralized`, or `corr`.

### Approx policy (decentralized outbound pull)

Per-caller-replica outbound pull with `--pull-policy`, intent binding, and the same `in_flight` / `pending_pulls` concurrency model as `lb` approx. Ingress stays push P2C on `EdgeBalancer`. Outbound pulls are **bound** by `request_id` by default; optional **`--no-bind`** pops the queue head (FCFS by default, or EDF with **`--approx-sched edf`**) per `(rb_id, target)` — see [approx-policy.md § No-bind mode](approx-policy.md#no-bind-mode---no-bind).

Full documentation: **[approx-policy.md](approx-policy.md)**.

### Corr policy (experimental)

`--lb-policy corr` uses the same shared outbound topology as `cl` (`DownstreamBalancer` + `OutboundGateway` per downstream target). Ingress stays push-based power-of-two on `EdgeBalancer`. Outbound routing is experimental and subject to change. `lb --lb-policy corr` is rejected at startup.

### Replica queue

Each replica has one queue holding upstream arrivals and downstream returns. Default discipline is FIFO; EDF (`--scheduling edf`) reorders all queue items by deadline on enqueue. See [scheduling.md](scheduling.md) for queue behavior under each policy.

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

Default push policies — for each (service, replica_idx):
    ReplicaBalancer(service, replica_idx).outbound ──▶ downstream Replica[j].input
    Replica(service, replica_idx) ──▶ own ReplicaBalancer.outbound
    Replica(service, replica_idx).outbound_release ──▶ own ReplicaBalancer.release_outbound

CL / centralized policy — for each downstream target T:
    DownstreamBalancer(T).outbound ──▶ Replica[j].input   (all replicas j)

CL / centralized policy — for each (service, replica_idx):
    OutboundGateway(service, replica_idx) ──▶ DownstreamBalancer(T) per reachable T
    Replica(service, replica_idx) ──▶ own OutboundGateway.input
    Replica(service, replica_idx).outbound_release ──▶ own OutboundGateway.release

Centralized only — for each downstream target replica j:
    Replica(T, j).pull ──▶ DownstreamBalancer(T).pull

Common:
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
| **microservice_utilization_pct** | `busy_time[ms] / (observation_time × cpu[ms]) × 100` |

`busy_time[ms]` is the sum of all sampled local hop durations executed on any server of microservice `ms`. Visiting the same microservice twice in one request contributes twice.

### Per server

| Metric | Definition |
|--------|------------|
| **server_utilization_pct** | `busy_time[ms][s] / (observation_time × (cpu[ms] / replicas[ms])) × 100` |
| **server_avg_queue_inflight** | Time-weighted average of `queue.len() + in_flight` per server. Under `--lb-policy centralized`, downstream pull targets also add `DownstreamBalancer.queue.len() / replicas[ms]` as an equal fair-share per server (work waiting at the shared pull queue before dispatch). |

`busy_time[ms][s]` is the sum of local hop durations executed on server `s` of microservice `ms`. Per-server utilization uses that server's concurrency slots (`cpu / replicas`) as capacity. When all servers have equal capacity, the microservice-level overall utilization equals the average of per-server utilizations.

### Per-microservice visit metrics

Recorded per visit (one per microservice per request on the request path) and exported under `by_microservice`. See [analyze.md](analyze.md) for definitions, normalization, and plotting.

| Field | Definition |
|-------|------------|
| **inter_arrival_ms** | Consecutive gaps between visit arrival timestamps (all servers merged) |
| **inter_departure_ms** | Consecutive gaps between visit departure timestamps |
| **response_time_ms** | `departure − arrival` (includes queueing, processing, downstream blocking) |
| **queueing_delay_ms** | `(response_time − Σ downstream dependency response times) − processing_time`; includes replica server queue and caller outbound-queue wait (pull policies) |
| **cumulative_queueing_delay_ms** | Running sum of `queueing_delay_ms` along the request path in `microservice_order` through the current hop |
| **processing_time_ms** | Sum of local hop durations at that microservice only |
| **slack_d_ms** | `deadline − arrival` at visit arrival (deadline set once at user ingress) |
| **prob_latency_gt_slo** | Fraction of visits with `departure > deadline` (equivalently `response_time_ms > slack_d_ms`) |

Top-level **total_processing_p99_ms** is the p99 of per-request total local processing time across the full call tree (same scale used to normalize response and queueing CDFs in analyze scripts).

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
| `--lb-policy` | Load-balancing policy: `random`, `power-of-two` (default), `least-request`, `round-robin`, `approx` (decentralized outbound pull; requires `--pull-policy`), `cl` (shared push P2C outbound), `cl-lr` (shared push least-request outbound), `centralized` (shared pull FCFS outbound), or `corr` (experimental shared push outbound). For `cl` / `cl-lr` / `centralized` / `corr` / `approx`, ingress stays push P2C on `EdgeBalancer`. |
| `--pull-policy` | Pull-intent server selection for `approx` (`random`, `power-of-two`, `least-request`, `round-robin`); **required** with `--lb-policy approx` |
| `--no-bind` | With `approx`: fulfill outbound pulls by popping queue head (ignore `pull.request_id`); FCFS by default |
| `--approx-sched` | Outbound approx queue discipline with `--no-bind`: `fifo` (default) or `edf`; independent of `--scheduling` |
| `--scheduling` | Server queue discipline at each replica: `fifo` (default) or `edf`; see [scheduling.md](scheduling.md) |
| `--force-fixed-svc` | Use fixed service times from callgraph instead of sampling |
| `--lb-subset-size` | Replica subset per balancer (`0` = all). Not supported with `cl`, `cl-lr`, `centralized`, or `corr`. |
| `--lb-subset-policy` | Subset assignment policy: `deterministic` (default) or `random` |
| `--seed` | Optional RNG seed for reproducible runs |
| `--format` | `human` or `json` |
| `--trace` | Emit a human-readable request-flow timeline on stderr |
| `--trace-limit` | Number of user requests to trace (default `5`; only applies with `--trace`) |
| `--scale` | Add this many cores and replicas to every microservice (default `0`) |

### Tracing

Use `--trace` to print a per-request timeline on **stderr** while stats still go to stdout. Each line shows simulation time, request id, entity, and action:

```
[t=0.000449s] req=1 UserArrival api=f1 entry=frontend:f1
[t=0.000449s] req=1 EdgeBalancer(api=f1) -> server=0
[t=0.000449s] req=1 Server(frontend/0) enqueue upstream endpoint=frontend:f1 queue=1 inflight=0
[t=0.000449s] req=1 Server(frontend/0) serve start endpoint=frontend:f1
[t=0.000480s] req=1 Server(frontend/0) serve done endpoint=frontend:f1 duration_ms=0.031
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
  "microservice_utilization_pct": {
    "frontend": 2.02,
    "backend1": 9.41,
    "backend2": 2.54,
    "shared": 7.46
  },
  "server_utilization_pct": {
    "frontend": { "0": 2.50, "1": 1.54 },
    "backend1": { "0": 10.2, "1": 9.1, "2": 8.9 },
    "backend2": { "0": 2.80, "1": 2.28 },
    "shared": { "0": 8.1, "1": 7.2, "2": 7.0, "3": 6.5 }
  },
  "server_avg_queue_inflight": {
    "frontend": { "0": 0.12, "1": 0.09 },
    "backend1": { "0": 1.4, "1": 1.2, "2": 1.1 },
    "backend2": { "0": 0.35, "1": 0.31 },
    "shared": { "0": 0.9, "1": 0.8, "2": 0.7, "3": 0.6 }
  },
  "by_api": {
    "f1": {
      "e2e_ms": [4.2, 5.1],
      "processing_time_ms": [3.0, 4.1],
      "unloaded_latency_p99_ms": 11.6,
      "slo_latency_ms": 58.0,
      "prob_latency_gt_slo": 0.012
    }
  },
  "by_microservice": {
    "frontend": {
      "inter_arrival_ms": [0.5, 0.3],
      "inter_departure_ms": [0.6, 0.4],
      "response_time_ms": [4.2, 5.1],
      "queueing_delay_ms": [1.2, 1.0],
      "processing_time_ms": [0.5, 0.4],
      "slack_d_ms": [54.0, 53.1],
      "prob_latency_gt_slo": 0.008
    }
  },
  "total_processing_p99_ms": 11.6
}
```

Human output lists per-server utilization indented under each microservice.

To plot e2e latency CDFs from `plot_cdfs.py`, see [Plot microservice e2e CDF](../README.md#plot-microservice-e2e-cdf) in the README. For per-microservice visit distributions, see [analyze.md](analyze.md).
