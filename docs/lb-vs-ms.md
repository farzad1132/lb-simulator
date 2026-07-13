# LB vs MS Feature Comparison

This repo ships two simulators built on the same load-balancing primitives:

| Binary | Purpose | Deep dive |
|--------|---------|-----------|
| **`lb`** | Flat pool of FCFS servers; configurable task arrivals from one or more clients | [lb-simulation.md](lb-simulation.md) |
| **`ms`** | Callgraph of microservices; nested synchronous RPCs with queueing at each replica | [microservice-simulation.md](microservice-simulation.md) |

Both use [`src/policy.rs`](../src/policy.rs) for routing algorithms and [`src/subset.rs`](../src/subset.rs) for server/replica subset assignment.

## Feature matrix

| Feature | lb | ms | Notes |
|---------|:--:|:--:|-------|
| Load-balancing policies | yes | yes | Push: `random`, `power-of-two`, `least-request`, `round-robin`. **Centralized** (`centralized`): lb = global flat pool; ms = per-downstream-target pull layer. **CL** (`cl`) and **Corr** (`corr`) are ms-only shared push layers. |
| Local inflight load view | yes | yes | All push policies use each balancer's **local inflight** counters, not shared backend load |
| Subset routing | yes | yes | `--lb-subset-size`, `--lb-subset-policy` (`deterministic`, `random`). Not supported with `cl`, `centralized`, or `corr` in ms. |
| `--seed`, `--format`, `--verbose` | yes | yes | |
| FCFS queue + concurrency | yes | yes | lb: `--concurrency` per server; ms: `cpu / replicas` per replica |
| Server queue scheduling | — | yes | ms: `--scheduling fifo` (default) or `edf`; see [scheduling.md](scheduling.md) |
| Poisson arrivals | yes | yes | lb: from `--load` (exponential default); ms: per-API `rps` in `load.json` |
| Arrival distribution | yes | — | lb: `--arrival exponential|constant`; ms: exponential only |
| SLO violation rate | yes | yes | lb: optional `--slo` (seconds); ms: `slo_ms` per API in `load.json` |
| Unloaded latency p99 | yes | yes | lb: p99 of service durations; ms: p99 of `processing_time_ms` |
| **Express lane** | yes | — | lb-only; see [expresslane.md](expresslane.md) |
| **Centralized pull dispatch** | yes | yes | lb: one global queue; servers pull on spare capacity. ms: one pull queue per downstream target (outbound only; ingress P2C). See [lb-simulation.md](lb-simulation.md#centralized-policy-pull-based) and [microservice-simulation.md](microservice-simulation.md#centralized-policy-pull-based-layer). |
| **CL centralized-layer outbound** | — | yes | One shared push P2C balancer per downstream microservice target. See [microservice-simulation.md](microservice-simulation.md#cl-policy-centralized-layer). |
| **Corr slack-d outbound** | — | yes | One shared push balancer per downstream target; routes by slack-d CDF buckets to inflight rank. See [microservice-simulation.md](microservice-simulation.md#corr-policy-slack-d-rank-routing). |
| Multiple ingress client LBs | yes | — | `--clients`: independent arrival sources; push policies use one LB per client; centralized uses one shared dispatcher |
| Per-API ingress LB | — | yes | `EdgeBalancer`: one per API, routes user traffic to entry replicas |
| Per-replica outbound LB | — | yes | `ReplicaBalancer`: one per replica (default push policies) |
| Shared downstream outbound LB | — | yes | `DownstreamBalancer`: one per downstream target (`--lb-policy cl`, `centralized`, or `corr`) |
| Flat topology CLI | yes | — | `--servers`, `--concurrency`, `--load` |
| Callgraph topology | — | yes | `--callgraph`, `--load-file` |
| Service distributions | yes | — | `exponential`, `constant`, `bimodal` via `--service-dist` |
| Per-endpoint exponential service times | — | yes | Means from callgraph (`avg_rt` or `exponential.mean`, in ms) |
| Nested synchronous RPCs | — | yes | Multi-hop call trees; siblings dispatched sequentially |
| Direct return routing | — | yes | Callee → caller replica via `CallerRef` (not load-balanced) |
| Request tracing | — | yes | `--trace`, `--trace-limit` (timeline on stderr) |
| Topology scaling | — | yes | `--scale` adds CPU and replicas to every service |
| Load/SLO CLI overrides | — | yes | `--rps`, `--slo-ms` override `load.json` |
| Per-microservice / per-server utilization | — | yes | `microservice_utilization_pct`, `server_utilization_pct` |
| Per-microservice visit metrics | — | yes | `by_microservice`, `total_processing_p99_ms` |
| Processing time metric | — | yes | `processing_time_ms`; queueing = `e2e_ms − processing_time_ms` |
| Split express metrics | yes | — | `regular_*`, `express_*`, pre/post-eviction queueing |

Default `--lb-policy` for **both** binaries is **`power-of-two`**.

## Load balancing (shared behavior)

Policies implement `LoadBalancePolicy::select(&mut self, loads: &[u32]) -> usize` in [`src/policy.rs`](../src/policy.rs). Before `select` runs, all push policies fill the `loads` slice from **local counters** that track requests this balancer has dispatched but not yet received a release for.

| Simulator | Balancer | Local inflight scope |
|-----------|----------|----------------------|
| lb | `LoadBalancer` | Per server in the shared pool |
| ms | `EdgeBalancer` | Per entry-service replica |
| ms | `ReplicaBalancer` | Per downstream-service replica (one table per downstream target) |
| ms | `DownstreamBalancer` | Per downstream microservice target (shared across all callers; `cl`, `centralized`, `corr`) |

These counters do **not** reflect other balancers' traffic, tasks waiting in downstream queues, or in-flight work dispatched by someone else.

**Power-of-two** differs from **least-request** only in selection scope: P2C samples two random backends from the subset and picks the lower local inflight; least-request scans the full subset for the minimum (random tie-break among minima).

### Push policies vs centralized pull

Push policies (`random`, `power-of-two`, `least-request`, `round-robin`) dispatch on arrival via `LoadBalancePolicy::select()`. **Centralized** is a different architecture: tasks queue at a dispatcher and servers pull work when they have spare capacity.

| | lb `centralized` | ms `centralized` |
|---|------------------|------------------|
| Scope | One global flat server pool | One pull queue per downstream microservice target (outbound only) |
| Ingress | Pull-based (same pool) | Push P2C on `EdgeBalancer` (unchanged) |
| Subset | Ignored | Rejected (`--lb-subset-size > 0` not allowed) |

**CL** (`cl`, ms-only) is also an architecture change: outbound RPCs share one `DownstreamBalancer` with push-based power-of-two on aggregate inflight. **Corr** (`corr`, ms-only) uses the same shared topology but maps each request's slack-d CDF percentile to a fixed inflight rank (low / mid / high load replica). Ingress stays per-API `EdgeBalancer` (always P2C for `cl` / `centralized` / `corr`). See [microservice-simulation.md — CL policy](microservice-simulation.md#cl-policy-centralized-layer) and [Corr policy](microservice-simulation.md#corr-policy-slack-d-rank-routing).

### Multiple LBs: ingress vs egress

These are **not** the same feature despite both involving more than one load balancer.

**lb — multiple ingress client LBs (`--clients`):**

```
Client0 → LB0 ─┐
Client1 → LB1 ─┼→ shared server pool
Client2 → LB2 ─┘
```

Each client has its own Poisson source and `LoadBalancer`. All LBs route to the same `--servers` pool. Models multiple independent frontends with partial observability. Subset `client_id` = load balancer index (`0 .. clients-1`).

**ms — per-API ingress + per-replica egress:**

```
User → EdgeBalancer(api) → entry replica
                              │
                              └→ ReplicaBalancer(service/replica) → downstream replica
```

- **EdgeBalancer** (one per API): routes **user ingress** to entry-service replicas. Honors `--lb-policy` for push policies; always P2C for `cl` / `centralized` / `corr`.
- **ReplicaBalancer** (one per replica per service): routes **that replica's outbound RPCs** to downstream services.

A single user request enters through one `EdgeBalancer`. Outbound LBs are tied to replicas making nested calls, not to independent traffic sources.

**ms — CL centralized-layer outbound (`--lb-policy cl`):**

```
User → EdgeBalancer(api) → entry replica
                              │
  Replica(A/0) ──┐
  Replica(A/1) ──┼→ DownstreamBalancer(target=B) → B replicas
  Replica(C/2) ──┘         (shared inflight + P2C push)
```

- **EdgeBalancer** (one per API): unchanged for push policies; always P2C for `cl` / `centralized` / `corr`.
- **DownstreamBalancer** (one per downstream microservice that receives RPCs): all caller replicas share one inflight table and one P2C routing decision per dispatch.
- **OutboundGateway** (one per replica): thin forwarder from a replica's single outbound port to the correct `DownstreamBalancer` (no load state).

Example (chain-3): one `EdgeBalancer` for API `handle`, plus `DownstreamBalancer` for `backend1` and `backend2`.

### Subset assignment

Both simulators call `subset::assign_subset(policy, n, client_id, subset_size)` but use different `client_id` values. **`cl`, `centralized`, and `corr` in ms reject `--lb-subset-size > 0`.**

| Balancer | `client_id` |
|----------|-------------|
| lb `LoadBalancer` | Load balancer index (`0 .. clients-1`) |
| ms `EdgeBalancer` | Sorted API index (APIs ordered lexicographically) |
| ms `ReplicaBalancer` | `replica_idx` within the calling service |

See [lb-simulation.md — Server subset](lb-simulation.md#server-subset) for the deterministic algorithm.

## lb-only features

### Express lane

Overflow path from regular servers to a dedicated express pool when queue depth or queueing delay exceeds a threshold. Use `--express-th`, `--express-del-th`, or both (combined OR eviction). `--ideal` applies to delay-only runs. Not implemented in `ms`.

Flags: `--expresslane`, `--express-size`, `--express-th`, `--express-del-th`, `--ideal`.

Full design: [expresslane.md](expresslane.md).

### Multiple ingress clients

`--clients C` creates C independent arrival sources and C load balancers. Aggregate arrival rate is unchanged (`per_client_arrival_mean = arrival_mean × C`); `--n` is split evenly across clients.

With `--arrival constant`, client `i` (0-based) schedules its first task at `i × arrival_mean`, then every `per_client_arrival_mean` thereafter. Without this phase offsetting, all clients would fire in lockstep every `per_client_arrival_mean`, producing bursts of `C` tasks instead of uniform global spacing of `arrival_mean`. Example with `C=3`, `arrival_mean = 1 s`: client 0 at 0, 3, 6, …; client 1 at 1, 4, 7, …; client 2 at 2, 5, 8, ….

With `--arrival exponential` (default), each client starts at `t=0` and samples `Exp(per_client_arrival_mean)` gaps; randomness desynchronizes clients and no offset is applied.

### Flat topology and service distributions

Configure pool size and load directly: `--servers`, `--concurrency`, `--load`.

Service time sampling: `--service-dist exponential|constant|bimodal` with optional `--service-modes` / `--service-mode-probs`. Default exponential/constant mean is 1 second.

Inter-arrival sampling: `--arrival exponential|constant` (default `exponential`). See [lb-simulation.md](lb-simulation.md#inter-arrival-distribution) for constant-mode multi-client phase offsetting.

### Optional SLO

Pass `--slo` (seconds) to emit `slo_latency` and `prob_latency_gt_slo`. Omit to skip SLO fields entirely.

## ms-only features

### Callgraph and nested RPCs

Topology comes from `callgraph.json` (services, endpoints, edges) and `load.json` (per-API RPS and SLO). Requests traverse the graph as nested synchronous RPCs; sibling calls at an endpoint run **sequentially** in JSON edge order.

### Per-replica outbound balancers

Each replica that makes downstream calls owns a `ReplicaBalancer` with independent local outbound inflight counters and (when subsetting) its own downstream replica subsets. This is the default for push policies (`random`, `power-of-two`, `least-request`, `round-robin`).

### CL centralized-layer outbound

With `--lb-policy cl`, outbound routing uses one shared `DownstreamBalancer` per downstream microservice target (push-based power-of-two on aggregate inflight). Thin `OutboundGateway` models per replica forward calls and releases to the shared balancers. `lb --lb-policy cl` is rejected at startup.

### Centralized pull-based outbound (ms)

With `--lb-policy centralized`, outbound routing uses the same shared topology as `cl`, but each `DownstreamBalancer` is pull-based (FCFS queue; replicas pull on spare capacity). Inflight is released after local service complete at the assigned replica. Ingress stays push P2C on `EdgeBalancer`. `--lb-subset-size > 0` is rejected.

### Corr slack-d outbound

With `--lb-policy corr`, outbound routing uses the same shared topology as `cl`, but each `DownstreamBalancer` maps slack-d to an inflight rank via the empirical CDF: warmup (`|slack_dist| < 200`) and `p < 0.5` → rank 0 (lowest inflight); `0.5 ≤ p < 0.8` → rank 1; `p ≥ 0.8` → rank 2; `sd < 0` (overdue) → rank 5. Ingress stays push P2C on `EdgeBalancer`. `lb --lb-policy corr` is rejected at startup.

### Direct returns

Downstream completions return directly to the **specific caller replica** via `CallerRef`, bypassing load balancers. Continuations may queue at the caller while waiting (`DownstreamReturn` queue items).

### Tracing and scaling

- `--trace` / `--trace-limit`: human-readable per-request timeline on stderr.
- `--scale N`: add N CPU cores and N replicas to every microservice node.

### Metrics shape

- Latencies in **milliseconds** (`e2e_ms`, `processing_time_ms`).
- Per-microservice `microservice_utilization_pct` and per-server `server_utilization_pct`.
- Per-microservice visit samples under `by_microservice`; top-level `total_processing_p99_ms`.
- Per-API stats under `by_api` including always-present SLO fields from `load.json`.

## Metrics and output

| | lb | ms |
|---|----|----|
| Time units | seconds | milliseconds |
| Primary latency fields | `e2e`, `queueing_delays` | `e2e_ms`, `processing_time_ms` |
| Utilization | single `utilization_pct` | per-microservice + per-server maps |
| Visit-level metrics | — | `by_microservice` (inter-arrival, response time, queueing, …) |
| SLO | optional (`--slo`) | required per API in `load.json`; overridable via `--slo-ms` |
| Express split metrics | when `--expresslane` | not available |

## Plot tooling

| Script | Simulator | Purpose |
|--------|-----------|---------|
| [`plot_cdfs.py`](../plot_cdfs.py) | lb, ms | E2E latency CDF (`--simulator lb\|ms`) |
| [`plot_lb_sweep.py`](../plot_lb_sweep.py) | lb | Parameter sweep vs load, clients, servers, etc. (one line per policy at fixed topology) |
| [`plot_lb_centralized_compare.py`](../plot_lb_centralized_compare.py) | lb | Centralized vs power-of-two at equal task/s (lb flat pool; ms `centralized` is per-downstream-target outbound) |
| [`plot_lb_express_heatmap.py`](../plot_lb_express_heatmap.py) | lb | Express lane heatmap (express-size × express-del-th) |
| [`plot_ms_chain_slo_heatmap.py`](../plot_ms_chain_slo_heatmap.py) | ms | SLO violation heatmap for chain topologies |
| [`analyze/ms_service_distributions.py`](../analyze/ms_service_distributions.py) | ms | Per-microservice visit distribution CDFs (see [analyze.md](analyze.md)) |
| [`compare_lb_ms.py`](../compare_lb_ms.py) | both | Overlay CDFs on equivalent single-hop topologies |

## When results should match

A single-hop callgraph (`USER → server:handle`) is equivalent to the flat `lb` simulator when capacity, service-time mean, arrival rate, and LB policy match:

| Concept | ms | lb |
|---------|----|----|
| Total capacity | `cpu` | `servers × concurrency` |
| Per-replica concurrency | `cpu / replicas` | `concurrency` |
| Server count | `replicas` | `servers` |
| Service mean | `avg_rt` in ms (exponential) | default 1.0 s |
| Arrival rate | `load.json` `rps` | derived from `--load` |

Load equivalence (service mean 1 s):

```
rps = load × cpu / service_mean_seconds
```

Fixtures: `tests/client_server/single_replica/` and `tests/client_server/multi_replica/`.

```bash
cargo build --release
python compare_lb_ms.py --scenario all --n 200000
```

Automated check: `cargo test lb_ms_equivalence`.

Multi-hop ms topologies (fan-in, chains, caller queue) have **no** lb equivalent. Express lane has **no** ms equivalent.

## See also

- [lb-simulation.md](lb-simulation.md) — flat simulator design
- [microservice-simulation.md](microservice-simulation.md) — callgraph simulator design
- [expresslane.md](expresslane.md) — express lane mode (lb only)
