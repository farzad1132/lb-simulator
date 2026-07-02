# LB vs MS Feature Comparison

This repo ships two simulators built on the same load-balancing primitives:

| Binary | Purpose | Deep dive |
|--------|---------|-----------|
| **`lb`** | Flat pool of FCFS servers; Poisson task arrivals from one or more clients | [lb-simulation.md](lb-simulation.md) |
| **`ms`** | Callgraph of microservices; nested synchronous RPCs with queueing at each replica | [microservice-simulation.md](microservice-simulation.md) |

Both use [`src/policy.rs`](../src/policy.rs) for routing algorithms and [`src/subset.rs`](../src/subset.rs) for server/replica subset assignment.

## Feature matrix

| Feature | lb | ms | Notes |
|---------|:--:|:--:|-------|
| Load-balancing policies | yes | yes | `random`, `power-of-two`, `least-request`, `round-robin` |
| Power-of-two true load | yes | yes | Reads shared `LoadRegistry`: `in_flight + queue.len()` at routing time |
| Least-request / random / round-robin | yes | yes | Use each balancer's **local inflight** counters, not true load |
| Subset routing | yes | yes | `--lb-subset-size`, `--lb-subset-policy` (`deterministic`, `random`) |
| `--seed`, `--format`, `--verbose` | yes | yes | |
| FCFS queue + concurrency | yes | yes | lb: `--concurrency` per server; ms: `cpu / replicas` per replica |
| Poisson arrivals | yes | yes | lb: from `--load`; ms: per-API `rps` in `load.json` |
| SLO violation rate | yes | yes | lb: optional `--slo` (seconds); ms: `slo_ms` per API in `load.json` |
| Unloaded latency p99 | yes | yes | lb: p99 of service durations; ms: p99 of `processing_time_ms` |
| **Express lane** | yes | — | lb-only; see [expresslane.md](expresslane.md) |
| Multiple ingress client LBs | yes | — | `--clients`: independent Poisson sources, each with its own LB |
| Per-API ingress LB | — | yes | `EdgeBalancer`: one per API, routes user traffic to entry replicas |
| Per-replica outbound LB | — | yes | `ReplicaBalancer`: one per replica, routes downstream RPCs |
| Flat topology CLI | yes | — | `--servers`, `--concurrency`, `--load` |
| Callgraph topology | — | yes | `--callgraph`, `--load-file` |
| Service distributions | yes | — | `exponential`, `constant`, `bimodal` via `--service-dist` |
| Per-endpoint exponential service times | — | yes | Means from callgraph (`avg_rt` or `exponential.mean`, in ms) |
| Nested synchronous RPCs | — | yes | Multi-hop call trees; siblings dispatched sequentially |
| Direct return routing | — | yes | Callee → caller replica via `CallerRef` (not load-balanced) |
| Request tracing | — | yes | `--trace`, `--trace-limit` (timeline on stderr) |
| Topology scaling | — | yes | `--scale` adds CPU and replicas to every service |
| Load/SLO CLI overrides | — | yes | `--rps`, `--slo-ms` override `load.json` |
| Per-service / per-replica utilization | — | yes | `utilization_pct`, `replica_utilization_pct` |
| Processing time metric | — | yes | `processing_time_ms`; queueing = `e2e_ms − processing_time_ms` |
| Split express metrics | yes | — | `regular_*`, `express_*`, pre/post-eviction queueing |

Default `--lb-policy` for **both** binaries is **`power-of-two`**.

## Load balancing (shared behavior)

Policies implement `LoadBalancePolicy::select(&mut self, loads: &[u32]) -> usize` in [`src/policy.rs`](../src/policy.rs). What differs is **which values fill the `loads` slice** before `select` runs.

### Power-of-two: true load in both simulators

Each server/replica publishes load to a shared [`LoadRegistry`](../src/load_registry.rs) on every queue or concurrency change:

```
true_load = in_flight + queue.len()
```

When `--lb-policy power-of-two` is active, balancers read these published values at routing time:

- **lb:** [`LoadBalancer::input`](../src/load_balancer.rs) reads `load_registry.get(server_idx)` for each server in the subset.
- **ms:** [`EdgeBalancer::input`](../src/microservice/balancer.rs) and [`ReplicaBalancer::outbound`](../src/microservice/balancer.rs) read the target service's registry for each replica in the subset.

The routed request is **not** counted in true load until the server/replica receives it. Local inflight is incremented separately on dispatch and decremented on release.

### Other policies: local inflight only

For `least-request`, `random`, and `round-robin`, balancers fill the load slice from **local counters** that track requests this balancer has dispatched but not yet received a release for:

| Simulator | Balancer | Local inflight scope |
|-----------|----------|----------------------|
| lb | `LoadBalancer` | Per server in the shared pool |
| ms | `EdgeBalancer` | Per entry-service replica |
| ms | `ReplicaBalancer` | Per downstream-service replica (one table per downstream target) |

These counters do **not** reflect other balancers' traffic, tasks waiting in downstream queues, or in-flight work dispatched by someone else.

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

- **EdgeBalancer** (one per API): routes **user ingress** to entry-service replicas.
- **ReplicaBalancer** (one per replica per service): routes **that replica's outbound RPCs** to downstream services.

A single user request enters through one `EdgeBalancer`. Outbound LBs are tied to replicas making nested calls, not to independent traffic sources.

### Subset assignment

Both simulators call `subset::assign_subset(policy, n, client_id, subset_size)` but use different `client_id` values:

| Balancer | `client_id` |
|----------|-------------|
| lb `LoadBalancer` | Load balancer index (`0 .. clients-1`) |
| ms `EdgeBalancer` | Sorted API index (APIs ordered lexicographically) |
| ms `ReplicaBalancer` | `replica_idx` within the calling service |

See [lb-simulation.md — Server subset](lb-simulation.md#server-subset) for the deterministic algorithm.

## lb-only features

### Express lane

Overflow path from regular servers to a dedicated express pool when queue depth or queueing delay exceeds a threshold. Not implemented in `ms`.

Flags: `--expresslane`, `--express-size`, `--express-th`, `--express-del-th`, `--ideal`.

Full design: [expresslane.md](expresslane.md).

### Multiple ingress clients

`--clients C` creates C independent Poisson sources and C load balancers. Aggregate arrival rate is unchanged (`per_client_arrival_mean = arrival_mean × C`); `--n` is split evenly across clients.

### Flat topology and service distributions

Configure pool size and load directly: `--servers`, `--concurrency`, `--load`.

Service time sampling: `--service-dist exponential|constant|bimodal` with optional `--service-modes` / `--service-mode-probs`. Default exponential/constant mean is 1 second.

### Optional SLO

Pass `--slo` (seconds) to emit `slo_latency` and `prob_latency_gt_slo`. Omit to skip SLO fields entirely.

## ms-only features

### Callgraph and nested RPCs

Topology comes from `callgraph.json` (services, endpoints, edges) and `load.json` (per-API RPS and SLO). Requests traverse the graph as nested synchronous RPCs; sibling calls at an endpoint run **sequentially** in JSON edge order.

### Per-replica outbound balancers

Each replica that makes downstream calls owns a `ReplicaBalancer` with independent local outbound inflight counters and (when subsetting) its own downstream replica subsets.

### Direct returns

Downstream completions return directly to the **specific caller replica** via `CallerRef`, bypassing load balancers. Continuations may queue at the caller while waiting (`DownstreamReturn` queue items).

### Tracing and scaling

- `--trace` / `--trace-limit`: human-readable per-request timeline on stderr.
- `--scale N`: add N CPU cores and N replicas to every microservice node.

### Metrics shape

- Latencies in **milliseconds** (`e2e_ms`, `processing_time_ms`).
- Per-service `utilization_pct` and per-replica `replica_utilization_pct`.
- Per-API stats under `by_api` including always-present SLO fields from `load.json`.

## Metrics and output

| | lb | ms |
|---|----|----|
| Time units | seconds | milliseconds |
| Primary latency fields | `e2e`, `queueing_delays` | `e2e_ms`, `processing_time_ms` |
| Utilization | single `utilization_pct` | per-service + per-replica maps |
| SLO | optional (`--slo`) | required per API in `load.json`; overridable via `--slo-ms` |
| Express split metrics | when `--expresslane` | not available |

## Plot tooling

| Script | Simulator | Purpose |
|--------|-----------|---------|
| [`plot_cdfs.py`](../plot_cdfs.py) | lb, ms | E2E latency CDF (`--simulator lb\|ms`) |
| [`plot_lb_sweep.py`](../plot_lb_sweep.py) | lb | Parameter sweep vs load, clients, servers, etc. |
| [`plot_lb_express_heatmap.py`](../plot_lb_express_heatmap.py) | lb | Express lane heatmap (express-size × express-del-th) |
| [`plot_ms_chain_slo_heatmap.py`](../plot_ms_chain_slo_heatmap.py) | ms | SLO violation heatmap for chain topologies |
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
