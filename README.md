# lb

Multi-server FCFS queue simulator with pluggable load-balancing policies. Tasks arrive from one or more clients with exponential (Poisson, default) or constant inter-arrival times (`--arrival`), receive exponential, constant, or bimodal (mixture-of-exponentials) service times, and are routed by each client's load balancer to a shared pool of servers. Each server has its own FIFO queue and can process multiple tasks concurrently (simulating CPU cores).

## Architecture

```
exp_source_0 → LoadBalancer_0 ─┐
exp_source_1 → LoadBalancer_1 ─┼→ Server_0 ─┐
...                            → Server_1 ─┼→ shared output sink
exp_source_C → LoadBalancer_C ─→ Server_N ─┘
         ▲                           │
         └──────── release ──────────┘
```

With `--clients 1`, this reduces to a single client → load balancer → servers path.

See [docs/lb-simulation.md](docs/lb-simulation.md) for the full design (port wiring, task flow, load balancing, metrics).

For a side-by-side feature comparison with the microservice simulator, see [docs/lb-vs-ms.md](docs/lb-vs-ms.md).

**Express lane mode** (`--expresslane`) adds a dedicated overflow path to express servers when regular-server queues exceed a threshold. Use `--express-th`, `--express-del-th`, or both (combined OR eviction). Delay-only runs support monitored timers by default; `--ideal` selects immediate eviction for delay triggers (not compatible with `--express-th`). See [docs/expresslane.md](docs/expresslane.md).

**Work shedding** (`--shed-delay`) returns queued tasks from overloaded servers back to the originating client load balancer for re-routing. Uses monitored queue-delay triggers (head-of-line wait and projected delay). Mutually exclusive with express lane. See [docs/work-shedding.md](docs/work-shedding.md).

Load-balancing policies live in [`src/policy.rs`](src/policy.rs). Available policies:

- **random** — uniform random server selection
- **power-of-two** — sample two random servers and route to the one with lower local inflight (requests this balancer has dispatched but not yet received a release for)
- **least-request** — route to the server with the fewest locally in-flight requests; random tie-break among minima
- **round-robin** — cycle through servers in a randomly shuffled order (per load balancer)
- **centralized** — pull-based: one global queue at a single dispatcher; servers request work when they have spare capacity (`lb`: flat pool; ignores `--lb-subset-size`; incompatible with `--expresslane`). In `ms`, `centralized` applies to outbound routing only (one pull queue per downstream target); see [microservice-simulation.md](microservice-simulation.md#centralized-policy-pull-based-layer).
- **approx** — decentralized pull: per-client FIFO queues in `lb`; per-caller-replica outbound queues in `ms` (ingress stays P2C); optional **`--approx-sched fcfs`** or **`--approx-sched edf`** (`ms`) for unbound queue-head pulls; see [docs/approx-policy.md](docs/approx-policy.md)
- **cl** — shared push power-of-two outbound layer (`ms` only; ingress stays P2C; `--lb-subset-size > 0` rejected)
- **cl-lr** — shared push least-request outbound layer (`ms` only; ingress stays P2C; `--lb-subset-size > 0` rejected)
- **corr** — experimental shared push outbound layer (`ms` only; same topology as `cl`; ingress stays P2C; `--lb-subset-size > 0` rejected)

Each load balancer can be restricted to a subset of servers via `--lb-subset-size` (push policies only; not supported with `cl`, `cl-lr`, `centralized`, or `corr` in `ms`). With the default (`0`), every LB sees the full server pool. With `k > 0`, each LB routes among `min(k, servers)` servers using its own local inflight counts. Subset assignment uses `--lb-subset-policy` (default `deterministic`: round-based seeded shuffle partitioned by client id; use `random` for independent shuffle-and-truncate per LB).

## Metrics

For each completed task, let `p99(duration)` be the 99th percentile of all sampled service durations in the run:

- **Unloaded latency baseline:** `p99(duration)` (reported as `unloaded_latency_p99`)
- **E2e latency:** `finish - start` in seconds (reported as `e2e`)
- **Queueing delay:** `(finish - start) - duration` in seconds (reported as `queueing_delays`)

When `--slo` is set (latency threshold in seconds), the simulator also reports **P(latency > SLO)** in human output and includes `slo_latency` and `prob_latency_gt_slo` in JSON output. Without `--slo`, no SLO metrics are emitted.

The simulator also reports **utilization** as total service time divided by observation time and total system capacity (`servers × concurrency`).

**Load** is the target utilization (0–1). For exponential and constant service distributions, service time mean is fixed at 1 second. For bimodal, the mean is `E[S] = p1·m1 + p2·m2` from `--service-modes` and `--service-mode-probs`. Inter-arrival time is derived from load and capacity:

```
load = service_mean / (arrival_mean × servers × concurrency)
arrival_mean = service_mean / (load × servers × concurrency)
```

With the default exponential/constant `service_mean = 1`: `arrival_mean = 1 / (load × servers × concurrency)`.

With multiple clients, each client runs an independent arrival source at a slower rate so the aggregate load is unchanged:

```
per_client_arrival_mean = service_mean / (load × servers × concurrency × clients)
                        = arrival_mean × clients
```

`--n` is the total number of tasks across all clients (split evenly, with remainder distributed to the first clients).

### Constant arrivals and multiple clients

With `--arrival constant`, each client schedules tasks at a fixed gap of `per_client_arrival_mean`. Without phase offsetting, all clients would schedule their first task at `t=0` and repeat every `per_client_arrival_mean`, producing bursts of `C` tasks instead of a steady `1/arrival_mean` task/s stream.

Client `i` (0-based) with `--clients C` uses:

```
first arrival offset for client i:  i × arrival_mean
subsequent gaps for client i:       per_client_arrival_mean  (constant)
```

Equivalently, client `i` arrival times are `i·arrival_mean + k·(C·arrival_mean)` for `k = 0, 1, 2, …`.

**Worked example** (`C=3`, `arrival_mean = 1 s`):

| Client | Arrival times (s) |
|--------|-------------------|
| 0 | 0, 3, 6, 9, … |
| 1 | 1, 4, 7, 10, … |
| 2 | 2, 5, 8, 11, … |

Merged global stream: 0, 1, 2, 3, 4, 5, … — uniform spacing of `arrival_mean`.

With `--arrival exponential` (default), each client starts at `t=0` and samples `Exp(per_client_arrival_mean)` gaps; randomness desynchronizes clients and no phase offset is applied. Aggregate arrival-rate formulas are unchanged; only the gap distribution differs.

## Requirements

- Rust (stable)
- Python 3 with `numpy`, `matplotlib`, and `tqdm` (a local venv is fine)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install numpy matplotlib tqdm
```

## Build

```bash
cargo build --release
```

The binary is at `target/release/lb`.

## Microservice simulator (`ms`)

A separate binary simulates microservice applications from a callgraph and per-API load file. Callgraph service times are in **milliseconds**; `load.json` specifies per-API **RPS** and **SLO latency (`slo_ms`)**. See [docs/microservice-simulation.md](docs/microservice-simulation.md) for the full design (request flow, metrics, wiring). Feature differences vs `lb`: [docs/lb-vs-ms.md](docs/lb-vs-ms.md).

```bash
cargo build --release
./target/release/ms \
  --callgraph tests/fanin/callgraph.json \
  --load-file tests/fanin/load.json \
  --format human \
  --n 10000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--callgraph` | (required) | Path to callgraph JSON |
| `--load-file` | (required) | Path to per-API load JSON (`rps` + `slo_ms`) |
| `--n` | `1000000` | Total requests, split across APIs by RPS weight |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `least-request`, `round-robin`, `cl`, `cl-lr`, `centralized`, `approx`, `corr`) |
| `--pull-policy` | (none) | Pull-intent server selection for `approx` (`random`, `power-of-two`, `least-request`, `round-robin`); **required** with `--lb-policy approx` |
| `--approx-sched` | (omit) | With `approx`: omit for bound 1:1 pulls; `fcfs` or `edf` (ms only) for unbound queue-head fulfillment; see [docs/approx-policy.md](docs/approx-policy.md) |
| `--lb-subset-size` | `0` | Replicas each balancer can route to (`0` = all; not supported with `cl`, `cl-lr`, `centralized`, or `corr`) |
| `--lb-subset-policy` | `deterministic` | Subset assignment (`deterministic` or `random`) |
| `--seed` | (none) | RNG seed for reproducible runs |
| `--scheduling` | `fifo` | Server queue discipline (`fifo` or deadline-ordered `edf`); see [docs/scheduling.md](docs/scheduling.md) |
| `--format` | `human` | `human` or `json` |
| `--scale` | `0` | Add this many cores and replicas to every microservice |

JSON output includes per-microservice `microservice_utilization_pct`, per-server `server_utilization_pct`, per-microservice visit metrics in `by_microservice`, top-level `total_processing_p99_ms`, and per-API latency arrays in ms (`e2e_ms`, `processing_time_ms`) plus SLO fields (`unloaded_latency_p99_ms` computed from samples, `slo_latency_ms` from `load.json`, `prob_latency_gt_slo` as the fraction of requests exceeding the SLO).

## Simulator CLI

```bash
# Human-readable output (utilization + percentile tables)
./target/release/lb --format human --n 10000

# JSON output for scripting / plotting
./target/release/lb --format json --n 10000

# Four servers, two concurrent tasks each (default: power-of-two)
./target/release/lb --format human --n 10000 --servers 4 --concurrency 2

# Power-of-two-choices vs random with four servers
./target/release/lb --format human --n 10000 --servers 4 --lb-policy random
./target/release/lb --format human --n 10000 --servers 4 --lb-policy power-of-two

# Subsetting: each LB routes to 3 of 10 servers
./target/release/lb --format human --n 10000 --servers 10 --lb-subset-size 3

# Round-robin with four servers
./target/release/lb --format human --n 10000 --servers 4 --lb-policy round-robin

# Bimodal service time: 90% fast (0.1s mean), 10% slow (1.0s mean)
./target/release/lb --format human --n 10000 --service-dist bimodal \
  --service-modes 0.1,1.0 --service-mode-probs 0.9,0.1
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization (0–1); sets inter-arrival time from service mean |
| `--n` | `1000000` | Number of tasks |
| `--arrival` | `exponential` | `exponential` or `constant` (inter-arrival distribution) |
| `--service-dist` | `exponential` | `exponential`, `constant`, or `bimodal` |
| `--service-modes` | (none) | Two exponential means for bimodal (required with `bimodal`) |
| `--service-mode-probs` | (none) | Two mode probabilities summing to 1 (required with `bimodal`) |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server (CPU cores) |
| `--clients` | `1` | Number of independent clients (each with its own load balancer) |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `least-request`, `round-robin`, `centralized`, `approx`) |
| `--pull-policy` | (none) | Pull-intent server selection for `approx` (`random`, `power-of-two`, `least-request`, `round-robin`); **required** with `--lb-policy approx` |
| `--approx-sched` | (omit) | With `approx`: omit for bound 1:1 pulls; `fcfs` for unbound FCFS queue-head fulfillment |
| `--lb-subset-size` | `0` | Servers each LB can route to (`0` = all servers) |
| `--lb-subset-policy` | `deterministic` | Subset assignment (`deterministic` or `random`) |
| `--seed` | (none) | RNG seed for reproducible runs |
| `--slo` | (none) | SLO latency threshold in seconds; when set, reports P(latency > SLO) |
| `--shed-delay` | (none) | Work shedding threshold in seconds; overloaded servers return queued tasks to the client LB for re-routing (push policies only; not combinable with `--expresslane`) |
| `--format` | `human` | `human` (utilization + p1–p100 tables) or `json` |

With default `--servers 1 --concurrency 1`, behavior matches the original single-server simulator.

JSON output shape (with `--slo 5.0`):

```json
{
  "utilization_pct": 80.0,
  "unloaded_latency_p99": 4.61,
  "slo_latency": 5.0,
  "prob_latency_gt_slo": 0.012,
  "e2e": [1.2, 1.5, ...],
  "queueing_delays": [0.2, 0.5, ...]
}
```

Without `--slo`, `slo_latency` and `prob_latency_gt_slo` are omitted from JSON.

With `--shed-delay`, JSON includes `pct_shed_requests` (percentage of completed requests shed at least once). Human output prints `shed requests: X.XX%`.

## Plot scripts overview

Python plotting scripts live in the repo root. Each runs the simulator (or compares simulators), collects metrics, and writes a PDF under `output/`.

| Script | X-axis | Y-axis | Purpose |
|--------|--------|--------|---------|
| [`plot_cdfs.py`](plot_cdfs.py) (lb) | e2e latency (s, log) | CDF | Full latency distribution for a single lb run; optional SLO / threshold marks |
| [`plot_cdfs.py`](plot_cdfs.py) (ms) | e2e latency (ms, log) | CDF | Per-API latency distribution for the microservice simulator |
| [`plot_lb_sweep.py`](plot_lb_sweep.py) | sweep param (load, clients, …) | configurable metric (default p99) | Compare LB policies (one line each) while sweeping one simulator parameter |
| [`plot_lb_load_compare.py`](plot_lb_load_compare.py) | load | configurable metric (default p99) | Compare experiment configs (policy, topology, subset size) at equal utilization |
| [`plot_lb_centralized_compare.py`](plot_lb_centralized_compare.py) | total arrival rate (task/s) | configurable metric (default p99) | Compare centralized vs power-of-two at equal offered load with different server counts |
| [`plot_lb_express_heatmap.py`](plot_lb_express_heatmap.py) | express pool size | express delay threshold | Heatmap of a metric across express-size vs express-del-th |
| [`optimize_express_lane.py`](optimize_express_lane.py) | — | — | Grid-search express_size × express_del_th × express_th; human-readable log only (no plots) |
| [`plot_ms_chain_slo_heatmap.py`](plot_ms_chain_slo_heatmap.py) | load level | chain depth (chain3 / chain6) | Heatmap of SLO violation rate (%) across load for chain topologies |
| [`compare_lb_ms.py`](compare_lb_ms.py) | latency (s, log) | CDF | Overlay lb vs ms CDFs on equivalent topologies to validate parity |

Use [`plot_lb_sweep.py`](plot_lb_sweep.py) with `--sweep load` or `--sweep lb-subset-size` for the common load and subset-size studies; other `--sweep` values (e.g. `clients`) use the same script. Use [`plot_lb_load_compare.py`](plot_lb_load_compare.py) when comparing named configs (different policies, server counts, or subset sizes) at the same load levels.

## Analysis scripts

Scripts under [`analyze/`](analyze/) run deeper behavioral analysis that requires extended `ms` JSON output (e.g. per-microservice visit metrics). See [docs/analyze.md](docs/analyze.md) for vocabulary, metrics, normalization, and usage.

| Script | Description |
|--------|-------------|
| [`analyze/ms_service_distributions.py`](analyze/ms_service_distributions.py) | Per-microservice inter-arrival, inter-departure, response time, and queueing delay CDFs (chain-3 default) |

## Plot e2e CDF

`plot_cdfs.py` builds the release binary once, runs the simulator, and writes an e2e latency CDF to `output/e2e_cdf.pdf`. The x-axis uses a log scale. Pass `--slo` to mark the SLO threshold on the plot.

```bash
python plot_cdfs.py --n 100000
```

Plot script options mirror the simulator (`--load`, `--n`, `--service-dist`, `--service-modes`, `--service-mode-probs`, `--servers`, `--concurrency`, `--clients`, `--lb-policy`, `--lb-subset-size`, `--seed`) plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization |
| `--output` | `output/e2e_cdf.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server |
| `--clients` | `1` | Number of independent clients |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `least-request`, `round-robin`, `centralized`, `approx`) |
| `--pull-policy` | (none) | Pull-intent server selection for `approx` (`random`, `power-of-two`, `least-request`, `round-robin`); **required** with `--lb-policy approx` |
| `--approx-sched` | (omit) | With `approx`: pass `fcfs` or `edf` to the simulator subprocess |
| `--lb-subset-size` | `0` | Servers each LB can route to (`0` = all servers) |
| `--lb-subset-policy` | `deterministic` | Subset assignment (`deterministic` or `random`) |
| `--seed` | (none) | RNG seed for reproducible simulation |
| `--slo` | (none) | SLO latency threshold in seconds (marked on CDF when set) |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--mark` | (none) | Additional latency threshold(s) in seconds to annotate with P(latency ≤ x) on the plot |

Example with a filename comment suffix:

```bash
python plot_cdfs.py --n 100000 --comment 4srv_c2
# writes output/e2e_cdf_4srv_c2.pdf
```

Example with custom parameters and additional threshold marks:

```bash
python plot_cdfs.py \
  --load 0.5 \
  --mark 10 \
  --mark 30
```

Example with full parameter set:

```bash
python plot_cdfs.py \
  --n 50000 \
  --load 0.9 \
  --service-dist exponential \
  --output output/e2e_cdf.pdf
```

Approx with oldest-FCFS pulls (`lb` only):

```bash
python plot_cdfs.py \
  --lb-policy approx --pull-policy least-request \
  --approx-sched fcfs --n 100000
```

On failure, `plot_cdfs.py` prints the simulator command, exit code, and full stderr/stdout. Set `RUST_BACKTRACE=1` for panic backtraces when debugging the Rust binary.

### Plot microservice e2e CDF

Use `--simulator ms` to run the microservice binary and plot per-API e2e latency CDFs. Latencies and `--mark` thresholds are in **milliseconds**. The SLO from `load.json` (`slo_ms`) is marked on each plot automatically.

```bash
python plot_cdfs.py --simulator ms \
  --callgraph tests/fanin/callgraph.json \
  --load-file tests/fanin/load.json \
  --n 100000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--simulator` | `lb` | `lb` or `ms` |
| `--callgraph` | (required for ms) | Path to callgraph JSON |
| `--load-file` | (required for ms) | Path to per-API load JSON (`rps` + `slo_ms`) |
| `--api` | (none) | Plot only this API; omit to plot all APIs from the load file (one subplot per API) |
| `--output` | `output/e2e_cdf_ms.pdf` | Output PDF path |
| `--mark` | (none) | Additional latency threshold(s) in **ms** |

Single API:

```bash
python plot_cdfs.py --simulator ms \
  --callgraph tests/fanin/callgraph.json \
  --load-file tests/fanin/load.json \
  --api f1 \
  --n 100000 \
  --comment fanin_f1
# writes output/e2e_cdf_ms_fanin_f1.pdf
```

With additional threshold marks (ms):

```bash
python plot_cdfs.py --simulator ms \
  --callgraph tests/fanin/callgraph.json \
  --load-file tests/fanin/load.json \
  --n 100000 \
  --mark 25 --mark 50
```

Shared flags with lb mode: `--n`, `--lb-policy` (default `power-of-two` for both), `--lb-subset-size`, `--lb-subset-policy`, `--seed`, `--binary`, `--comment`.

## Plot LB parameter sweep

`plot_lb_sweep.py` runs the lb simulator over a grid of `(lb-policy, sweep-value)` pairs and writes a line plot with one line per LB policy. Choose the x-axis parameter with `--sweep` (default `load`); all other simulator parameters stay fixed unless they are the sweep axis. Choose the y-axis with `--metric` (default `p99`).

```bash
python plot_lb_sweep.py --sweep load --servers 10 --n 100000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--sweep` | `load` | X-axis parameter: `load`, `clients`, `servers`, `concurrency`, `lb-subset-size` |
| `--series` | `lb-policy` | Legend lines (v1: `lb-policy` only) |
| `--metric` | `p99` | Y-axis: `p99`, `p50`, `p90`, `utilization`, `slo-violation`, or `p{N}` |
| `--output` | `output/lb_{sweep}_{metric}.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--load` | (see below) | Fixed load, or multiple values when `--sweep load` |
| `--load-min` / `--load-max` / `--load-step` | `0.1` / `1.0` / `0.1` | Load range when `--sweep load` and `--load` omitted |
| `--clients` | (see below) | Fixed client count, or multiple when `--sweep clients` |
| `--clients-min` / `--clients-max` / `--clients-step` | `1` / `8` / `1` | Client range when `--sweep clients` and `--clients` omitted |
| `--servers` | (see below) | Fixed server count, or multiple when `--sweep servers` |
| `--servers-min` / `--servers-max` / `--servers-step` | `1` / `8` / `1` | Server range when `--sweep servers` and `--servers` omitted |
| `--concurrency` | (see below) | Fixed concurrency, or multiple when `--sweep concurrency` |
| `--concurrency-min` / `--concurrency-max` / `--concurrency-step` | `1` / `4` / `1` | Concurrency range when `--sweep concurrency` and `--concurrency` omitted |
| `--lb-subset-size` | (see below) | Fixed subset size (`0` = all), or multiple when `--sweep lb-subset-size` |
| `--subset-min` / `--subset-max` / `--subset-step` | (none) / (none) / `1` | Subset range when sweeping subset size |
| `--n` | `1000000` | Tasks per run |
| `--service-dist` | `exponential` | Service distribution (`exponential`, `constant`, or `bimodal`) |
| `--service-modes` | (none) | Two exponential means for bimodal |
| `--service-mode-probs` | (none) | Two mode probabilities for bimodal |
| `--lb-policy` | all five policies | Policies to compare (series lines) |
| `--slo` | (none) | SLO threshold in seconds (required for `--metric slo-violation`) |
| `--seed` | (none) | RNG seed for reproducible runs |
| `--format` | `compact` | `human` (summary + e2e percentiles) or `compact` (one line per run) |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--no-build` | (off) | Do not run `cargo build --release` |

When a parameter is **not** the sweep axis, pass a single value (e.g. `--load 0.8`, `--clients 4`). When it **is** the sweep axis, pass multiple values (`--load 0.5 0.7 0.9`) or use the param-specific min/max/step flags. For `--sweep lb-subset-size` with no explicit values, subset sizes default to powers of two from `1` through `--servers`, then `0` (full pool; x-axis label `all`).

Sweep load (replaces former `plot_load_sweep.py`):

```bash
python plot_lb_sweep.py \
  --sweep load \
  --load-min 0.3 \
  --load-max 0.95 \
  --load-step 0.05 \
  --lb-policy power-of-two least-request \
  --lb-subset-size 4 \
  --n 100000 \
  --comment subset4
# writes output/lb_load_p99_subset4.pdf
```

Sweep subset size (replaces former `plot_lb_subset_sweep.py`):

```bash
python plot_lb_sweep.py \
  --sweep lb-subset-size \
  --load 0.8 \
  --servers 10 \
  --clients 10 \
  --concurrency 4 \
  --n 500000 \
  --lb-subset-size 0 1 2 3 5 10 \
  --comment multi_client
# writes output/lb_lb_subset_size_p99_multi_client.pdf
```

Sweep client count:

```bash
python plot_lb_sweep.py \
  --sweep clients \
  --clients 1 2 4 8 \
  --load 0.8 \
  --n 100000
# writes output/lb_clients_p99.pdf
```

## Plot LB config load compare

`plot_lb_load_compare.py` compares named experiment configs while sweeping **raw load** on the x-axis. Each config can differ in LB policy, client/server counts, concurrency, `lb_subset_size`, and (for approx) `approx_sched`. All configs share the same load values (target utilization).

Use this when you want to compare specific topologies at equal utilization. Use [`plot_lb_sweep.py`](plot_lb_sweep.py) for generic one-parameter sweeps with one line per policy. Use [`plot_lb_centralized_compare.py`](plot_lb_centralized_compare.py) when the x-axis should be equal offered load (task/s) across different server counts.

Edit experiment configs in the `DEFAULT_CONFIGS` list at the top of [`plot_lb_load_compare.py`](plot_lb_load_compare.py) (or shared [`lb_plot_configs.py`](lb_plot_configs.py) types). Use `--config-index` to run a subset without editing the file. For approx configs, set `approx_sched="fcfs"` on individual `ExperimentConfig` entries to enable unbound FCFS pull fulfillment.

```bash
python plot_lb_load_compare.py --n 100000 --seed 42
# writes output/lb_load_compare_p99.pdf
```

| Flag | Default | Description |
|------|---------|-------------|
| `--metric` | `p99` | Y-axis: `p99`, `p50`, `p90`, `utilization`, `slo-violation`, or `p{N}` |
| `--output` | `output/lb_load_compare_{metric}.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--load` | (see below) | Explicit load values for x-axis |
| `--load-min` / `--load-max` / `--load-step` | `0.1` / `0.9` / `0.1` | Load sweep range when `--load` omitted |
| `--config-index` | (all) | Run only these `DEFAULT_CONFIGS` indices (0-based) |
| `--n` | `1000000` | Tasks per run |
| `--service-dist` | `exponential` | Service distribution (`exponential`, `constant`, or `bimodal`) |
| `--seed` | (none) | RNG seed for reproducible runs |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--no-build` | (off) | Do not run `cargo build --release` |

Example comparing subset sizes at fixed topology:

```bash
python plot_lb_load_compare.py \
  --config-index 1 2 3 4 5 \
  --load-min 0.3 --load-max 0.9 --load-step 0.1 \
  --n 100000 \
  --comment subset
# writes output/lb_load_compare_p99_subset.pdf
```

Example comparing bound vs unbound approx (per-config `approx_sched` in `DEFAULT_CONFIGS`):

```bash
python plot_lb_load_compare.py \
  --config-index 7 8 \
  --comment nb \
  --n 100000
```

## Plot centralized vs scaled power-of-two

`plot_lb_centralized_compare.py` compares **centralized** against **power-of-two** at different server counts while holding **offered load (task/s)** constant on the x-axis. The question it helps answer: if power-of-two uses extra servers, does p99 latency approach centralized at the same arrival rate?

The reference topology is centralized with 10 clients and 10 servers. A reference load sweep (default `0.1`–`0.9` step `0.1`) maps to arrival rates `1`–`9` task/s when service mean is 1 s and concurrency is 1. For configs with a different server count, `--load` is scaled so aggregate arrival rate matches:

```
load = arrival_rate × service_mean / (servers × concurrency)
```

With the default reference (10 servers), each +0.1 reference load adds 1 task/s. For 12 servers at 1 task/s: `load = 0.1 × (10/12)`.

Edit experiment configs in the `DEFAULT_CONFIGS` list at the top of [`plot_lb_centralized_compare.py`](plot_lb_centralized_compare.py). Use `--config-index` to run a subset without editing the file.

```bash
python plot_lb_centralized_compare.py --n 100000 --seed 42
# writes output/lb_centralized_compare_p99.pdf
```

| Flag | Default | Description |
|------|---------|-------------|
| `--metric` | `p99` | Y-axis: `p99`, `p50`, `p90`, `utilization`, `slo-violation`, or `p{N}` |
| `--output` | `output/lb_centralized_compare_{metric}.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--ref-load-min` / `--ref-load-max` / `--ref-load-step` | `0.1` / `0.9` / `0.1` | Reference load sweep (maps to x-axis task/s via `--ref-servers`) |
| `--ref-servers` | `10` | Reference server count for load-to-rate mapping |
| `--config-index` | (all) | Run only these `DEFAULT_CONFIGS` indices (0-based) |
| `--n` | `100000` | Tasks per run |
| `--service-dist` | `exponential` | Service distribution (`exponential`, `constant`, or `bimodal`) |
| `--seed` | (none) | RNG seed for reproducible runs |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--no-build` | (off) | Do not run `cargo build --release` |

Subset during development:

```bash
python plot_lb_centralized_compare.py \
  --config-index 0 3 4 \
  --ref-load-min 0.1 --ref-load-max 0.5 --ref-load-step 0.1 \
  --n 100000
```

## Optimize express lane parameters

`optimize_express_lane.py` grid-searches express lane parameters to minimize a metric (default **p99**). By default it searches **combined eviction mode** over `express_size`, `express_del_th`, and `express_th` (3D grid). With `--ideal`, it searches **delay-only oracle mode** over `express_size` and `express_del_th` only (2D grid; `--express-th` is not allowed). There is **no plot or PDF output** — progress is written to a human-readable log under `optimizer_logs/` (gitignored). The log is rewritten after each simulation with a results table, current optimum, and optimum history; rows where a new best is found are marked `NEW OPTIMUM`.

Default grid (with `--servers 10`): express_size 1–4, express_del_th 1–10, express_th 0–6 → 280 runs. Values `express_size=0` and `express_del_th=0` are dropped.

```bash
python optimize_express_lane.py \
  --comment bimodal-p2c \
  --servers 10 --load 0.8 --n 100000

# Delay-only oracle eviction (2D grid):
python optimize_express_lane.py \
  --ideal --comment ideal-p2c \
  --servers 10 --load 0.8 --n 100000

# Resume after interrupt:
python optimize_express_lane.py --resume optimizer_logs/express_lane_20250702_153045_bimodal-p2c.log
```

| Flag | Default | Description |
|------|---------|-------------|
| `--comment` | (none) | Label included in log filename (`express_lane_{timestamp}_{comment}_n{N}.log`) |
| `--log-dir` | `optimizer_logs/` | Directory for optimizer logs |
| `--resume` | (none) | Continue from an existing log file |
| `--metric` | `p99` | Objective metric (`p99`, `p50`, `utilization`, `slo-violation`, or `p{N}`) |
| `--ideal` | off | Delay-only oracle eviction; 2D grid (no `express_th`) |
| `--express-size-min/max/step` | `0` / `4` / `1` | Express pool size sweep |
| `--express-del-th-min/max/step` | `0` / `10` / `1` | Express delay threshold sweep (seconds) |
| `--express-th-min/max/step` | `0` / `6` / `1` | Express queue depth threshold sweep (combined mode only) |
| `--load` | `0.8` | Target utilization |
| `--servers` | `10` | Total servers (regular + express) |
| `--n` | `1000000` | Tasks per run |
| `--lb-policy` | `power-of-two` | Client LB policy for regular pool |
| `--seed` | (none) | RNG seed |
| `--binary` / `--no-build` | (build release) | Prebuilt binary options |

## Plot microservice chain SLO heatmap

`plot_ms_chain_slo_heatmap.py` runs the ms simulator on chain3 and chain6 fixtures at each load level and writes a heatmap of SLO violation rate (%) to `output/ms_chain_slo_heatmap.pdf`. Cell color encodes the violation percentage; rows are chain depth, columns are load.

```bash
python plot_ms_chain_slo_heatmap.py --n 100000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | `output/ms_chain_slo_heatmap.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--load-min` / `--load-max` / `--load-step` | `0.1` / `0.9` / `0.1` | Load sweep range |
| `--n` | `100000` | Requests per run |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `least-request`, `round-robin`, `cl`, `cl-lr`, `centralized`, `approx`, `corr`) |
| `--pull-policy` | (none) | Pull-intent server selection for `approx` (required with `--lb-policy approx`) |
| `--approx-sched` | (omit) | With `--lb-policy approx`: omit for bound pulls; `fcfs` or `edf` (ms only) for unbound queue-head fulfillment |
| `--lb-subset-size` | `0` | Subset size per LB (`0` = all replicas) |
| `--scheduling` | `fifo` | Server queue discipline (`fifo` or deadline-ordered `edf`) |
| `--binary` | (build release) | Use a prebuilt ms binary and skip `cargo build --release` |

## Compare lb and ms simulators

`compare_lb_ms.py` runs equivalent lb and ms topologies and checks that utilization and latency percentiles (p50, p90, p99) match within tolerance. Pass `--plot` to write overlay CDF plots to `output/`.

```bash
python compare_lb_ms.py --scenario all --plot
```

| Flag | Default | Description |
|------|---------|-------------|
| `--scenario` | `all` | `single`, `multi`, or `all` fixture topologies |
| `--n` | `200000` | Total requests per run |
| `--load` | `0.8` | Target utilization for lb (ms `load.json` rps must match) |
| `--lb-policy` | `power-of-two` | Load-balancing policy for both simulators (`cl`, `cl-lr`, `corr`, and ms `centralized` are shared-layer outbound; subset not supported with those policies) |
| `--plot` | (off) | Write lb vs ms overlay CDF plots to `--output-dir` |
| `--output-dir` | `output/` | Directory for optional CDF plots |
| `--no-build` | (off) | Do not run `cargo build --release` |
