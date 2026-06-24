# lb

Multi-server FCFS queue simulator with pluggable load-balancing policies. Tasks arrive according to independent Poisson processes from one or more clients, receive exponential or constant service times, and are routed by each client's load balancer to a shared pool of servers. Each server has its own FIFO queue and can process multiple tasks concurrently (simulating CPU cores).

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

Load-balancing policies live in [`src/policy.rs`](src/policy.rs). Available policies:

- **random** — uniform random server selection
- **power-of-two** — sample two random servers and route to the one with fewer locally in-flight requests (dispatched by this LB but not yet completed)
- **round-robin** — cycle through servers in a randomly shuffled order (per load balancer)

Each load balancer can be restricted to a random subset of servers via `--lb-subset-size`. With the default (`0`), every LB sees the full server pool. With `k > 0`, each LB independently samples `min(k, servers)` servers at startup and only routes among that subset using its own local inflight counts.

## Metrics

For each completed task, let `p99(duration)` be the 99th percentile of all sampled service durations in the run:

- **Unloaded latency baseline:** `p99(duration)` (reported as `unloaded_latency_p99`)
- **SLO latency:** `5 × p99(duration)` (reported as `slo_latency`)
- **E2e latency:** `finish - start` in seconds (reported as `e2e`)
- **Queueing delay:** `(finish - start) - duration` in seconds (reported as `queueing_delays`)

The simulator also reports **utilization** as total service time divided by observation time and total system capacity (`servers × concurrency`).

**Load** is the target utilization (0–1). Service time is fixed at mean 1 second; inter-arrival time is derived from load and capacity:

```
load = service_mean / (arrival_mean × servers × concurrency)
arrival_mean = service_mean / (load × servers × concurrency)
```

With `service_mean = 1`: `arrival_mean = 1 / (load × servers × concurrency)`.

With multiple clients, each client runs an independent Poisson source at a slower rate so the aggregate load is unchanged:

```
per_client_arrival_mean = service_mean / (load × servers × concurrency × clients)
                        = arrival_mean × clients
```

`--n` is the total number of tasks across all clients (split evenly, with remainder distributed to the first clients).

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

A separate binary simulates microservice applications from a callgraph and per-API load file. Callgraph service times are in **milliseconds**; `load.json` rates are in **RPS**. See [docs/microservice-simulation.md](docs/microservice-simulation.md) for the full design (request flow, metrics, wiring).

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
| `--load-file` | (required) | Path to per-API RPS JSON |
| `--n` | `1000000` | Total requests, split across APIs by RPS weight |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `round-robin`) |
| `--lb-subset-size` | `0` | Replicas each balancer can route to (`0` = all) |
| `--format` | `human` | `human` or `json` |

JSON output includes per-microservice `utilization_pct` and per-API latency arrays in ms (`e2e_ms`, `processing_time_ms`) plus SLO fields (`unloaded_latency_p99_ms`, `slo_latency_ms` = 5× unloaded p99, same rule as `lb`).

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
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization (0–1); sets inter-arrival time from fixed service mean 1s |
| `--n` | `1000000` | Number of tasks |
| `--service-dist` | `exponential` | `exponential` or `constant` |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server (CPU cores) |
| `--clients` | `1` | Number of independent clients (each with its own load balancer) |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `round-robin`) |
| `--lb-subset-size` | `0` | Servers each LB can route to (`0` = all servers) |
| `--format` | `human` | `human` (utilization + p1–p100 tables) or `json` |

With default `--servers 1 --concurrency 1`, behavior matches the original single-server simulator.

JSON output shape:

```json
{
  "utilization_pct": 80.0,
  "unloaded_latency_p99": 4.61,
  "slo_latency": 23.05,
  "e2e": [1.2, 1.5, ...],
  "queueing_delays": [0.2, 0.5, ...]
}
```

## Plot e2e CDF

`plot_cdfs.py` builds the release binary once, runs the simulator, and writes an e2e latency CDF to `output/e2e_cdf.pdf`. The x-axis uses a log scale. The SLO latency (`5 × unloaded p99`) is marked on the plot automatically.

```bash
python plot_cdfs.py --n 100000
```

Plot script options mirror the simulator (`--load`, `--n`, `--service-dist`, `--servers`, `--concurrency`, `--clients`, `--lb-policy`, `--lb-subset-size`) plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization |
| `--output` | `output/e2e_cdf.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server |
| `--clients` | `1` | Number of independent clients |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `round-robin`) |
| `--lb-subset-size` | `0` | Servers each LB can route to (`0` = all servers) |
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

On failure, `plot_cdfs.py` prints the simulator command, exit code, and full stderr/stdout. Set `RUST_BACKTRACE=1` for panic backtraces when debugging the Rust binary.

## Plot SLO violation probability vs load

`plot_load_sweep.py` runs the simulator at each load point (default 0.1, 0.2, …, 1.0), computes P(latency > SLO) using each run's own `slo_latency`, and writes a line plot to `output/slo_violation.pdf`. A progress bar shows sweep status on stderr.

```bash
python plot_load_sweep.py --n 100000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | `output/slo_violation.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--load-min` / `--load-max` / `--load-step` | `0.1` / `1.0` / `0.1` | Load sweep range |
| `--n` | `1000000` | Tasks per load point |
| `--service-dist` | `exponential` | Service distribution |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server |
| `--clients` | `1` | Number of independent clients |
| `--lb-policy` | `power-of-two` | Load-balancing policy (`random`, `power-of-two`, `round-robin`) |
| `--lb-subset-size` | `0` | Subset size(s) per LB (`0` = all servers); pass multiple values to compare on one plot |
| `--format` | `human` | `human` (summary + e2e latency percentiles per load) or `compact` (one line per load) |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |

Example comparing subset sizes:

```bash
python plot_load_sweep.py \
  --servers 10 \
  --lb-subset-size 0 2 4 8 \
  --n 100000 \
  --comment subset_cmp
# writes output/slo_violation_subset_cmp.pdf with legend k=0, k=2, k=4, k=8
```

Example:

```bash
python plot_load_sweep.py \
  --n 100000 \
  --comment random_lb \
  --load-min 0.1 \
  --load-max 1.0 \
  --load-step 0.1
# writes output/slo_violation_random_lb.pdf
```

Another example with an explicit output path:

```bash
python plot_load_sweep.py \
  --n 100000 \
  --load-min 0.1 \
  --load-max 1.0 \
  --load-step 0.1 \
  --output output/slo_violation.pdf
```
