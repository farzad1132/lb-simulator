# lb

Multi-server FCFS queue simulator with pluggable load-balancing policies. Tasks arrive according to independent Poisson processes from one or more clients, receive exponential or constant service times, and are routed by each client's load balancer to a shared pool of servers. Each server has its own FIFO queue and can process multiple tasks concurrently (simulating CPU cores).

## Architecture

```
exp_source_0 → LoadBalancer_0 ─┐
exp_source_1 → LoadBalancer_1 ─┼→ Server_0 ─┐
...                            → Server_1 ─┼→ shared output sink
exp_source_C → LoadBalancer_C ─→ Server_N ─┘
```

With `--clients 1`, this reduces to a single client → load balancer → servers path.

Load-balancing policies live in [`src/policy.rs`](src/policy.rs). The initial policy is **random** server selection; add new variants by implementing `LoadBalancePolicy` and extending `LoadBalancePolicyKind`.

## Metrics

For each completed task, let `p99(duration)` be the 99th percentile of all sampled service durations in the run:

- **Unloaded latency baseline:** `p99(duration)` (reported as `unloaded_latency_p99`)
- **Normalized e2e latency (slowdown):** `(finish - start) / p99(duration)`
- **Normalized queueing delay:** `((finish - start) - duration) / p99(duration)`

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

## Simulator CLI

```bash
# Human-readable output (utilization + percentile tables)
./target/release/lb --format human --n 10000

# JSON output for scripting / plotting
./target/release/lb --format json --n 10000

# Four servers, two concurrent tasks each, random load balancing
./target/release/lb --format human --n 10000 --servers 4 --concurrency 2
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
| `--lb-policy` | `random` | Load-balancing policy (`random`) |
| `--format` | `human` | `human` (utilization + p1–p100 tables) or `json` |

With default `--servers 1 --concurrency 1`, behavior matches the original single-server simulator.

JSON output shape:

```json
{
  "utilization_pct": 80.0,
  "unloaded_latency_p99": 3.68,
  "normalized_e2e": [1.2, 1.5, ...],
  "normalized_queueing_delays": [0.2, 0.5, ...]
}
```

## Plot e2e CDF

`plot_cdfs.py` builds the release binary once, runs the simulator, and writes a normalized e2e latency CDF to `output/e2e_cdf.pdf`. The x-axis uses a log scale so low slowdown values (1×–10×) are easy to read.

```bash
python plot_cdfs.py --n 100000
```

Plot script options mirror the simulator (`--load`, `--n`, `--service-dist`, `--servers`, `--concurrency`, `--clients`) plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization |
| `--output` | `output/e2e_cdf.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server |
| `--clients` | `1` | Number of independent clients |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--mark` | (none) | Slowdown value(s) to annotate with P(slowdown ≤ x) on the plot |

Example with a filename comment suffix:

```bash
python plot_cdfs.py --n 100000 --comment 4srv_c2
# writes output/e2e_cdf_4srv_c2.pdf
```

Example with custom parameters and threshold marks:

```bash
python plot_cdfs.py \
  --load 0.5 \
  --mark 5 \
  --mark 10
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

## Plot slowdown probability vs load

`plot_load_sweep.py` runs the simulator at each load point (default 0.1, 0.2, …, 1.0), computes P(slowdown ≥ threshold), and writes a line plot to `output/slowdown_ge_5.pdf`. A progress bar shows sweep status on stderr.

```bash
python plot_load_sweep.py --n 100000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | `output/slowdown_ge_5.pdf` | Output PDF path |
| `--comment` | (none) | Suffix appended to output filename before `.pdf` |
| `--threshold` | `5` | Slowdown cutoff |
| `--load-min` / `--load-max` / `--load-step` | `0.1` / `1.0` / `0.1` | Load sweep range |
| `--n` | `1000000` | Tasks per load point |
| `--service-dist` | `exponential` | Service distribution |
| `--servers` | `1` | Number of servers |
| `--concurrency` | `1` | Concurrent tasks per server |
| `--clients` | `1` | Number of independent clients |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |

Example:

```bash
python plot_load_sweep.py \
  --n 100000 \
  --comment random_lb \
  --threshold 5 \
  --load-min 0.1 \
  --load-max 1.0 \
  --load-step 0.1
# writes output/slowdown_ge_5_random_lb.pdf
```

Another example with an explicit output path:

```bash
python plot_load_sweep.py \
  --n 100000 \
  --threshold 5 \
  --load-min 0.1 \
  --load-max 1.0 \
  --load-step 0.1 \
  --output output/slowdown_ge_5.pdf
```
