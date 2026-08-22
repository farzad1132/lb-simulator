# Load Balancer Simulator

A discrete-event simulator for evaluating load balancing strategies in distributed systems. This project provides two complementary simulators: `lb` for client-server architectures with shared server pools, and `ms` for microservice topologies with complex call graphs.

## Overview

The simulator models task arrival, routing, queueing, and processing to evaluate how different load balancing policies affect latency distribution, queue buildup, and system utilization under controlled conditions.

**Key capabilities:**
- Multiple load balancing policies (power-of-two choices, least-request, random, round-robin, centralized pull queues, decentralized pull)
- Flexible arrival patterns (exponential/Poisson, constant inter-arrival)
- Service time distributions (exponential, constant, bimodal)
- Express lane overflow routing for latency-sensitive workloads
- Work shedding with monitored queue delays
- Microservice call graph simulation with per-API SLO tracking

**Use cases:**
- Compare load balancing policies at different utilization levels
- Study queueing behavior with subsetting (restricted routing)
- Evaluate microservice architectures with realistic call graphs
- Optimize express lane parameters for mixed workloads

## Project Structure

```
.
├── src/
│   ├── main.rs                  # CLI entry point for lb binary
│   ├── bin/ms.rs                # CLI entry point for ms binary
│   ├── lib.rs                   # Library root
│   ├── lb_simulate.rs           # Load balancer simulation logic
│   ├── load_balancer.rs         # Load balancer entity (routing, release tracking)
│   ├── server.rs                # Server entity (FCFS queue, concurrent workers)
│   ├── policy.rs                # Load balancing policy implementations
│   ├── subset.rs                # Server subsetting logic (deterministic/random)
│   ├── prequal.rs               # Prequal policy (async probe pool)
│   ├── approx.rs                # Approx policy (decentralized pull)
│   ├── scheduling.rs            # Server queue disciplines (FIFO, EDF)
│   └── microservice/            # Microservice simulator modules
│       ├── mod.rs               # Module root and main run logic
│       ├── callgraph.rs         # Callgraph loading and validation
│       ├── simulate.rs          # Microservice simulation orchestration
│       ├── replica.rs           # Microservice replica entity
│       ├── balancer.rs          # Outbound routing within microservices
│       ├── sidecar.rs           # Sidecar for approx-share policy
│       ├── hop.rs               # Per-hop metrics tracking
│       └── trace.rs             # Request tracing (debug output)
│
├── tests/                       # Integration tests with fixture callgraphs and load files
│   ├── lb_*.rs                  # Load balancer simulator tests
│   ├── ms_*.rs                  # Microservice simulator tests
│   ├── chain/                   # Chain topologies (2, 3, 6, 10 hops)
│   ├── fanin/                   # Fan-in topologies (single/multi)
│   ├── fanout/                  # Fan-out topologies (scaleout/scaleup)
│   ├── client_server/           # Simple client-server topologies
│   └── caller_queue/            # Caller queue topology
│
├── docs/                        # Detailed design documentation
│   ├── lb-simulation.md         # Load balancer architecture and policies
│   ├── microservice-simulation.md  # Microservice simulator design
│   ├── lb-vs-ms.md              # Feature comparison between simulators
│   ├── expresslane.md           # Express lane overflow routing
│   ├── work-shedding.md         # Work shedding with monitored delays
│   ├── approx-policy.md         # Decentralized pull policies
│   ├── jbsq-policy.md           # Bounded centralized pull policy
│   ├── prequal-policy.md        # Prequal policy design
│   ├── scheduling.md            # Server queue disciplines (FIFO, EDF)
│   └── analyze.md               # Analysis scripts documentation
│
├── analyze/                     # Deep behavioral analysis scripts
│   ├── ms_service_distributions.py  # Per-microservice inter-arrival, response time CDFs
│   └── lb_service_distributions.py  # Per-server distributions
│
├── plot_cdfs.py                 # Plot e2e latency CDFs
├── plot_lb_sweep.py             # Parameter sweep plots (load, clients, subset size)
├── plot_lb_load_compare.py      # Compare configs at equal utilization
├── plot_lb_subset_compare.py    # Compare configs across subset sizes
├── plot_lb_centralized_compare.py  # Centralized vs P2C at equal offered load
├── plot_lb_express_heatmap.py   # Express lane parameter heatmap
├── optimize_express_lane.py     # Grid search for express lane parameters
├── plot_ms_chain_slo_heatmap.py # SLO violation heatmap for chain topologies
├── plot_ms_chain_load_compare.py  # Compare MS configs on one chain
├── compare_lb_ms.py             # Validate lb vs ms equivalence
│
├── Cargo.toml                   # Rust package manifest
├── requirements.txt             # Python dependencies for plotting
└── .venv/                       # Python virtual environment (created by user)
```

## Installation

### Prerequisites

- **Rust** (stable toolchain, 1.83.0 or later)
- **Python 3** with `numpy`, `matplotlib`, and `tqdm` (for plotting scripts)

### Setup

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Create and activate a Python virtual environment:**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Install Python dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Build the simulator binaries:**

   ```bash
   cargo build --release
   ```

   The binaries are placed in `target/release/`:
   - `target/release/lb` — load balancer simulator
   - `target/release/ms` — microservice simulator

## Usage

### Load Balancer Simulator (`lb`)

The `lb` binary simulates a shared server pool with independent clients, each running its own load balancer.

**Basic example:**

```bash
./target/release/lb --format human --n 10000 --servers 4 --concurrency 2
```

**Compare power-of-two vs random:**

```bash
./target/release/lb --format human --n 10000 --servers 4 --lb-policy random
./target/release/lb --format human --n 10000 --servers 4 --lb-policy power-of-two
```

**Key options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--load` | `0.8` | Target utilization (0–1) |
| `--n` | `1000000` | Number of tasks to simulate |
| `--servers` | `1` | Number of servers in the pool |
| `--concurrency` | `1` | Concurrent tasks per server (simulates CPU cores) |
| `--clients` | `1` | Number of independent clients |
| `--lb-policy` | `power-of-two` | Load balancing policy: `random`, `power-of-two`, `least-request`, `round-robin`, `centralized`, `approx`, `prequal` |
| `--lb-subset-size` | `0` | Servers each load balancer can route to (`0` = all servers) |
| `--arrival` | `exponential` | Inter-arrival distribution: `exponential` or `constant` |
| `--service-dist` | `exponential` | Service time distribution: `exponential`, `constant`, or `bimodal` |
| `--slo` | (none) | SLO latency threshold in seconds; when set, reports P(latency > SLO) |
| `--format` | `human` | Output format: `human` (tables) or `json` |
| `--seed` | (none) | RNG seed for reproducible runs |

**JSON output** (with `--format json`) includes:
- `utilization_pct`: actual system utilization
- `unloaded_latency_p99`: 99th percentile of sampled service times
- `e2e`: array of end-to-end latencies (seconds)
- `queueing_delays`: array of queueing delays (seconds)
- `slo_latency` and `prob_latency_gt_slo`: SLO threshold and violation rate (when `--slo` is set)

### Microservice Simulator (`ms`)

The `ms` binary simulates microservice topologies from a callgraph JSON and per-API load file.

**Basic example:**

```bash
./target/release/ms \
  --callgraph tests/chain/3/callgraph.json \
  --load-file tests/chain/3/load.json \
  --format human \
  --n 10000
```

**Key options:**

| Flag | Default | Description |
|------|---------|-------------|
| `--callgraph` | (required) | Path to callgraph JSON file |
| `--load-file` | (required) | Path to per-API load JSON (`rps` + `slo_ms`) |
| `--n` | `1000000` | Total requests (split across APIs by RPS weight) |
| `--lb-policy` | `power-of-two` | Load balancing policy for outbound routing |
| `--pull-policy` | (none) | Pull-intent server selection for `approx`/`approx-share` (required with those policies) |
| `--lb-subset-size` | `0` | Replicas each balancer can route to (`0` = all) |
| `--scheduling` | `fifo` | Server queue discipline: `fifo` or deadline-ordered `edf` |
| `--scale` | `0` | Add N cores and replicas to every microservice |
| `--format` | `human` | Output format: `human` or `json` |
| `--seed` | (none) | RNG seed for reproducible runs |

**Callgraph format** (JSON):
- `services`: map of service name → `{replicas, cpus, service_time_ms}`
- `edges`: list of `{from_service, to_service, fanout}` (fanout = downstream calls per upstream request)

**Load file format** (JSON):
- Per-API entry: `{api_name, rps, slo_ms}`

**JSON output** includes:
- Per-API latency arrays (`e2e_ms`, `processing_time_ms`)
- Per-API SLO fields (`unloaded_latency_p99_ms`, `slo_latency_ms`, `prob_latency_gt_slo`)
- Per-microservice utilization and visit metrics (`by_microservice`)

### Running Tests

```bash
cargo test
```

Integration tests validate policy correctness, lb/ms equivalence, and subsetting behavior using fixture callgraphs under `tests/`.

### Plotting Scripts

The Python scripts generate PDFs under `output/` (created automatically).

**Plot e2e latency CDF:**

```bash
python plot_cdfs.py --n 100000 --load 0.8 --servers 4 --lb-policy power-of-two
```

**Sweep load (compare policies):**

```bash
python plot_lb_sweep.py \
  --sweep load \
  --load-min 0.3 --load-max 0.95 --load-step 0.05 \
  --lb-policy power-of-two least-request \
  --servers 10 \
  --n 100000
```

**Compare microservice configs on chain topology:**

```bash
python plot_ms_chain_load_compare.py --chain 3 --n 100000
```

**Plot microservice SLO heatmap (chain3, chain6, chain10):**

```bash
python plot_ms_chain_slo_heatmap.py --n 100000
```

For full plotting script documentation, see the original README sections or run `python <script>.py --help`.

## Important Features

### Load Balancing Policies

- **Random**: Uniform random server selection
- **Power-of-two choices**: Sample two servers, route to the one with lower local inflight
- **Least-request**: Route to the server with fewest locally in-flight requests
- **Round-robin**: Cycle through servers in shuffled order (per load balancer)
- **Centralized**: Pull-based FIFO queue(s) at central dispatcher(s); servers request work when spare capacity available
- **JBSQ** (ms only): Bounded central pull; replicas pull while occupancy is below `--jbsq-n`
- **Approx**: Decentralized pull with per-client (lb) or per-caller-replica (ms) FIFO queues; optional unbound queue-head scheduling (`--approx-sched fcfs|edf|edf+`)
- **Approx-share** (ms only): Dual-mode sidecars with grouped replicas
- **Prequal**: Decentralized push with async RIF probe pool
- **CL** (ms only): Shared push power-of-two outbound layer

### Subsetting

Restrict each load balancer to a subset of servers with `--lb-subset-size k`. With `k > 0`:
- **Push / approx**: Each client LB routes among `min(k, servers)` servers
- **Centralized (lb)**: `k` must divide `--servers`; one shared pull queue per subset
- **Not supported**: `prequal` and certain ms policies (`cl`, `cl-lr`, `cl-r`, `cl-rr`, `corr`)

### Express Lane

Overflow routing to dedicated express servers when regular-server queues exceed thresholds. Enable with `--expresslane` and configure with `--express-th` (queue depth) or `--express-del-th` (queue delay). See `docs/expresslane.md`.

### Work Shedding

Overloaded servers return queued tasks to the originating client load balancer for re-routing. Enable with `--shed-delay` (monitored queue-delay trigger). Mutually exclusive with express lane. See `docs/work-shedding.md`.

### Scheduling Policies

Server queue discipline (ms only):
- **FIFO**: First-in-first-out (default)
- **EDF**: Earliest-deadline-first (requires `--scheduling edf`); see `docs/scheduling.md`

### Service Distributions

- **Exponential** (default): Memoryless service times (mean = 1s for lb, in ms for ms)
- **Constant**: Fixed service time
- **Bimodal**: Mixture of two exponential modes (specify with `--service-modes` and `--service-mode-probs`)

## Documentation

Detailed design documents are in `docs/`:
- `lb-simulation.md` — Load balancer architecture, wiring, and policies
- `microservice-simulation.md` — Microservice simulator design and request flow
- `lb-vs-ms.md` — Feature comparison between `lb` and `ms`
- `expresslane.md` — Express lane overflow routing
- `work-shedding.md` — Work shedding with monitored delays
- `approx-policy.md` — Decentralized pull policies (approx, approx-share)
- `jbsq-policy.md` — Bounded centralized pull (JBSQ)
- `prequal-policy.md` — Prequal policy (async probe pool)
- `scheduling.md` — Server queue disciplines (FIFO, EDF)
- `analyze.md` — Analysis scripts for per-microservice distributions

## License

This project does not currently specify a license. Please contact the repository owner for licensing information.

## Contributing

Contributions are welcome. Please ensure all tests pass before submitting pull requests:

```bash
cargo test
cargo fmt --check
cargo clippy -- -D warnings
```
