# lb

Single-server FCFS queue simulator. Tasks arrive according to a Poisson process, receive exponential or constant service times, and are served by one server with a FIFO queue.

## Metrics

For each completed task:

- **Normalized e2e latency (slowdown):** `(finish - start) / duration`
- **Normalized queueing delay:** `((finish - start) - duration) / duration`

The simulator also reports server **utilization** (fraction of observation time the server was busy).

## Requirements

- Rust (stable)
- Python 3 with `numpy` and `matplotlib` (a local venv is fine)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install numpy matplotlib
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
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--arrival-mean` | `1.0` | Mean inter-arrival time (seconds) |
| `--service-mean` | `0.8` | Mean service time (seconds) |
| `--n` | `1000000` | Number of tasks |
| `--service-dist` | `exponential` | `exponential` or `constant` |
| `--format` | `human` | `human` (utilization + p1–p100 tables) or `json` |

JSON output shape:

```json
{
  "utilization_pct": 80.0,
  "normalized_e2e": [1.2, 1.5, ...],
  "normalized_queueing_delays": [0.2, 0.5, ...]
}
```

## Plot e2e CDF

`plot_cdfs.py` builds the release binary once, runs the simulator, and writes a normalized e2e latency CDF to `output/e2e_cdf.pdf`. The x-axis uses a log scale so low slowdown values (1×–10×) are easy to read.

```bash
python plot_cdfs.py --n 100000
```

Plot script options mirror the simulator (`--arrival-mean`, `--service-mean`, `--n`, `--service-dist`) plus:

| Flag | Default | Description |
|------|---------|-------------|
| `--output` | `output/e2e_cdf.pdf` | Output PDF path |
| `--binary` | (build release) | Use a prebuilt binary and skip `cargo build --release` |
| `--mark` | (none) | Slowdown value(s) to annotate with P(slowdown ≤ x) on the plot |

Example with custom parameters and threshold marks:

```bash
python plot_cdfs.py \
  --service-mean 0.5 \
  --mark 5 \
  --mark 10
```

Example with full parameter set:

```bash
python plot_cdfs.py \
  --n 50000 \
  --arrival-mean 1.0 \
  --service-mean 0.9 \
  --service-dist exponential \
  --output output/e2e_cdf.pdf
```

On failure, `plot_cdfs.py` prints the simulator command, exit code, and full stderr/stdout. Set `RUST_BACKTRACE=1` for panic backtraces when debugging the Rust binary.
