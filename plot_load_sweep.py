#!/usr/bin/env python3
"""Sweep load and plot P(slowdown >= threshold) vs load."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
from tqdm import tqdm

from plot_cdfs import REPO_ROOT, ensure_release_binary, output_path_with_comment, run_simulation
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    ecdf_probability,
    plot_line,
)

DEFAULT_OUTPUT = REPO_ROOT / "output" / "slowdown_ge_5.pdf"


def load_values(load_min: float, load_max: float, load_step: float) -> list[float]:
    values = np.arange(load_min, load_max + load_step / 2, load_step, dtype=float)
    return [float(v) for v in values]


def prob_slowdown_ge(data: dict, threshold: float) -> float:
    samples = data["normalized_e2e"]
    if not samples:
        return 0.0
    return 1.0 - ecdf_probability(samples, threshold)


def run_load_sweep(
    binary: Path,
    loads: list[float],
    *,
    n: int,
    service_dist: str,
    threshold: float,
    servers: int = 1,
    concurrency: int = 1,
    clients: int = 1,
) -> tuple[list[float], list[float]]:
    probs: list[float] = []
    for load in tqdm(loads, desc="load sweep", unit="run"):
        data = run_simulation(
            binary,
            load=load,
            n=n,
            service_dist=service_dist,
            servers=servers,
            concurrency=concurrency,
            clients=clients,
        )
        if not data["normalized_e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        prob_ge = prob_slowdown_ge(data, threshold)
        probs.append(prob_ge)
        tqdm.write(
            f"load={load:g}  P(slowdown>={threshold:g})={prob_ge:.6f}  "
            f"utilization={data['utilization_pct']:.1f}%"
        )
    return loads, probs


def plot_slowdown_prob(
    loads: list[float],
    probs: list[float],
    output_path: Path,
    *,
    threshold: float,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)
    plot_line(ax, loads, probs, style=style, show_markers=True)
    ax.set_xlim(min(loads), max(loads))
    ax.set_ylim(0.0, 1.0)
    ax.set_xticks(loads)
    ax.set_xticklabels([f"{load:g}" for load in loads])
    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel="Load",
        ylabel=f"P(slowdown ≥ {threshold:g})",
    )
    grid.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep load and plot P(slowdown >= threshold) vs load.",
    )
    parser.add_argument("--binary", type=Path, default=None,
                        help="Prebuilt release binary (skips cargo build --release)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Output PDF path")
    parser.add_argument(
        "--comment", type=str, default=None,
        help="Suffix appended to output filename before .pdf (e.g. slowdown_ge_5_foo.pdf)",
    )
    parser.add_argument("--threshold", type=float, default=5.0,
                        help="Slowdown cutoff")
    parser.add_argument("--load-min", type=float, default=0.1)
    parser.add_argument("--load-max", type=float, default=1.0)
    parser.add_argument("--load-step", type=float, default=0.1)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--service-dist", choices=["exponential", "constant"],
                        default="exponential")
    parser.add_argument("--servers", type=int, default=1,
                        help="Number of servers (passed to lb simulator)")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Concurrent tasks per server (passed to lb simulator)")
    parser.add_argument("--clients", type=int, default=1,
                        help="Number of independent clients (passed to lb simulator)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    loads = load_values(args.load_min, args.load_max, args.load_step)
    if not loads:
        print("no load values in sweep range", file=sys.stderr)
        sys.exit(1)

    binary = ensure_release_binary(REPO_ROOT, args.binary)
    loads, probs = run_load_sweep(
        binary,
        loads,
        n=args.n,
        service_dist=args.service_dist,
        threshold=args.threshold,
        servers=args.servers,
        concurrency=args.concurrency,
        clients=args.clients,
    )
    output_path = output_path_with_comment(args.output, args.comment)
    plot_slowdown_prob(
        loads,
        probs,
        output_path,
        threshold=args.threshold,
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
