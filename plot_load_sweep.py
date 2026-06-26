#!/usr/bin/env python3
"""Sweep load and plot P(latency > SLO) vs load."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
from tqdm import tqdm

from plot_cdfs import (
    LB_POLICIES,
    REPO_ROOT,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    ecdf_probability,
    plot_line,
)

DEFAULT_OUTPUT = REPO_ROOT / "output" / "slo_violation.pdf"
HUMAN_PERCENTILES = (1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 100)


def percentile(sorted_values: list[float], pct: float) -> float:
    idx = round((len(sorted_values) - 1) * pct / 100.0)
    return sorted_values[int(idx)]


def format_percentile_table(label: str, values: list[float]) -> str:
    sorted_values = sorted(values)
    parts = [
        f"p{int(pct)}: {percentile(sorted_values, pct):>8.4f}"
        for pct in HUMAN_PERCENTILES
    ]
    return f"{label}\n  " + "  ".join(parts)


def report_run_stats(
    *,
    lb_subset_size: int,
    load: float,
    data: dict,
    prob_gt: float,
    slo: float,
    output_format: str,
) -> None:
    summary = (
        f"k={lb_subset_size}  load={load:g}  P(latency>SLO)={prob_gt:.6f}  "
        f"SLO={slo:.4f}s  "
        f"utilization={data['utilization_pct']:.1f}%"
    )
    if output_format == "human":
        tqdm.write(summary)
        tqdm.write(format_percentile_table("e2e latency (s):", data["e2e"]))
    else:
        tqdm.write(summary)


def load_values(load_min: float, load_max: float, load_step: float) -> list[float]:
    values = np.arange(load_min, load_max + load_step / 2, load_step, dtype=float)
    return [float(v) for v in values]


def prob_latency_gt_slo(data: dict, slo: float) -> float:
    if "prob_latency_gt_slo" in data:
        return data["prob_latency_gt_slo"]
    samples = data["e2e"]
    if not samples:
        return 0.0
    return 1.0 - ecdf_probability(samples, slo)


def run_load_sweep(
    binary: Path,
    loads: list[float],
    *,
    slo: float,
    n: int,
    service_dist: str,
    servers: int = 1,
    concurrency: int = 1,
    clients: int = 1,
    lb_policy: str = "power-of-two",
    lb_subset_size: int = 0,
    output_format: str = "human",
    service_modes: list[float] | None = None,
    service_mode_probs: list[float] | None = None,
) -> list[float]:
    probs: list[float] = []
    for load in tqdm(
        loads,
        desc=f"k={lb_subset_size} load sweep",
        unit="run",
    ):
        data = run_simulation(
            binary,
            load=load,
            n=n,
            service_dist=service_dist,
            servers=servers,
            concurrency=concurrency,
            clients=clients,
            lb_policy=lb_policy,
            lb_subset_size=lb_subset_size,
            service_modes=service_modes,
            service_mode_probs=service_mode_probs,
            slo=slo,
        )
        if not data["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        prob_gt = prob_latency_gt_slo(data, slo)
        probs.append(prob_gt)
        report_run_stats(
            lb_subset_size=lb_subset_size,
            load=load,
            data=data,
            prob_gt=prob_gt,
            slo=slo,
            output_format=output_format,
        )
    return probs


def plot_slo_violation_prob(
    loads: list[float],
    series: list[tuple[int, list[float]]],
    output_path: Path,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)
    for color_idx, (k, probs) in enumerate(series):
        plot_line(
            ax,
            loads,
            probs,
            label=f"k={k}",
            style=style,
            color_idx=color_idx,
            show_markers=True,
        )
    ax.set_xlim(min(loads), max(loads))
    ax.set_ylim(0.0, 1.0)
    ax.set_xticks(loads)
    ax.set_xticklabels([f"{load:g}" for load in loads])
    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel="Load",
        ylabel="P(latency > SLO)",
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep load and plot P(latency > SLO) vs load.",
    )
    parser.add_argument("--binary", type=Path, default=None,
                        help="Prebuilt release binary (skips cargo build --release)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Output PDF path")
    parser.add_argument(
        "--comment", type=str, default=None,
        help="Suffix appended to output filename before .pdf (e.g. slo_violation_foo.pdf)",
    )
    parser.add_argument("--load-min", type=float, default=0.1)
    parser.add_argument("--load-max", type=float, default=1.0)
    parser.add_argument("--load-step", type=float, default=0.1)
    parser.add_argument("--slo", type=float, required=True,
                        help="SLO latency threshold in seconds (passed to lb simulator)")
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--service-dist", choices=["exponential", "constant", "bimodal"],
                        default="exponential")
    parser.add_argument(
        "--service-modes", type=float, nargs=2, metavar=("M0", "M1"),
        help="Exponential means for bimodal modes (required with --service-dist bimodal)",
    )
    parser.add_argument(
        "--service-mode-probs", type=float, nargs=2, metavar=("P0", "P1"),
        help="Mode selection probabilities (required with --service-dist bimodal)",
    )
    parser.add_argument("--servers", type=int, default=1,
                        help="Number of servers (passed to lb simulator)")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Concurrent tasks per server (passed to lb simulator)")
    parser.add_argument("--clients", type=int, default=1,
                        help="Number of independent clients (passed to lb simulator)")
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default="power-of-two",
                        help="Load-balancing policy (passed to lb simulator)")
    parser.add_argument(
        "--lb-subset-size", type=int, nargs="+", default=[0],
        help="Subset size(s) per LB (0 = all servers); pass multiple to compare",
    )
    parser.add_argument("--format", choices=["human", "compact"], default="human",
                        help="human: summary + e2e latency percentiles; compact: one line per load")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    loads = load_values(args.load_min, args.load_max, args.load_step)
    if not loads:
        print("no load values in sweep range", file=sys.stderr)
        sys.exit(1)

    binary = ensure_release_binary(REPO_ROOT, args.binary)
    series: list[tuple[int, list[float]]] = []
    for k in args.lb_subset_size:
        probs = run_load_sweep(
            binary,
            loads,
            slo=args.slo,
            n=args.n,
            service_dist=args.service_dist,
            servers=args.servers,
            concurrency=args.concurrency,
            clients=args.clients,
            lb_policy=args.lb_policy,
            lb_subset_size=k,
            output_format=args.format,
            service_modes=args.service_modes,
            service_mode_probs=args.service_mode_probs,
        )
        series.append((k, probs))

    output_path = output_path_with_comment(args.output, args.comment)
    plot_slo_violation_prob(loads, series, output_path)
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
