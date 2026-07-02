#!/usr/bin/env python3
"""Sweep express-size vs express-del-th and plot a metric heatmap."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-express-heatmap-plot-cache"
_MPL_CACHE = _CACHE_ROOT / "matplotlib"
_XDG_CACHE = _CACHE_ROOT / "xdg"
_MPL_CACHE.mkdir(parents=True, exist_ok=True)
_XDG_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))
os.environ.setdefault("XDG_CACHE_HOME", str(_XDG_CACHE))
os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np

try:
    from tqdm import tqdm
except ModuleNotFoundError:
    def tqdm(iterable, **_kwargs):
        return iterable

from plot_cdfs import (
    LB_POLICIES,
    REPO_ROOT,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plot_lb_sweep import (
    METRIC_CHOICES,
    extract_metric,
    metric_ylabel,
    parse_metric,
    range_values,
)
from plotting_primitive import ACM_COMPACT_HALF, SubplotGrid, plot_heatmap

DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"


def express_size_values(args: argparse.Namespace) -> list[int]:
    if args.express_size is not None:
        values = list(args.express_size)
    else:
        values = range_values(
            args.express_size_min,
            args.express_size_max,
            args.express_size_step,
            value_type=int,
            step_flag="--express-size-step",
        )
    max_size = args.servers - 1
    if max_size < 1:
        raise SystemExit(f"--servers must be >= 2 for express lane (got {args.servers})")
    filtered = [v for v in values if 1 <= v <= max_size]
    if not filtered:
        raise SystemExit(
            f"no valid express-size values in [1, {max_size}]; "
            f"check --express-size or --express-size-min/max/step"
        )
    if len(filtered) < len(values):
        dropped = [v for v in values if v not in filtered]
        print(
            f"dropping invalid express-size values (must be in [1, {max_size}]): {dropped}",
            file=sys.stderr,
        )
    return filtered


def express_del_th_values(args: argparse.Namespace) -> list[float]:
    if args.express_del_th is not None:
        values = [float(v) for v in args.express_del_th]
    else:
        values = range_values(
            args.express_del_th_min,
            args.express_del_th_max,
            args.express_del_th_step,
            value_type=float,
            step_flag="--express-del-th-step",
        )
    if not values:
        raise SystemExit("no express-del-th values in sweep range")
    for value in values:
        if value <= 0:
            raise SystemExit(f"--express-del-th values must be positive (got {value:g})")
    return values


def annotation_fmt(metric: str) -> str:
    kind, _ = parse_metric(metric)
    if kind == "utilization":
        return "{:.1f}"
    if kind == "slo-violation":
        return "{:.3f}"
    return "{:.3f}"


def format_run_summary(
    *,
    sim_kwargs: dict,
    metric_name: str,
    metric_value: float,
    data: dict,
) -> str:
    parts = [
        f"policy={sim_kwargs['lb_policy']}",
        f"load={sim_kwargs['load']:g}",
        f"servers={sim_kwargs['servers']}",
        f"express_size={sim_kwargs['express_size']}",
        f"express_del_th={sim_kwargs['express_del_th']:g}",
    ]
    if sim_kwargs.get("ideal"):
        parts.append("ideal")
    kind, pct = parse_metric(metric_name)
    if kind == "utilization":
        parts.append(f"utilization={metric_value:.1f}%")
    elif kind == "slo-violation":
        parts.append(f"P(latency>SLO)={metric_value:.6f}")
    else:
        parts.append(f"p{int(pct)}={metric_value:.6f}s")
    parts.append(f"utilization={data['utilization_pct']:.1f}%")
    return "  ".join(parts)


def default_output_path(metric: str) -> Path:
    metric_slug = metric.replace("-", "_")
    return DEFAULT_OUTPUT_DIR / f"lb_express_heatmap_{metric_slug}.pdf"


def plot_title(args: argparse.Namespace) -> str:
    parts = [
        f"load={args.load:g}",
        f"servers={args.servers}",
        f"clients={args.clients}",
        f"policy={args.lb_policy}",
    ]
    if args.ideal:
        parts.append("ideal")
    return "  ".join(parts)


def run_express_sweep(
    binary: Path,
    *,
    express_sizes: list[int],
    express_del_ths: list[float],
    base_kwargs: dict,
    metric: str,
    slo: float | None,
) -> np.ndarray:
    data = np.zeros((len(express_del_ths), len(express_sizes)), dtype=float)
    pairs = [
        (row, col, express_del_th, express_size)
        for row, express_del_th in enumerate(express_del_ths)
        for col, express_size in enumerate(express_sizes)
    ]

    for row, col, express_del_th, express_size in tqdm(
        pairs,
        desc="express-size × express-del-th sweep",
        unit="run",
    ):
        sim_kwargs = {
            **base_kwargs,
            "expresslane": True,
            "express_size": express_size,
            "express_del_th": express_del_th,
        }
        result = run_simulation(binary, **sim_kwargs)
        if not result["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        metric_value = extract_metric(result, metric, slo=slo)
        data[row, col] = metric_value
        summary = format_run_summary(
            sim_kwargs=sim_kwargs,
            metric_name=metric,
            metric_value=metric_value,
            data=result,
        )
        write = getattr(tqdm, "write", None)
        if write is None:
            print(summary, file=sys.stderr)
        else:
            write(summary)

    return data


def plot_express_heatmap(
    express_sizes: list[int],
    express_del_ths: list[float],
    data: np.ndarray,
    *,
    metric: str,
    output_path: Path,
    title: str,
) -> None:
    style = replace(ACM_COMPACT_HALF, aspect_ratio=0.62)
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)
    plot_heatmap(
        ax,
        data,
        x_labels=[str(size) for size in express_sizes],
        y_labels=[f"{value:g}" for value in express_del_ths],
        style=style,
        vmin=float(np.nanmin(data)),
        vmax=float(np.nanmax(data)),
        colorbar_label=metric_ylabel(metric),
        annotation_fmt=annotation_fmt(metric),
    )
    grid.configure_ax(
        ax,
        xlabel="Express pool size",
        ylabel="Express delay threshold (s)",
        title=title,
        grid=False,
        auto_ticks=False,
    )
    grid.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep express-size vs express-del-th and plot a metric heatmap.",
    )
    parser.add_argument(
        "--metric",
        default="p99",
        help=f"Heatmap metric: {', '.join(METRIC_CHOICES)}, or p{{N}} (default: p99)",
    )
    parser.add_argument(
        "--binary",
        type=Path,
        default=None,
        help="Prebuilt release binary (skips cargo build --release)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Do not run cargo build --release",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PDF path (default: output/lb_express_heatmap_{metric}.pdf)",
    )
    parser.add_argument(
        "--comment",
        type=str,
        default=None,
        help="Suffix appended to output filename before .pdf",
    )
    parser.add_argument("--load", type=float, default=0.8)
    parser.add_argument("--servers", type=int, default=10)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--lb-subset-size", type=int, default=0)
    parser.add_argument(
        "--express-size",
        type=int,
        nargs="*",
        default=None,
        help="Express pool size(s); overrides --express-size-min/max/step",
    )
    parser.add_argument("--express-size-min", type=int, default=1)
    parser.add_argument("--express-size-max", type=int, default=4)
    parser.add_argument("--express-size-step", type=int, default=1)
    parser.add_argument(
        "--express-del-th",
        type=float,
        nargs="*",
        default=None,
        help="Express delay threshold(s) in seconds; overrides min/max/step",
    )
    parser.add_argument("--express-del-th-min", type=float, default=0.1)
    parser.add_argument("--express-del-th-max", type=float, default=2.0)
    parser.add_argument("--express-del-th-step", type=float, default=0.2)
    parser.add_argument(
        "--ideal",
        action="store_true",
        help="Immediate oracle eviction when projected delay exceeds threshold (default: monitored timer)",
    )
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument(
        "--service-dist",
        choices=["exponential", "constant", "bimodal"],
        default="exponential",
    )
    parser.add_argument(
        "--service-modes",
        type=float,
        nargs=2,
        metavar=("M0", "M1"),
        help="Exponential means for bimodal modes (required with --service-dist bimodal)",
    )
    parser.add_argument(
        "--service-mode-probs",
        type=float,
        nargs=2,
        metavar=("P0", "P1"),
        help="Mode selection probabilities (required with --service-dist bimodal)",
    )
    parser.add_argument(
        "--lb-policy",
        choices=LB_POLICIES,
        default="power-of-two",
    )
    parser.add_argument(
        "--slo",
        type=float,
        default=None,
        help="SLO latency threshold in seconds (required for --metric slo-violation)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="RNG seed for reproducible simulation",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parse_metric(args.metric)
    if parse_metric(args.metric)[0] == "slo-violation" and args.slo is None:
        raise SystemExit("--slo is required when --metric slo-violation")

    express_sizes = express_size_values(args)
    express_del_ths = express_del_th_values(args)

    if args.no_build:
        binary = args.binary or DEFAULT_BINARY
    else:
        binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="lb")

    if not binary.is_file():
        raise SystemExit(f"lb binary not found: {binary}")

    base_kwargs = {
        "load": args.load,
        "n": args.n,
        "service_dist": args.service_dist,
        "servers": args.servers,
        "concurrency": args.concurrency,
        "clients": args.clients,
        "lb_policy": args.lb_policy,
        "lb_subset_size": args.lb_subset_size,
        "service_modes": args.service_modes,
        "service_mode_probs": args.service_mode_probs,
        "seed": args.seed,
        "slo": args.slo,
        "ideal": args.ideal,
    }

    data = run_express_sweep(
        binary,
        express_sizes=express_sizes,
        express_del_ths=express_del_ths,
        base_kwargs=base_kwargs,
        metric=args.metric,
        slo=args.slo,
    )

    output_path = args.output or default_output_path(args.metric)
    output_path = output_path_with_comment(output_path, args.comment)
    plot_express_heatmap(
        express_sizes,
        express_del_ths,
        data,
        metric=args.metric,
        output_path=output_path,
        title=plot_title(args),
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
