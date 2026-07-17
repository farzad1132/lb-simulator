#!/usr/bin/env python3
"""Sweep an lb simulator parameter and plot a metric vs sweep axis per LB policy."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any

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
    PlotStyle,
    SubplotGrid,
    configure_y_axis_ticks,
    ecdf_probability,
    percentile,
    plot_line,
)

DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"
HUMAN_PERCENTILES = (1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 100)

SWEEP_CHOICES = ("load", "clients", "servers", "concurrency", "lb-subset-size")
SERIES_CHOICES = ("lb-policy",)
METRIC_CHOICES = ("p99", "p50", "p90", "utilization", "slo-violation")


@dataclass(frozen=True)
class ParamSpec:
    name: str
    sim_key: str
    xlabel: str
    fixed_default: float | int
    tick_label: Callable[[Any], str] | None = None
    use_index_x: bool = False


SWEEP_PARAMS: dict[str, ParamSpec] = {
    "load": ParamSpec("load", "load", "Load", 0.8),
    "clients": ParamSpec("clients", "clients", "Clients", 1),
    "servers": ParamSpec("servers", "servers", "Servers", 1),
    "concurrency": ParamSpec("concurrency", "concurrency", "Concurrency", 1),
    "lb-subset-size": ParamSpec(
        "lb-subset-size",
        "lb_subset_size",
        "LB subset size",
        0,
        tick_label=lambda k: "all" if k == 0 else str(k),
        use_index_x=True,
    ),
}

SERIES_PARAMS: dict[str, ParamSpec] = {
    "lb-policy": ParamSpec("lb-policy", "lb_policy", "LB policy", "power-of-two"),
}


def format_percentile_table(label: str, values: list[float]) -> str:
    sorted_values = sorted(values)
    parts = [
        f"p{int(pct)}: {percentile(sorted_values, pct):>8.4f}"
        for pct in HUMAN_PERCENTILES
    ]
    return f"{label}\n  " + "  ".join(parts)


def range_values(
    vmin: float | int,
    vmax: float | int,
    step: float | int,
    *,
    value_type: type,
    step_flag: str,
) -> list[Any]:
    if step <= 0:
        raise SystemExit(f"{step_flag} must be positive")
    if vmin > vmax:
        raise SystemExit(f"min must be <= max for {step_flag}")
    values = np.arange(vmin, vmax + step / 2, step, dtype=float)
    if value_type is int:
        return [int(v) for v in values]
    return [value_type(v) for v in values]


def default_subset_sizes(servers: int) -> list[int]:
    """Powers of two from 1 through servers, then 0 (full pool)."""
    servers = max(servers, 1)
    sizes: list[int] = []
    k = 1
    while k < servers:
        sizes.append(k)
        k *= 2
    sizes.append(servers)
    sizes.append(0)
    return sizes


def resolve_sweep_values(args: argparse.Namespace, sweep: str) -> list[Any]:
    if sweep == "load":
        if args.load is not None:
            return list(args.load)
        return range_values(
            args.load_min, args.load_max, args.load_step,
            value_type=float, step_flag="--load-step",
        )
    if sweep == "clients":
        if args.clients is not None:
            return list(args.clients)
        return range_values(
            args.clients_min, args.clients_max, args.clients_step,
            value_type=int, step_flag="--clients-step",
        )
    if sweep == "servers":
        if args.servers is not None:
            return list(args.servers)
        return range_values(
            args.servers_min, args.servers_max, args.servers_step,
            value_type=int, step_flag="--servers-step",
        )
    if sweep == "concurrency":
        if args.concurrency is not None:
            return list(args.concurrency)
        return range_values(
            args.concurrency_min, args.concurrency_max, args.concurrency_step,
            value_type=int, step_flag="--concurrency-step",
        )
    if sweep == "lb-subset-size":
        if args.lb_subset_size is not None:
            return list(args.lb_subset_size)
        if args.subset_min is not None or args.subset_max is not None:
            if args.subset_min is None or args.subset_max is None:
                raise SystemExit(
                    "--subset-min and --subset-max must both be set when using subset range"
                )
            return range_values(
                args.subset_min, args.subset_max, args.subset_step,
                value_type=int, step_flag="--subset-step",
            )
        fixed_servers = fixed_param_value(args, "servers")
        return default_subset_sizes(int(fixed_servers))
    raise SystemExit(f"unsupported sweep parameter: {sweep}")


def fixed_param_value(args: argparse.Namespace, param: str) -> Any:
    spec = SWEEP_PARAMS[param]
    if param == "load":
        if args.load is not None:
            return args.load[0]
        return spec.fixed_default
    if param == "clients":
        if args.clients is not None:
            return args.clients[0]
        return spec.fixed_default
    if param == "servers":
        if args.servers is not None:
            return args.servers[0]
        return spec.fixed_default
    if param == "concurrency":
        if args.concurrency is not None:
            return args.concurrency[0]
        return spec.fixed_default
    if param == "lb-subset-size":
        if args.lb_subset_size is not None:
            return args.lb_subset_size[0]
        return spec.fixed_default
    raise SystemExit(f"unsupported parameter: {param}")


def base_sim_kwargs(args: argparse.Namespace, sweep: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "n": args.n,
        "service_dist": args.service_dist,
        "service_modes": args.service_modes,
        "service_mode_probs": args.service_mode_probs,
        "seed": args.seed,
        "slo": args.slo,
    }
    for param in SWEEP_PARAMS:
        if param != sweep:
            kwargs[SWEEP_PARAMS[param].sim_key] = fixed_param_value(args, param)
    return kwargs


def parse_metric(metric: str) -> tuple[str, float | None]:
    if metric == "utilization":
        return ("utilization", None)
    if metric == "slo-violation":
        return ("slo-violation", None)
    match = re.fullmatch(r"p(\d+)", metric)
    if match:
        return ("percentile", float(match.group(1)))
    raise SystemExit(
        f"unsupported metric {metric!r}; choose from {', '.join(METRIC_CHOICES)} "
        "or p{N} (e.g. p95)"
    )


def metric_ylabel(metric: str) -> str:
    kind, pct = parse_metric(metric)
    if kind == "utilization":
        return "Utilization (%)"
    if kind == "slo-violation":
        return "SLO Violation Ratio"
    return f"p{int(pct)} e2e latency (s)"


def extract_metric(data: dict, metric: str, *, slo: float | None) -> float:
    kind, pct = parse_metric(metric)
    if kind == "utilization":
        return float(data["utilization_pct"])
    if kind == "slo-violation":
        if slo is None:
            raise SystemExit("--slo is required when --metric slo-violation")
        if "prob_latency_gt_slo" in data:
            return float(data["prob_latency_gt_slo"])
        samples = data["e2e"]
        if not samples:
            return 0.0
        return 1.0 - ecdf_probability(samples, slo)
    if not data["e2e"]:
        raise SystemExit("no completed tasks")
    return percentile(data["e2e"], pct)


def series_label(series: str, value: Any) -> str:
    return str(value)


def format_run_summary(
    *,
    sim_kwargs: dict[str, Any],
    metric_name: str,
    metric_value: float,
    data: dict,
) -> str:
    parts = [
        f"policy={sim_kwargs['lb_policy']}",
        f"k={sim_kwargs['lb_subset_size']}",
        f"load={sim_kwargs['load']:g}",
        f"clients={sim_kwargs['clients']}",
        f"servers={sim_kwargs['servers']}",
        f"concurrency={sim_kwargs['concurrency']}",
    ]
    kind, pct = parse_metric(metric_name)
    if kind == "utilization":
        parts.append(f"utilization={metric_value:.1f}%")
    elif kind == "slo-violation":
        parts.append(f"P(latency>SLO)={metric_value:.6f}")
    else:
        parts.append(f"p{int(pct)}={metric_value:.6f}s")
    parts.append(f"utilization={data['utilization_pct']:.1f}%")
    return "  ".join(parts)


def _slo_violation_y_step(y_top: float, *, target_ticks: int = 5) -> float:
    import math

    if y_top <= 0:
        return 1e-4
    magnitude = 10 ** math.floor(math.log10(y_top))
    nice_steps = [
        magnitude * scale * mult
        for scale in (1, 0.1, 0.01)
        for mult in (1, 2, 5)
    ]
    best_step = nice_steps[0]
    best_error = float("inf")
    for candidate in nice_steps:
        tick_count = y_top / candidate
        if tick_count < 2:
            continue
        error = abs(tick_count - target_ticks)
        if error < best_error:
            best_error = error
            best_step = candidate
    return best_step


def _configure_slo_violation_y_axis(
    ax,
    series: list[tuple[str, list[float]]],
    style: PlotStyle,
) -> None:
    all_y = [v for _, ys in series for v in ys]
    if not all_y or max(all_y) == 0.0:
        ax.set_ylim(0.0, 1e-4)
        ax.set_yticks([0.0])
        ax.set_yticklabels(["0"], fontsize=style.font_size - 1)
        return

    y_max = min(1.0, max(all_y))
    pad = style.axis_guard_fraction * y_max
    y_top = min(1.0, y_max + pad)
    y_step = _slo_violation_y_step(y_top)
    configure_y_axis_ticks(
        ax,
        y_data=all_y,
        style=style,
        ylim=(0.0, y_top),
        y_step=y_step,
    )
    ax.set_ylim(0.0, y_top)


def default_output_path(sweep: str, metric: str) -> Path:
    sweep_slug = sweep.replace("-", "_")
    metric_slug = metric.replace("-", "_")
    return DEFAULT_OUTPUT_DIR / f"lb_{sweep_slug}_{metric_slug}.pdf"


def run_lb_sweep(
    binary: Path,
    sweep: str,
    sweep_values: list[Any],
    series_values: list[str],
    *,
    base_kwargs: dict[str, Any],
    sweep_spec: ParamSpec,
    series_spec: ParamSpec,
    metric: str,
    slo: float | None,
    output_format: str,
) -> list[tuple[str, list[float]]]:
    series: list[tuple[str, list[float]]] = [(str(v), []) for v in series_values]
    series_index = {str(v): idx for idx, v in enumerate(series_values)}
    pairs = list(product(series_values, sweep_values))

    for series_val, sweep_val in tqdm(
        pairs,
        desc=f"{series_spec.name} × {sweep_spec.name} sweep",
        unit="run",
    ):
        sim_kwargs = {
            **base_kwargs,
            sweep_spec.sim_key: sweep_val,
            series_spec.sim_key: series_val,
        }
        data = run_simulation(binary, **sim_kwargs)
        if not data["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        metric_value = extract_metric(data, metric, slo=slo)
        label = series_label(series_spec.name, series_val)
        series[series_index[label]][1].append(metric_value)
        summary = format_run_summary(
            sim_kwargs=sim_kwargs,
            metric_name=metric,
            metric_value=metric_value,
            data=data,
        )
        if output_format == "human":
            tqdm.write(summary)
            tqdm.write(format_percentile_table("e2e latency (s):", data["e2e"]))
        else:
            tqdm.write(summary)
    return series


def plot_sweep(
    sweep_values: list[Any],
    series: list[tuple[str, list[float]]],
    *,
    sweep_spec: ParamSpec,
    metric: str,
    output_path: Path,
    title: str | None = None,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    if sweep_spec.use_index_x:
        x_values = list(range(len(sweep_values)))
        tick_labels = [
            sweep_spec.tick_label(v) if sweep_spec.tick_label else str(v)
            for v in sweep_values
        ]
    else:
        x_values = sweep_values
        tick_labels = [f"{v:g}" if isinstance(v, float) else str(v) for v in sweep_values]

    for color_idx, (label, y_values) in enumerate(series):
        plot_line(
            ax,
            x_values,
            y_values,
            label=label,
            style=style,
            color_idx=color_idx,
            show_markers=True,
        )

    ax.set_xticks(x_values)
    ax.set_xticklabels(tick_labels)
    if not sweep_spec.use_index_x and all(isinstance(v, (int, float)) for v in sweep_values):
        ax.set_xlim(min(sweep_values), max(sweep_values))

    ylabel = metric_ylabel(metric)
    if parse_metric(metric)[0] == "slo-violation":
        _configure_slo_violation_y_axis(ax, series, style)

    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel=sweep_spec.xlabel,
        ylabel=ylabel,
        title=title or "",
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def plot_title(args: argparse.Namespace, sweep: str) -> str | None:
    parts: list[str] = []
    if sweep != "load":
        parts.append(f"load={fixed_param_value(args, 'load'):g}")
    if sweep != "clients":
        parts.append(f"clients={fixed_param_value(args, 'clients')}")
    if sweep != "servers":
        parts.append(f"servers={fixed_param_value(args, 'servers')}")
    return "  ".join(parts) if parts else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sweep an lb simulator parameter and plot a metric per LB policy.",
    )
    parser.add_argument(
        "--sweep",
        choices=SWEEP_CHOICES,
        default="load",
        help="Parameter varied along the x-axis (default: load)",
    )
    parser.add_argument(
        "--series",
        choices=SERIES_CHOICES,
        default="lb-policy",
        help="Parameter with one plot line each (default: lb-policy)",
    )
    parser.add_argument(
        "--metric",
        default="slo-violation",
        help="Y-axis metric: p99, p50, p90, utilization, slo-violation, or p{N}",
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
        help="Output PDF path (default: output/lb_{sweep}_{metric}.pdf)",
    )
    parser.add_argument(
        "--comment",
        type=str,
        default=None,
        help="Suffix appended to output filename before .pdf",
    )
    parser.add_argument(
        "--load",
        type=float,
        nargs="*",
        default=None,
        help="Load value(s); multiple values when --sweep load, else fixed load",
    )
    parser.add_argument("--load-min", type=float, default=0.1)
    parser.add_argument("--load-max", type=float, default=1.0)
    parser.add_argument("--load-step", type=float, default=0.1)
    parser.add_argument(
        "--clients",
        type=int,
        nargs="*",
        default=None,
        help="Client count(s); multiple when --sweep clients, else fixed",
    )
    parser.add_argument("--clients-min", type=int, default=1)
    parser.add_argument("--clients-max", type=int, default=8)
    parser.add_argument("--clients-step", type=int, default=1)
    parser.add_argument(
        "--servers",
        type=int,
        nargs="*",
        default=None,
        help="Server count(s); multiple when --sweep servers, else fixed",
    )
    parser.add_argument("--servers-min", type=int, default=1)
    parser.add_argument("--servers-max", type=int, default=8)
    parser.add_argument("--servers-step", type=int, default=1)
    parser.add_argument(
        "--concurrency",
        type=int,
        nargs="*",
        default=None,
        help="Concurrency value(s); multiple when --sweep concurrency, else fixed",
    )
    parser.add_argument("--concurrency-min", type=int, default=1)
    parser.add_argument("--concurrency-max", type=int, default=4)
    parser.add_argument("--concurrency-step", type=int, default=1)
    parser.add_argument(
        "--lb-subset-size",
        type=int,
        nargs="*",
        default=None,
        help="Subset size(s); multiple when --sweep lb-subset-size, else fixed (0=all)",
    )
    parser.add_argument("--subset-min", type=int, default=None)
    parser.add_argument("--subset-max", type=int, default=None)
    parser.add_argument("--subset-step", type=int, default=1)
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
        nargs="+",
        default=list(LB_POLICIES),
        help="LB policies to compare when --series lb-policy (default: all)",
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
    parser.add_argument(
        "--format",
        choices=["human", "compact"],
        default="compact",
        help="human: summary + e2e latency percentiles; compact: one line per run",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sweep == args.series:
        raise SystemExit("--sweep and --series must name different parameters")

    parse_metric(args.metric)

    sweep_spec = SWEEP_PARAMS[args.sweep]
    series_spec = SERIES_PARAMS[args.series]

    sweep_values = resolve_sweep_values(args, args.sweep)
    if not sweep_values:
        raise SystemExit(f"no values in sweep range for {args.sweep}")

    if args.series == "lb-policy":
        series_values = list(args.lb_policy)
    else:
        raise SystemExit(f"unsupported series parameter: {args.series}")

    if "prequal" in series_values:
        if args.sweep == "lb-subset-size":
            raise SystemExit(
                "--lb-policy prequal is incompatible with --sweep lb-subset-size"
            )
        fixed_subset = fixed_param_value(args, "lb-subset-size")
        if int(fixed_subset) > 0:
            raise SystemExit(
                "--lb-subset-size is not supported with --lb-policy prequal"
            )

    if args.no_build:
        binary = args.binary or DEFAULT_BINARY
    else:
        binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="lb")

    if not binary.is_file():
        raise SystemExit(f"lb binary not found: {binary}")

    base_kwargs = base_sim_kwargs(args, args.sweep)
    series = run_lb_sweep(
        binary,
        args.sweep,
        sweep_values,
        series_values,
        base_kwargs=base_kwargs,
        sweep_spec=sweep_spec,
        series_spec=series_spec,
        metric=args.metric,
        slo=args.slo,
        output_format=args.format,
    )

    output_path = args.output or default_output_path(args.sweep, args.metric)
    output_path = output_path_with_comment(output_path, args.comment)
    title = plot_title(args, args.sweep)
    plot_sweep(
        sweep_values,
        series,
        sweep_spec=sweep_spec,
        metric=args.metric,
        output_path=output_path,
        title=title,
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
