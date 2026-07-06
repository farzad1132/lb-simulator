#!/usr/bin/env python3
"""Compare centralized vs push LB policies at equal offered load (task/s).

Each experiment config may use a different server count; --load is scaled so
all configs share the same total arrival rate on the x-axis.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from itertools import product
from pathlib import Path
from typing import Any

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-centralized-compare-plot-cache"
_MPL_CACHE = _CACHE_ROOT / "matplotlib"
_XDG_CACHE = _CACHE_ROOT / "xdg"
_MPL_CACHE.mkdir(parents=True, exist_ok=True)
_XDG_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))
os.environ.setdefault("XDG_CACHE_HOME", str(_XDG_CACHE))
os.environ.setdefault("MPLBACKEND", "Agg")

from tqdm import tqdm
import numpy as np

from lb_plot_configs import ExperimentConfig, select_configs, uses_express_lane
from plot_cdfs import (
    REPO_ROOT,
    SERVICE_MEAN,
    bimodal_service_mean,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plot_lb_sweep import (
    extract_metric,
    metric_ylabel,
    parse_metric,
    range_values,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    configure_y_axis_ticks,
    distinct_series_styles,
    plot_line,
)

DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"

DEFAULT_CONFIGS: list[ExperimentConfig] = [
    ExperimentConfig("CQ-10", "centralized", 10, 10),
    ExperimentConfig("P2C-10", "power-of-two", 10, 10),
    ExperimentConfig("P2C-11", "power-of-two", 10, 11),
    ExperimentConfig("P2C-12", "power-of-two", 10, 12),
    ExperimentConfig("P2C-13", "power-of-two", 10, 13),
    ExperimentConfig("P2C-14", "power-of-two", 10, 14)
]


def load_for_arrival_rate(
    arrival_rate: float,
    servers: int,
    concurrency: int = 1,
    *,
    service_mean: float = SERVICE_MEAN,
) -> float:
    capacity = max(servers, 1) * max(concurrency, 1)
    return arrival_rate * service_mean / capacity


def reference_arrival_rates(
    ref_load_min: float,
    ref_load_max: float,
    ref_load_step: float,
    *,
    ref_servers: int,
    ref_concurrency: int = 1,
    service_mean: float = SERVICE_MEAN,
) -> list[float]:
    ref_loads = range_values(
        ref_load_min,
        ref_load_max,
        ref_load_step,
        value_type=float,
        step_flag="--ref-load-step",
    )
    ref_capacity = max(ref_servers, 1) * max(ref_concurrency, 1)
    return [
        round(load * ref_capacity / service_mean, 1)
        for load in ref_loads
    ]


def resolve_service_mean(args: argparse.Namespace) -> float:
    if args.service_dist == "bimodal":
        if args.service_modes is None or args.service_mode_probs is None:
            raise SystemExit(
                "--service-modes and --service-mode-probs are required "
                "with --service-dist bimodal"
            )
        return bimodal_service_mean(args.service_modes, args.service_mode_probs)
    return SERVICE_MEAN


def format_run_summary(
    *,
    config: ExperimentConfig,
    arrival_rate: float,
    load: float,
    metric_name: str,
    metric_value: float,
    data: dict[str, Any],
) -> str:
    kind, pct = parse_metric(metric_name)
    measured_rate = float(data["total_arrival_rate"])
    parts = [
        f"label={config.label}",
        f"rate={arrival_rate:.1f} task/s",
        f"load={load:g}",
        f"servers={config.servers}",
        f"clients={config.clients}",
        f"measured_rate={measured_rate:.4f}",
    ]
    if uses_express_lane(config):
        parts.append(f"express_size={config.express_size}")
        if config.express_del_th is not None:
            parts.append(f"express_del_th={config.express_del_th:g}")
        if config.express_th is not None:
            parts.append(f"express_th={config.express_th}")
        if config.ideal:
            parts.append("ideal")
    if kind == "utilization":
        parts.append(f"utilization={metric_value:.1f}%")
    else:
        parts.append(f"p{int(pct)}={metric_value:.6f}s")
    parts.append(f"utilization={data['utilization_pct']:.1f}%")
    return "  ".join(parts)


def run_comparison_sweep(
    binary: Path,
    configs: list[ExperimentConfig],
    arrival_rates: list[float],
    *,
    base_kwargs: dict[str, Any],
    service_mean: float,
    metric: str,
    slo: float | None,
) -> list[tuple[str, list[float], list[float]]]:
    """Return (label, x arrival rates, y metric values) per config."""
    series: list[tuple[str, list[float], list[float]]] = [
        (config.label, [], []) for config in configs
    ]
    pairs = list(product(configs, arrival_rates))

    for config, arrival_rate in tqdm(
        pairs,
        desc="config × arrival rate",
        unit="run",
    ):
        load = load_for_arrival_rate(
            arrival_rate,
            config.servers,
            config.concurrency,
            service_mean=service_mean,
        )
        sim_kwargs = {
            **base_kwargs,
            "load": load,
            "lb_policy": config.lb_policy,
            "clients": config.clients,
            "servers": config.servers,
            "concurrency": config.concurrency,
            "lb_subset_size": config.lb_subset_size,
        }
        if uses_express_lane(config):
            sim_kwargs.update(
                expresslane=True,
                express_size=config.express_size,
                express_del_th=config.express_del_th,
                express_th=config.express_th,
                ideal=config.ideal,
            )
        data = run_simulation(binary, **sim_kwargs)
        if not data["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        metric_value = extract_metric(data, metric, slo=slo)
        idx = configs.index(config)
        series[idx][1].append(arrival_rate)
        series[idx][2].append(metric_value)
        tqdm.write(
            format_run_summary(
                config=config,
                arrival_rate=arrival_rate,
                load=load,
                metric_name=metric,
                metric_value=metric_value,
                data=data,
            )
        )
    return series


def _y_ticks_in_range(y_min: float, y_max: float, step: float) -> list[float]:
    tick_start = math.floor(y_min / step) * step
    tick_end = math.ceil(y_max / step) * step
    ticks = np.arange(tick_start, tick_end + step / 2, step)
    return [float(t) for t in ticks if y_min - 1e-9 <= t <= y_max + 1e-9]


def _nice_axis_step(y_min: float, y_max: float, min_ticks: int = 5) -> float:
    span = y_max - y_min
    if span <= 0:
        return 1.0
    raw = span / max(min_ticks - 1, 1)
    magnitude = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    candidates: list[float] = []
    for scale in (0.01, 0.1, 1, 10):
        for mult in (1, 2, 5, 10):
            step = mult * magnitude * scale
            if step > 0:
                candidates.append(step)
    valid = [
        step
        for step in sorted(set(candidates))
        if len(_y_ticks_in_range(y_min, y_max, step)) >= min_ticks
    ]
    if valid:
        return max(valid)
    return span / max(min_ticks - 1, 1)


def plot_comparison(
    arrival_rates: list[float],
    series: list[tuple[str, list[float], list[float]]],
    *,
    metric: str,
    output_path: Path,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    series_styles = distinct_series_styles(len(series), style)
    for i, (label, _xs, y_values) in enumerate(series):
        line_style = series_styles[i]
        plot_line(
            ax,
            arrival_rates,
            y_values,
            label=label,
            style=style,
            show_markers=True,
            color=line_style["color"],
            marker=line_style["marker"],
            linestyle=line_style["linestyle"],
        )

    ax.set_xticks(arrival_rates)
    ax.set_xticklabels([f"{rate:.1f}" for rate in arrival_rates])
    ax.set_xlim(min(arrival_rates), max(arrival_rates))

    all_y = [v for _, _, ys in series for v in ys]
    if all_y:
        y_min = min(all_y)
        y_max = 4 * y_min
        y_floor = 0.0
        y_step = _nice_axis_step(y_floor, y_max, min_ticks=5)
        configure_y_axis_ticks(
            ax,
            y_data=all_y,
            style=style,
            ylim=(y_floor, y_max),
            y_step=y_step,
        )
        ax.set_ylim(y_floor, y_max)

    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel="Total arrival rate (task/s)",
        ylabel=metric_ylabel(metric),
        title="",
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def default_output_path(metric: str) -> Path:
    metric_slug = metric.replace("-", "_")
    return DEFAULT_OUTPUT_DIR / f"lb_centralized_compare_{metric_slug}.pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare centralized vs push LB policies at equal offered load "
            "(task/s), scaling --load per server count."
        ),
    )
    parser.add_argument(
        "--metric",
        default="p99",
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
        help="Output PDF path",
    )
    parser.add_argument(
        "--comment",
        default=None,
        help="Suffix appended to output filename before .pdf",
    )
    parser.add_argument(
        "--ref-load-min",
        type=float,
        default=0.1,
        help="Reference load sweep minimum (default: 0.1)",
    )
    parser.add_argument(
        "--ref-load-max",
        type=float,
        default=0.9,
        help="Reference load sweep maximum (default: 0.9)",
    )
    parser.add_argument(
        "--ref-load-step",
        type=float,
        default=0.1,
        help="Reference load sweep step (default: 0.1)",
    )
    parser.add_argument(
        "--ref-servers",
        type=int,
        default=10,
        help="Reference server count for load-to-rate mapping (default: 10)",
    )
    parser.add_argument(
        "--ref-concurrency",
        type=int,
        default=1,
        help="Reference concurrency for load-to-rate mapping (default: 1)",
    )
    parser.add_argument(
        "--config-index",
        type=int,
        nargs="+",
        default=None,
        metavar="I",
        help="Run only these DEFAULT_CONFIGS indices (0-based)",
    )
    parser.add_argument("--n", type=int, default=100_000_0)
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
        help="Exponential means for bimodal modes",
    )
    parser.add_argument(
        "--service-mode-probs",
        type=float,
        nargs=2,
        metavar=("P0", "P1"),
        help="Mode selection probabilities for bimodal",
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

    configs = select_configs(DEFAULT_CONFIGS, args.config_index)
    service_mean = resolve_service_mean(args)
    arrival_rates = reference_arrival_rates(
        args.ref_load_min,
        args.ref_load_max,
        args.ref_load_step,
        ref_servers=args.ref_servers,
        ref_concurrency=args.ref_concurrency,
        service_mean=service_mean,
    )
    if not arrival_rates:
        raise SystemExit("no arrival rates in reference load range")

    if args.no_build:
        binary = args.binary or DEFAULT_BINARY
    else:
        binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="lb")

    if not binary.is_file():
        raise SystemExit(f"lb binary not found: {binary}")

    base_kwargs: dict[str, Any] = {
        "n": args.n,
        "service_dist": args.service_dist,
        "service_modes": args.service_modes,
        "service_mode_probs": args.service_mode_probs,
        "seed": args.seed,
        "slo": args.slo,
    }

    series = run_comparison_sweep(
        binary,
        configs,
        arrival_rates,
        base_kwargs=base_kwargs,
        service_mean=service_mean,
        metric=args.metric,
        slo=args.slo,
    )

    output_path = args.output or default_output_path(args.metric)
    output_path = output_path_with_comment(output_path, args.comment)
    plot_comparison(
        arrival_rates,
        series,
        metric=args.metric,
        output_path=output_path,
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
