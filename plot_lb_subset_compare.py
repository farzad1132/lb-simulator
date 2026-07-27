#!/usr/bin/env python3
"""Compare lb experiment configs while sweeping lb-subset-size on the x-axis.

Each config may differ in policy, topology, pull_policy, and approx_sched.
All configs share the same subset-size values from --lb-subset-size.
ExperimentConfig.lb_subset_size is ignored (the sweep supplies k).
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

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-subset-compare-plot-cache"
_MPL_CACHE = _CACHE_ROOT / "matplotlib"
_XDG_CACHE = _CACHE_ROOT / "xdg"
_MPL_CACHE.mkdir(parents=True, exist_ok=True)
_XDG_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))
os.environ.setdefault("XDG_CACHE_HOME", str(_XDG_CACHE))
os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np
from tqdm import tqdm

from lb_plot_configs import (
    ExperimentConfig,
    select_configs,
    uses_express_lane,
    uses_pull_policy,
    uses_work_shedding,
)
from plot_cdfs import (
    REPO_ROOT,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plot_lb_sweep import (
    extract_metric,
    metric_ylabel,
    parse_metric,
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
    ExperimentConfig("CQ", "centralized", 96, 24),
    ExperimentConfig("P2C", "power-of-two", 96, 24),
    ExperimentConfig("LR", "least-request", 96, 24),
    ExperimentConfig("R", "random", 96, 24),
    ExperimentConfig("RR", "round-robin", 96, 24),
    ExperimentConfig("Approx", "approx", 96, 24, pull_policy="least-request"),
    ExperimentConfig("Approx-FCFS", "approx", 96, 24, pull_policy="least-request", approx_sched="fcfs"),
]


def validate_subset_sweep(
    configs: list[ExperimentConfig],
    subset_sizes: list[int],
    *,
    clients_override: int | None = None,
    servers_override: int | None = None,
) -> None:
    for config in configs:
        clients = clients_override if clients_override is not None else config.clients
        servers = servers_override if servers_override is not None else config.servers
        for k in subset_sizes:
            if k < 0:
                raise SystemExit(f"--lb-subset-size values must be >= 0 (got {k})")
            if config.lb_policy == "prequal" and k > 0:
                raise SystemExit(
                    f"config {config.label!r}: lb_subset_size > 0 is not supported "
                    "with lb_policy prequal"
                )
            if config.lb_policy == "centralized" and k > 0:
                effective_k = min(k, servers)
                if servers % effective_k != 0:
                    raise SystemExit(
                        f"config {config.label!r}: --lb-subset-size {k} must evenly "
                        f"divide servers={servers} with centralized policy"
                    )
                subset_count = servers // effective_k
                if clients % subset_count != 0:
                    raise SystemExit(
                        f"config {config.label!r}: clients={clients} must be divisible "
                        f"by subset count {subset_count} "
                        f"(servers/subset-size) with centralized policy "
                        f"(k={k})"
                    )


def format_run_summary(
    *,
    config: ExperimentConfig,
    subset_size: int,
    metric_name: str,
    metric_value: float,
    data: dict[str, Any],
    clients: int,
    servers: int,
    load: float,
) -> str:
    kind, pct = parse_metric(metric_name)
    k_label = "all" if subset_size == 0 else str(subset_size)
    parts = [
        f"label={config.label}",
        f"k={k_label}",
        f"load={load:g}",
        f"policy={config.lb_policy}",
        f"servers={servers}",
        f"clients={clients}",
    ]
    if uses_pull_policy(config):
        parts.append(f"pull_policy={config.pull_policy}")
    if config.approx_sched is not None:
        parts.append(f"approx_sched={config.approx_sched}")
    if uses_express_lane(config):
        parts.append(f"express_size={config.express_size}")
        if config.express_del_th is not None:
            parts.append(f"express_del_th={config.express_del_th:g}")
        if config.express_th is not None:
            parts.append(f"express_th={config.express_th}")
        if config.ideal:
            parts.append("ideal")
    if uses_work_shedding(config):
        parts.append(f"shed_delay={config.shed_delay:g}")
    if kind == "utilization":
        parts.append(f"utilization={metric_value:.1f}%")
    elif kind == "slo-violation":
        parts.append(f"P(latency>SLO)={metric_value:.6f}")
    else:
        parts.append(f"p{int(pct)}={metric_value:.6f}s")
    parts.append(f"utilization={data['utilization_pct']:.1f}%")
    return "  ".join(parts)


def run_subset_sweep(
    binary: Path,
    configs: list[ExperimentConfig],
    subset_sizes: list[int],
    *,
    load: float,
    base_kwargs: dict[str, Any],
    metric: str,
    slo: float | None,
    clients_override: int | None = None,
    servers_override: int | None = None,
) -> list[tuple[str, list[float]]]:
    """Return (label, y metric values) per config; x is shared subset_sizes."""
    series: list[tuple[str, list[float]]] = [
        (config.label, []) for config in configs
    ]
    pairs = list(product(configs, subset_sizes))

    for config, subset_size in tqdm(
        pairs,
        desc="config × subset-size",
        unit="run",
    ):
        clients = clients_override if clients_override is not None else config.clients
        servers = servers_override if servers_override is not None else config.servers
        sim_kwargs = {
            **base_kwargs,
            "load": load,
            "lb_policy": config.lb_policy,
            "clients": clients,
            "servers": servers,
            "concurrency": config.concurrency,
            "lb_subset_size": subset_size,
        }
        if uses_pull_policy(config):
            sim_kwargs["pull_policy"] = config.pull_policy
            if config.approx_sched is not None:
                sim_kwargs["approx_sched"] = config.approx_sched
        if uses_express_lane(config):
            sim_kwargs.update(
                expresslane=True,
                express_size=config.express_size,
                express_del_th=config.express_del_th,
                express_th=config.express_th,
                ideal=config.ideal,
            )
        if uses_work_shedding(config):
            sim_kwargs["shed_delay"] = config.shed_delay
        data = run_simulation(binary, **sim_kwargs)
        if not data["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        metric_value = extract_metric(data, metric, slo=slo)
        idx = configs.index(config)
        series[idx][1].append(metric_value)
        tqdm.write(
            format_run_summary(
                config=config,
                subset_size=subset_size,
                metric_name=metric,
                metric_value=metric_value,
                data=data,
                clients=clients,
                servers=servers,
                load=load,
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


def plot_subset_compare(
    subset_sizes: list[int],
    series: list[tuple[str, list[float]]],
    *,
    metric: str,
    output_path: Path,
    title: str | None = None,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    x_values = list(range(len(subset_sizes)))
    tick_labels = ["all" if k == 0 else str(k) for k in subset_sizes]

    series_styles = distinct_series_styles(len(series), style)
    for i, (label, y_values) in enumerate(series):
        line_style = series_styles[i]
        plot_line(
            ax,
            x_values,
            y_values,
            label=label,
            style=style,
            show_markers=True,
            color=line_style["color"],
            marker=line_style["marker"],
            linestyle=line_style["linestyle"],
        )

    ax.set_xticks(x_values)
    ax.set_xticklabels(tick_labels)

    all_y = [v for _, ys in series for v in ys]
    if all_y:
        y_max = max(all_y)
        y_floor = 0.0
        pad = style.axis_guard_fraction * y_max if y_max > 0 else 0.0
        y_top = y_max + pad
        y_step = _nice_axis_step(y_floor, y_top, min_ticks=5)
        configure_y_axis_ticks(
            ax,
            y_data=all_y,
            style=style,
            ylim=(y_floor, y_top),
            y_step=y_step,
        )
        ax.set_ylim(y_floor, y_top)

    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel="LB subset size",
        ylabel=metric_ylabel(metric),
        title=title or "",
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def plot_title(
    *,
    load: float,
    clients: int | str,
    servers: int | str,
) -> str:
    return f"load={load:g}  clients={clients}  servers={servers}"


def resolve_title_topology(
    configs: list[ExperimentConfig],
    *,
    clients_override: int | None,
    servers_override: int | None,
) -> tuple[int | str, int | str]:
    if clients_override is not None:
        clients: int | str = clients_override
    else:
        client_values = {c.clients for c in configs}
        clients = client_values.pop() if len(client_values) == 1 else "mixed"

    if servers_override is not None:
        servers: int | str = servers_override
    else:
        server_values = {c.servers for c in configs}
        servers = server_values.pop() if len(server_values) == 1 else "mixed"

    return clients, servers


def default_output_path(metric: str) -> Path:
    metric_slug = metric.replace("-", "_")
    return DEFAULT_OUTPUT_DIR / f"lb_subset_compare_{metric_slug}.pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare lb experiment configs while sweeping lb-subset-size on the x-axis."
        ),
    )
    parser.add_argument(
        "--lb-subset-size",
        type=int,
        nargs="+",
        required=True,
        metavar="K",
        help="Subset sizes for x-axis (e.g. 1 2 3 4 6 12); 0 = full pool",
    )
    parser.add_argument(
        "--load",
        type=float,
        default=0.8,
        help="Fixed target utilization (default: 0.8)",
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
        "--config-index",
        type=int,
        nargs="+",
        default=None,
        metavar="I",
        help="Run only these DEFAULT_CONFIGS indices (0-based)",
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
    parser.add_argument(
        "--clients",
        type=int,
        default=None,
        help="Override number of clients for all configs",
    )
    parser.add_argument(
        "--servers",
        type=int,
        default=None,
        help="Override number of servers for all configs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    parse_metric(args.metric)

    configs = select_configs(DEFAULT_CONFIGS, args.config_index)
    subset_sizes = list(args.lb_subset_size)
    if not subset_sizes:
        raise SystemExit("--lb-subset-size requires at least one value")

    validate_subset_sweep(
        configs,
        subset_sizes,
        clients_override=args.clients,
        servers_override=args.servers,
    )

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

    series = run_subset_sweep(
        binary,
        configs,
        subset_sizes,
        load=args.load,
        base_kwargs=base_kwargs,
        metric=args.metric,
        slo=args.slo,
        clients_override=args.clients,
        servers_override=args.servers,
    )

    output_path = args.output or default_output_path(args.metric)
    output_path = output_path_with_comment(output_path, args.comment)
    clients, servers = resolve_title_topology(
        configs,
        clients_override=args.clients,
        servers_override=args.servers,
    )
    plot_subset_compare(
        subset_sizes,
        series,
        metric=args.metric,
        output_path=output_path,
        title=plot_title(load=args.load, clients=clients, servers=servers),
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
