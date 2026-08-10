#!/usr/bin/env python3
"""Scatter of per-tier average occupancy for MS configs at a single load.

Requires --chain {3,6,10}. X-axis is microservice tier; Y-axis is mean
server_avg_queue_inflight over that tier's replicas. One point per config at
each tier.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-ms-occupancy-compare-plot-cache"
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
    MS_SERVICE_DISTS,
    REPO_ROOT,
    ensure_release_binary,
    output_path_with_comment,
    run_ms_simulation,
)
from plot_ms_chain_load_compare import (
    CHAIN_FIXTURES,
    MsExperimentConfig,
    resolve_config_rps,
    select_configs,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    distinct_series_styles,
)

DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"

# Placeholder configs — edit to compare the policies you care about.
DEFAULT_CONFIGS: list[MsExperimentConfig] = [
    MsExperimentConfig("CQ", "centralized"),
    MsExperimentConfig("P2C", "power-of-two"),
    MsExperimentConfig("LR", "least-request"),
    MsExperimentConfig("RR", "round-robin"),
    MsExperimentConfig("R", "random"),
    MsExperimentConfig("Approx", "approx", pull_policy="least-request"),
]


def _log(message: str) -> None:
    write = getattr(tqdm, "write", None)
    if write is None:
        print(message, file=sys.stderr)
    else:
        write(message)


def microservice_order(data: dict) -> list[str]:
    order = data.get("microservice_order")
    if order is not None:
        return list(order)
    raise SystemExit("ms JSON missing microservice_order; rebuild the ms binary")


def per_tier_average_occupancy(data: dict, microservices: list[str]) -> np.ndarray:
    by_ms = data.get("server_avg_queue_inflight") or {}
    if not by_ms:
        raise SystemExit("ms JSON missing server_avg_queue_inflight")
    values: list[float] = []
    for ms in microservices:
        if ms not in by_ms:
            raise SystemExit(f"ms JSON missing server_avg_queue_inflight for {ms}")
        replica_avgs = [float(v) for v in by_ms[ms].values()]
        if not replica_avgs:
            raise SystemExit(f"server_avg_queue_inflight has no replicas for {ms}")
        values.append(float(np.mean(replica_avgs)))
    return np.asarray(values, dtype=float)


def format_run_summary(
    *,
    config: MsExperimentConfig,
    load: float,
    rps: float,
    tier_occupancy: np.ndarray,
) -> str:
    parts = [
        f"label={config.label}",
        f"load={load:g}",
        f"policy={config.lb_policy}",
        f"k={config.lb_subset_size}",
        f"scheduling={config.scheduling}",
    ]
    if config.pull_policy is not None:
        parts.append(f"pull_policy={config.pull_policy}")
    if config.approx_sched is not None:
        parts.append(f"approx_sched={config.approx_sched}")
    if config.lb_policy == "centralized" and config.centralized_sched != "fcfs":
        parts.append(f"centralized_sched={config.centralized_sched}")
    if config.scale is not None:
        parts.append(f"scale={config.scale}")
    parts.append(f"rps={rps:g}")
    tier_str = ",".join(f"{v:.3f}" for v in tier_occupancy)
    parts.append(f"tier_avg_occupancy=[{tier_str}]")
    return "  ".join(parts)


def run_occupancy_compare(
    binary: Path,
    configs: list[MsExperimentConfig],
    *,
    load: float,
    callgraph: Path,
    load_file: Path,
    n: int,
    seed: int | None,
    service_dist: str,
) -> tuple[list[str], list[tuple[str, np.ndarray]]]:
    """Return (microservices, [(label, per-tier avg occupancy)])."""
    microservices: list[str] | None = None
    series: list[tuple[str, np.ndarray]] = []

    for config in tqdm(configs, desc="config", unit="run"):
        rps = load * resolve_config_rps(config)
        data = run_ms_simulation(
            binary,
            callgraph=callgraph,
            load_file=load_file,
            n=n,
            lb_policy=config.lb_policy,
            pull_policy=config.pull_policy,
            lb_subset_size=config.lb_subset_size,
            scheduling=config.scheduling,
            centralized_sched=config.centralized_sched,
            seed=seed,
            rps=rps,
            service_dist=service_dist,
            approx_sched=config.approx_sched,
            scale=config.scale,
        )
        order = microservice_order(data)
        if microservices is None:
            microservices = order
        elif order != microservices:
            raise SystemExit(
                f"microservice_order mismatch for {config.label!r}: "
                f"{order} vs {microservices}"
            )
        tier_occupancy = per_tier_average_occupancy(data, microservices)
        series.append((config.label, tier_occupancy))
        _log(
            format_run_summary(
                config=config,
                load=load,
                rps=rps,
                tier_occupancy=tier_occupancy,
            )
        )

    if microservices is None:
        raise SystemExit("no simulations ran")
    return microservices, series


def plot_occupancy_scatter(
    microservices: list[str],
    series: list[tuple[str, np.ndarray]],
    *,
    output_path: Path,
) -> None:
    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    n_tiers = len(microservices)
    positions = list(range(n_tiers))
    series_styles = distinct_series_styles(len(series), style)
    all_y: list[float] = []

    for cfg_idx, (label, tier_occupancy) in enumerate(series):
        line_style = series_styles[cfg_idx]
        ys = [float(v) for v in tier_occupancy]
        all_y.extend(ys)
        ax.scatter(
            positions,
            ys,
            label=label,
            color=line_style["color"],
            marker=line_style["marker"],
            s=(style.marker_size * 1.8) ** 2,
            edgecolors="black",
            linewidths=0.5,
            zorder=3,
        )

    y_hi = max(all_y) if all_y else 1.0
    if y_hi <= 0:
        y_hi = 1.0
    y_pad = style.axis_guard_fraction * y_hi

    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    ax.set_xlim(-0.5, n_tiers - 0.5)
    ax.set_ylim(0.0, y_hi + y_pad)

    grid.configure_ax(
        ax,
        xlabel="Microservice tier",
        ylabel="Average Occupancy",
        title="",
        show_xlabel=True,
        show_ylabel=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def default_output_path(
    chain: int,
    *,
    scale: int | None = None,
    lb_subset_size: int | None = None,
) -> Path:
    name = f"ms_chain{chain}_occupancy_compare"
    if scale is not None and scale != 0:
        name += f"_scale{scale}"
    if lb_subset_size is not None:
        name += f"_k{lb_subset_size}"
    return DEFAULT_OUTPUT_DIR / f"{name}.pdf"


def resolve_fixtures(args: argparse.Namespace) -> tuple[Path, Path]:
    default_callgraph, default_load = CHAIN_FIXTURES[args.chain]
    callgraph = args.callgraph if args.callgraph is not None else default_callgraph
    load_file = args.load_file if args.load_file is not None else default_load
    return callgraph, load_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scatter of per-tier average occupancy for MS experiment configs "
            "at a single load."
        ),
    )
    parser.add_argument(
        "--chain",
        type=int,
        choices=[3, 6, 10],
        required=True,
        help="Chain depth / fixture set (required: 3, 6, or 10)",
    )
    parser.add_argument(
        "--callgraph",
        type=Path,
        default=None,
        help="Override callgraph.json for the selected chain",
    )
    parser.add_argument(
        "--load-file",
        type=Path,
        default=None,
        help="Override load.json for the selected chain",
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=None,
        help=(
            "Override scale for all configs "
            "(add this many cpu cores and replicas to every microservice)"
        ),
    )
    parser.add_argument(
        "--lb-subset-size",
        type=int,
        default=None,
        help=(
            "Override lb-subset-size for all configs "
            "(0 = all replicas; ignores per-config values)"
        ),
    )
    parser.add_argument(
        "--binary",
        type=Path,
        default=None,
        help="Prebuilt ms release binary (skips cargo build --release)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PDF path",
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
        default=0.7,
        help="Single load level (simulator rps = load × config rps; default: 0.7)",
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=None,
        help=(
            "Override base rps for all configs "
            "(simulator rps = load × rps)"
        ),
    )
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument(
        "--config-index",
        type=int,
        nargs="+",
        default=None,
        metavar="I",
        help="Run only these DEFAULT_CONFIGS indices (0-based)",
    )
    parser.add_argument(
        "--service-dist",
        choices=MS_SERVICE_DISTS,
        default="exp",
        help="Service-time distribution (default: exp)",
    )
    parser.add_argument("--seed", type=int, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.load <= 0:
        raise SystemExit(f"--load must be > 0 (got {args.load})")
    configs = select_configs(
        DEFAULT_CONFIGS,
        args.config_index,
        lb_subset_size=args.lb_subset_size,
        scale=args.scale,
        rps=args.rps,
    )

    callgraph, load_file = resolve_fixtures(args)
    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")

    microservices, series = run_occupancy_compare(
        binary,
        configs,
        load=args.load,
        callgraph=callgraph,
        load_file=load_file,
        n=args.n,
        seed=args.seed,
        service_dist=args.service_dist,
    )

    output_path = args.output or default_output_path(
        args.chain,
        scale=args.scale,
        lb_subset_size=args.lb_subset_size,
    )
    output_path = output_path_with_comment(output_path, args.comment)
    plot_occupancy_scatter(
        microservices,
        series,
        output_path=output_path,
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
