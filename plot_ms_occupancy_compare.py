#!/usr/bin/env python3
"""Scatter of per-tier average queue length for MS configs at a single load.

Requires --chain {3,6,10}. X-axis is microservice tier; Y-axis is mean
server_avg_queue (queue length only; excludes in-flight) over that tier's
replicas. One point per config at each tier.

One RNG seed is used for every config. If --seed is omitted, a seed is picked
for this execution and logged.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import math
import os
import random
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
    EQ_SCALE_HELP,
    MsExperimentConfig,
    eq_scale_filename_suffix,
    format_eq_scale_parts,
    ms_eq_scale_kwargs,
    parse_eq_scale_specs,
    resolve_config_rps,
    resolve_config_service_dist,
    select_configs,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    distinct_series_styles,
)

DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"
Y_TICK_STEP = 0.5

# Placeholder configs — edit to compare the policies you care about.
DEFAULT_CONFIGS: list[MsExperimentConfig] = [
    MsExperimentConfig("CPull", "centralized"),
    #MsExperimentConfig("JBSQ-2", "jbsq", jbsq_n=2),
    #MsExperimentConfig("C-P2C", "cl"),
    #MsExperimentConfig("C-RR", "cl-lr"),
    #MsExperimentConfig("C-R", "cl-r"),
    #MsExperimentConfig("Prequal", "prequal"),
    MsExperimentConfig("P2C", "power-of-two"),
    MsExperimentConfig("P2C*", "power-of-two", eq_scale=90),
    #MsExperimentConfig("LR", "least-request"),
    MsExperimentConfig("RR", "round-robin"),
    #MsExperimentConfig("R", "random"),
    MsExperimentConfig("AmphiQueue", "amphiqueue", pull_policy="least-request"),
    MsExperimentConfig("AmphiQueue*", "amphiqueue", pull_policy="least-request", eq_scale=90),
    MsExperimentConfig("AmphiQueue*-K10", "amphiqueue", pull_policy="least-request", eq_scale=90, lb_subset_size=10),
    #MsExperimentConfig("AmphiQueue-FCFS", "amphiqueue", pull_policy="least-request", amphiqueue_sched="fcfs"),
    #MsExperimentConfig("AmphiQueue-EDF", "amphiqueue", pull_policy="least-request", amphiqueue_sched="edf"),
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


def per_tier_average_queue(data: dict, microservices: list[str]) -> np.ndarray:
    by_ms = data.get("server_avg_queue") or {}
    if not by_ms:
        raise SystemExit("ms JSON missing server_avg_queue; rebuild the ms binary")
    values: list[float] = []
    for ms in microservices:
        if ms not in by_ms:
            raise SystemExit(f"ms JSON missing server_avg_queue for {ms}")
        replica_avgs = [float(v) for v in by_ms[ms].values()]
        if not replica_avgs:
            raise SystemExit(f"server_avg_queue has no replicas for {ms}")
        values.append(float(np.mean(replica_avgs)))
    return np.asarray(values, dtype=float)


def format_run_summary(
    *,
    config: MsExperimentConfig,
    load: float,
    rps: float,
    tier_queue: np.ndarray,
    seed: int | None = None,
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
    if config.amphiqueue_sched is not None:
        parts.append(f"amphiqueue_sched={config.amphiqueue_sched}")
    if config.lb_policy in ("centralized", "jbsq") and config.centralized_sched != "fcfs":
        parts.append(f"centralized_sched={config.centralized_sched}")
    if config.jbsq_n is not None:
        parts.append(f"jbsq_n={config.jbsq_n}")
    if config.scale is not None:
        parts.append(f"scale={config.scale}")
    parts.extend(format_eq_scale_parts(config))
    if config.service_dist is not None:
        parts.append(f"service_dist={config.service_dist}")
    parts.append(f"rps={rps:g}")
    if seed is not None:
        parts.append(f"seed={seed}")
    tier_str = ",".join(f"{v:.3f}" for v in tier_queue)
    parts.append(f"tier_avg_queue=[{tier_str}]")
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
    default_service_dist: str = "exp",
) -> tuple[list[str], list[tuple[str, np.ndarray]]]:
    """Return (microservices, [(label, per-tier avg queue length)]).

    The same seed is used for every config.
    """
    microservices: list[str] | None = None
    series: list[tuple[str, np.ndarray]] = []
    if seed is not None:
        _log(f"shared seed: {seed}")

    for config in tqdm(configs, desc="config", unit="run"):
        rps = load * resolve_config_rps(config)
        service_dist = resolve_config_service_dist(
            config, default=default_service_dist
        )
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
            amphiqueue_sched=config.amphiqueue_sched,
            jbsq_n=config.jbsq_n,
            scale=config.scale,
            **ms_eq_scale_kwargs(config),
        )
        order = microservice_order(data)
        if microservices is None:
            microservices = order
        elif order != microservices:
            raise SystemExit(
                f"microservice_order mismatch for {config.label!r}: "
                f"{order} vs {microservices}"
            )
        tier_queue = per_tier_average_queue(data, microservices)
        series.append((config.label, tier_queue))
        _log(
            format_run_summary(
                config=config,
                load=load,
                rps=rps,
                tier_queue=tier_queue,
                seed=seed,
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
    #style = replace(style, aspect_ratio=)
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
            #s=(style.marker_size * 1.8) ** 2,
            edgecolors="black",
            linewidths=0.5,
            zorder=3,
        )

    y_lo = min(all_y) if all_y else 0.0
    y_hi = max(all_y) if all_y else Y_TICK_STEP
    # Snap to 0.5 ticks so axis bounds sit on tick marks with no extra pad.
    y_min = math.floor(y_lo / Y_TICK_STEP + 1e-12) * Y_TICK_STEP
    y_max = math.ceil(y_hi / Y_TICK_STEP - 1e-12) * Y_TICK_STEP
    if y_max <= y_min:
        y_max = y_min + Y_TICK_STEP

    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    ax.set_xlim(-0.5, n_tiers - 0.5)

    grid.configure_ax(
        ax,
        xlabel="Microservice Tier",
        ylabel="Average Queue Length",
        title="",
        show_xlabel=True,
        show_ylabel=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=True,
        y_step=Y_TICK_STEP,
        ylim=(y_min, y_max),
    )
    grid.add_shared_legend(position="top")
    grid.save(output_path)


def default_output_path(
    chain: int,
    *,
    scale: int | None = None,
    eq_scale: int | None = None,
    tier_eq_scale: tuple[tuple[str, int], ...] = (),
    lb_subset_size: int | None = None,
) -> Path:
    name = f"ms_chain{chain}_occupancy_compare"
    if scale is not None and scale != 0:
        name += f"_scale{scale}"
    name += eq_scale_filename_suffix(eq_scale, tier_eq_scale)
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
        "--eq-scale",
        nargs="+",
        default=None,
        metavar="SPEC",
        help=EQ_SCALE_HELP,
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
        default=None,
        help=(
            "Override service-time distribution for all configs "
            f"(choices: {', '.join(MS_SERVICE_DISTS)}; "
            "default: per-config or exp)"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "RNG seed shared by every config "
            "(default: pick a seed for this execution and log it)"
        ),
    )
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
        eq_scale_override=parse_eq_scale_specs(args.eq_scale),
        rps=args.rps,
        service_dist=args.service_dist,
    )

    callgraph, load_file = resolve_fixtures(args)
    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")

    if args.seed is None:
        args.seed = random.randrange(2**32)

    microservices, series = run_occupancy_compare(
        binary,
        configs,
        load=args.load,
        callgraph=callgraph,
        load_file=load_file,
        n=args.n,
        seed=args.seed,
    )

    output_path = args.output or default_output_path(
        args.chain,
        scale=args.scale,
        eq_scale=configs[0].eq_scale if configs else None,
        tier_eq_scale=configs[0].tier_eq_scale if configs else (),
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
