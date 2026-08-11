#!/usr/bin/env python3
"""Compare MS experiment configs on one chain topology while sweeping load.

Requires --chain {3,6,10}. X-axis is load; Y-axis is SLO violation rate (%).
One line per named config in DEFAULT_CONFIGS.

Each load level uses one shared RNG seed for every policy (derived from --seed
when set) so cross-policy comparisons at a given load are consistent.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import tempfile
from dataclasses import dataclass, replace
from itertools import product
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-ms-chain-load-compare-plot-cache"
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
    validate_prequal_subset,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    distinct_series_styles,
    plot_line,
)

DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"
CHAIN_FIXTURES = {
    3: (
        REPO_ROOT / "tests" / "chain" / "3" / "callgraph.json",
        REPO_ROOT / "tests" / "chain" / "3" / "load.json",
    ),
    6: (
        REPO_ROOT / "tests" / "chain" / "6" / "callgraph.json",
        REPO_ROOT / "tests" / "chain" / "6" / "load.json",
    ),
    10: (
        REPO_ROOT / "tests" / "chain" / "10" / "callgraph.json",
        REPO_ROOT / "tests" / "chain" / "10" / "load.json",
    ),
}
DEFAULT_RPS = 10_000.0
CALIBRATION_N = 300_000
SLO_UNLOADED_LATENCY_MULTIPLIER = 2.0


@dataclass(frozen=True)
class MsExperimentConfig:
    label: str
    lb_policy: str
    lb_subset_size: int = 0
    pull_policy: str | None = None
    approx_sched: str | None = None
    approx_share: int = 1  # replicas per sidecar; only for approx-share
    scheduling: str = "fifo"
    centralized_sched: str = "fcfs"  # centralized / jbsq shared pull queue
    jbsq_n: int | None = None  # required when lb_policy == jbsq
    scale: int | None = None
    rps: float | None = None  # base rate; simulator rps = load * rps
    service_dist: str | None = None  # None = CLI override or "exp"
    slo_ms: float | None = None  # None = calibrate from unloaded p99 × multiplier


def uses_approx_protocol(config: MsExperimentConfig) -> bool:
    return config.lb_policy in ("approx", "approx-share")


def uses_central_pull_queue(config: MsExperimentConfig) -> bool:
    return config.lb_policy in ("centralized", "jbsq")


# Placeholder configs — edit to compare the policies you care about.
DEFAULT_CONFIGS: list[MsExperimentConfig] = [
    MsExperimentConfig("CPull", "centralized"),
    MsExperimentConfig("JBSQ-1", "jbsq", jbsq_n=1),
    MsExperimentConfig("JBSQ-2", "jbsq", jbsq_n=2),
    MsExperimentConfig("JBSQ-5", "jbsq", jbsq_n=3),
    #MsExperimentConfig("JBSQ-2-EDF", "jbsq", jbsq_n=2, centralized_sched="edf"),
    MsExperimentConfig("CPush", "cl"),
    MsExperimentConfig("Prequal", "prequal"),
    #MsExperimentConfig("CQ-EDF", "centralized", centralized_sched="edf"),
    #MsExperimentConfig("CQ-K10", "centralized", lb_subset_size=10),
    #MsExperimentConfig("CQ-K20", "centralized", lb_subset_size=20),
    #MsExperimentConfig("P2C-K10", "power-of-two", lb_subset_size=10),
    #MsExperimentConfig("P2C-K20", "power-of-two", lb_subset_size=20),
    MsExperimentConfig("P2C", "power-of-two"),
    MsExperimentConfig("LR", "least-request"),
    MsExperimentConfig("WRR", "round-robin"),
    MsExperimentConfig("R", "random"),
    #MsExperimentConfig("Approx", "approx", pull_policy="least-request"),
    #MsExperimentConfig("Approx-S2", "approx-share", pull_policy="least-request", approx_share=2),
    #MsExperimentConfig("Approx-K10", "approx", pull_policy="least-request", lb_subset_size=10),
    #MsExperimentConfig("Approx-FCFS", "approx", pull_policy="least-request", approx_sched="fcfs",),
    #MsExperimentConfig("Approx-FCFS-S2", "approx-share", pull_policy="least-request", approx_sched="fcfs", approx_share=2),
    #MsExperimentConfig("Approx-FCFS-K10", "approx", pull_policy="least-request", approx_sched="fcfs", lb_subset_size=10),
    #MsExperimentConfig("Approx-EDF-K10", "approx", pull_policy="least-request", approx_sched="edf", lb_subset_size=10),
    #MsExperimentConfig("Approx-EDF-K20", "approx", pull_policy="least-request", approx_sched="edf", lb_subset_size=20),
    #MsExperimentConfig("Approx-EDF-R100-K10", "approx", pull_policy="least-request", approx_sched="edf", scale=90, rps=100_100, lb_subset_size=10),
    #MsExperimentConfig("Approx-EDF", "approx", pull_policy="least-request", approx_sched="edf"),
    #MsExperimentConfig("Approx-EDF-S2", "approx-share", pull_policy="least-request", approx_sched="edf", approx_share=2),
    #MsExperimentConfig("Approx-EDF-S3", "approx-share", pull_policy="least-request", approx_sched="edf", approx_share=3),
    #MsExperimentConfig("ApproxShare-1", "approx-share", pull_policy="least-request", approx_share=1),
    #MsExperimentConfig("ApproxShare-EDF-S1", "approx-share", pull_policy="least-request", approx_sched="edf", approx_share=1),
    #MsExperimentConfig("ApproxShare-EDF-S2", "approx-share", pull_policy="least-request", approx_sched="edf", approx_share=2),
    #MsExperimentConfig("ApproxShare-EDF-S5", "approx-share", pull_policy="least-request", approx_sched="edf", approx_share=5),
]


def resolve_config_rps(config: MsExperimentConfig) -> float:
    return DEFAULT_RPS if config.rps is None else config.rps


def resolve_config_service_dist(
    config: MsExperimentConfig,
    *,
    default: str = "exp",
) -> str:
    return default if config.service_dist is None else config.service_dist


def validate_ms_config(config: MsExperimentConfig) -> None:
    label = config.label
    if uses_approx_protocol(config) and config.pull_policy is None:
        raise SystemExit(
            f"config {label!r}: pull_policy is required when lb_policy is "
            f"{config.lb_policy}"
        )
    if not uses_approx_protocol(config) and config.pull_policy is not None:
        raise SystemExit(
            f"config {label!r}: pull_policy is only valid when lb_policy is "
            "approx or approx-share"
        )
    if config.approx_sched is not None and not uses_approx_protocol(config):
        raise SystemExit(
            f"config {label!r}: approx_sched is only valid when lb_policy is "
            "approx or approx-share"
        )
    if config.centralized_sched != "fcfs" and not uses_central_pull_queue(config):
        raise SystemExit(
            f"config {label!r}: centralized_sched is only valid when lb_policy is "
            "centralized or jbsq"
        )
    if config.lb_policy == "jbsq":
        if config.jbsq_n is None:
            raise SystemExit(f"config {label!r}: jbsq_n is required when lb_policy is jbsq")
        if config.jbsq_n < 1:
            raise SystemExit(
                f"config {label!r}: jbsq_n must be >= 1 (got {config.jbsq_n})"
            )
    elif config.jbsq_n is not None:
        raise SystemExit(
            f"config {label!r}: jbsq_n is only valid when lb_policy is jbsq"
        )
    if config.lb_policy == "approx-share":
        if config.approx_share < 1:
            raise SystemExit(
                f"config {label!r}: approx_share must be >= 1 (got {config.approx_share})"
            )
    elif config.approx_share != 1:
        raise SystemExit(
            f"config {label!r}: approx_share is only valid when lb_policy is approx-share"
        )
    if config.scale is not None and config.scale < 0:
        raise SystemExit(f"config {label!r}: scale must be >= 0 (got {config.scale})")
    if config.rps is not None and config.rps <= 0:
        raise SystemExit(f"config {label!r}: rps must be > 0 (got {config.rps})")
    if config.service_dist is not None and config.service_dist not in MS_SERVICE_DISTS:
        raise SystemExit(
            f"config {label!r}: service_dist must be one of "
            f"{', '.join(MS_SERVICE_DISTS)} (got {config.service_dist!r})"
        )
    if config.slo_ms is not None and config.slo_ms <= 0:
        raise SystemExit(f"config {label!r}: slo_ms must be > 0 (got {config.slo_ms})")
    validate_prequal_subset(config.lb_policy, config.lb_subset_size)


def select_configs(
    configs: list[MsExperimentConfig],
    config_index: list[int] | None,
    *,
    lb_subset_size: int | None = None,
    scale: int | None = None,
    rps: float | None = None,
    service_dist: str | None = None,
    slo_ms: float | None = None,
) -> list[MsExperimentConfig]:
    if config_index is None:
        selected = list(configs)
    else:
        selected = []
        for idx in config_index:
            if idx < 0 or idx >= len(configs):
                raise SystemExit(
                    f"--config-index {idx} out of range (0 .. {len(configs) - 1})"
                )
            selected.append(configs[idx])
    if lb_subset_size is not None:
        if lb_subset_size < 0:
            raise SystemExit(f"--lb-subset-size must be >= 0 (got {lb_subset_size})")
        selected = [
            replace(config, lb_subset_size=lb_subset_size) for config in selected
        ]
    if scale is not None:
        if scale < 0:
            raise SystemExit(f"--scale must be >= 0 (got {scale})")
        selected = [replace(config, scale=scale) for config in selected]
    if rps is not None:
        if rps <= 0:
            raise SystemExit(f"--rps must be > 0 (got {rps})")
        selected = [replace(config, rps=rps) for config in selected]
    if service_dist is not None:
        if service_dist not in MS_SERVICE_DISTS:
            raise SystemExit(
                f"--service-dist must be one of {', '.join(MS_SERVICE_DISTS)} "
                f"(got {service_dist!r})"
            )
        selected = [replace(config, service_dist=service_dist) for config in selected]
    if slo_ms is not None:
        if slo_ms <= 0:
            raise SystemExit(f"--slo-ms must be > 0 (got {slo_ms})")
        selected = [replace(config, slo_ms=slo_ms) for config in selected]
    for config in selected:
        validate_ms_config(config)
    return selected


def api_stats(data: dict, api: str) -> dict:
    by_api = data["by_api"]
    if api not in by_api:
        valid = ", ".join(sorted(by_api.keys())) or "(none)"
        raise SystemExit(f"API {api!r} not in simulation output; valid APIs: {valid}")
    stats = by_api[api]
    if not stats["e2e_ms"]:
        raise SystemExit(f"no completed requests for API {api!r}")
    return stats


def slo_from_unloaded_latency_ms(stats: dict) -> float:
    return SLO_UNLOADED_LATENCY_MULTIPLIER * stats["unloaded_latency_p99_ms"]


def load_values(load_min: float, load_max: float, load_step: float) -> list[float]:
    values = np.arange(load_min, load_max + load_step / 2, load_step, dtype=float)
    return [round(float(v), 10) for v in values]


def _log(message: str) -> None:
    write = getattr(tqdm, "write", None)
    if write is None:
        print(message, file=sys.stderr)
    else:
        write(message)


def calibrate_topology_slo(
    binary: Path,
    *,
    callgraph: Path,
    load_file: Path,
    api: str,
    config: MsExperimentConfig,
    seed: int | None,
    default_service_dist: str = "exp",
) -> float:
    service_dist = resolve_config_service_dist(config, default=default_service_dist)
    data = run_ms_simulation(
        binary,
        callgraph=callgraph,
        load_file=load_file,
        n=CALIBRATION_N,
        lb_policy=config.lb_policy,
        pull_policy=config.pull_policy,
        lb_subset_size=config.lb_subset_size,
        scheduling=config.scheduling,
        centralized_sched=config.centralized_sched,
        seed=seed,
        service_dist=service_dist,
        approx_sched=config.approx_sched,
        approx_share=(
            config.approx_share if config.lb_policy == "approx-share" else None
        ),
        jbsq_n=config.jbsq_n,
        scale=config.scale,
    )
    return slo_from_unloaded_latency_ms(api_stats(data, api))


def average_utilization_pct(data: dict) -> float:
    utils = data.get("microservice_utilization_pct") or {}
    if not utils:
        raise SystemExit("ms JSON missing microservice_utilization_pct")
    return sum(float(v) for v in utils.values()) / len(utils)


def seeds_for_loads(loads: list[float], base_seed: int | None) -> dict[float, int]:
    """One seed per load level, shared across all policies at that load.

    Derived from ``base_seed`` when set so sweeps are reproducible; otherwise
    drawn once from the OS RNG for this process.
    """
    rng = random.Random(base_seed)
    return {load: rng.randrange(2**63) for load in loads}


def format_run_summary(
    *,
    config: MsExperimentConfig,
    load: float,
    rps: float,
    slo_ms: float,
    violation_pct: float,
    utilization_pct: float,
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
    if config.approx_sched is not None:
        parts.append(f"approx_sched={config.approx_sched}")
    if uses_central_pull_queue(config) and config.centralized_sched != "fcfs":
        parts.append(f"centralized_sched={config.centralized_sched}")
    if config.jbsq_n is not None:
        parts.append(f"jbsq_n={config.jbsq_n}")
    if config.lb_policy == "approx-share":
        parts.append(f"approx_share={config.approx_share}")
    if config.scale is not None:
        parts.append(f"scale={config.scale}")
    if config.service_dist is not None:
        parts.append(f"service_dist={config.service_dist}")
    if seed is not None:
        parts.append(f"seed={seed}")
    parts.append(f"rps={rps:g}")
    parts.append(f"SLO={slo_ms:.4f}ms")
    parts.append(f"utilization={utilization_pct:.1f}%")
    parts.append(f"violations={violation_pct:.2f}%")
    return "  ".join(parts)


def run_load_compare_sweep(
    binary: Path,
    configs: list[MsExperimentConfig],
    loads: list[float],
    *,
    callgraph: Path,
    load_file: Path,
    api: str,
    slo_by_label: dict[str, float],
    n: int,
    seed: int | None,
    default_service_dist: str = "exp",
) -> list[tuple[str, list[float]]]:
    """Return (label, SLO violation %) per config; x is shared loads.

    Each load level gets one seed shared by every policy at that load.
    """
    series: list[tuple[str, list[float]]] = [
        (config.label, []) for config in configs
    ]
    seed_by_load = seeds_for_loads(loads, seed)
    _log(
        "per-load seeds: "
        + ", ".join(f"{load:g}→{seed_by_load[load]}" for load in loads)
    )
    pairs = list(product(configs, loads))

    for config, load in tqdm(pairs, desc="config × load", unit="run"):
        rps = load * resolve_config_rps(config)
        slo_ms = slo_by_label[config.label]
        service_dist = resolve_config_service_dist(
            config, default=default_service_dist
        )
        run_seed = seed_by_load[load]
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
            seed=run_seed,
            rps=rps,
            slo_ms=slo_ms,
            service_dist=service_dist,
            approx_sched=config.approx_sched,
            approx_share=(
                config.approx_share if config.lb_policy == "approx-share" else None
            ),
            jbsq_n=config.jbsq_n,
            scale=config.scale,
        )
        violation_pct = api_stats(data, api)["prob_latency_gt_slo"] * 100.0
        utilization_pct = average_utilization_pct(data)
        idx = configs.index(config)
        series[idx][1].append(violation_pct)
        _log(
            format_run_summary(
                config=config,
                load=load,
                rps=rps,
                slo_ms=slo_ms,
                violation_pct=violation_pct,
                utilization_pct=utilization_pct,
                seed=run_seed,
            )
        )
    return series


Y_AXIS_MAX = 10.0
Y_AXIS_MIN = 0.0


def plot_load_compare(
    loads: list[float],
    series: list[tuple[str, list[float]]],
    *,
    output_path: Path,
) -> None:
    style = ACM_COMPACT_HALF
    style = replace(style, aspect_ratio=0.4)
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    series_styles = distinct_series_styles(len(series), style)
    for i, (label, y_values) in enumerate(series):
        line_style = series_styles[i]
        plot_line(
            ax,
            loads,
            y_values,
            label=label,
            style=style,
            show_markers=True,
            color=line_style["color"],
            marker=line_style["marker"],
            linestyle=line_style["linestyle"],
        )

    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        xlabel="Load",
        ylabel="% of SLO violations",
        title="",
        ylim=(Y_AXIS_MIN, Y_AXIS_MAX),
        auto_ticks=False,
        grid=False,
    )
    load_min, load_max = min(loads), max(loads)
    xtick_start = np.ceil(load_min * 10 - 1e-9) / 10
    xtick_stop = np.floor(load_max * 10 + 1e-9) / 10
    xticks = [
        round(float(v), 10)
        for v in np.arange(xtick_start, xtick_stop + 0.05, 0.1, dtype=float)
    ]
    yticks = np.arange(Y_AXIS_MIN, Y_AXIS_MAX + 0.1, 2)
    y_grid = np.arange(Y_AXIS_MIN, Y_AXIS_MAX + 0.1, 1)
    # Labels on major ticks; grid drawn explicitly so it can differ from labels.
    ax.set_xticks(xticks)
    ax.set_xticklabels([f"{tick:g}" for tick in xticks])
    ax.set_yticks(yticks)
    ax.set_xlim(load_min, load_max)
    ax.set_ylim(Y_AXIS_MIN, Y_AXIS_MAX)
    ax.grid(False)
    ax.set_axisbelow(True)
    grid_kw = dict(color="0.5", alpha=0.3, linewidth=0.5, zorder=0)
    for x in loads:
        ax.axvline(x, **grid_kw)
    for y in y_grid:
        ax.axhline(y, **grid_kw)

    grid.add_shared_legend(position="top")
    grid.save(output_path)


def default_output_path(
    chain: int,
    *,
    scale: int | None = None,
    lb_subset_size: int | None = None,
) -> Path:
    name = f"ms_chain{chain}_load_compare_slo"
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
            "Compare MS experiment configs on one chain topology while sweeping "
            "load (SLO violation %)."
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
    parser.add_argument("--api", type=str, default="handle")
    parser.add_argument("--load-min", type=float, default=0.1)
    parser.add_argument("--load-max", type=float, default=0.9)
    parser.add_argument("--load-step", type=float, default=0.1)
    parser.add_argument(
        "--rps",
        type=float,
        default=None,
        help=(
            f"Override base rps for all configs "
            f"(simulator rps = load × rps; default per config or {DEFAULT_RPS:g})"
        ),
    )
    parser.add_argument("--n", type=int, default=100_000)
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
        "--slo-ms",
        type=float,
        default=None,
        help=(
            "Override SLO latency (ms) for all configs "
            "(default: per-config or calibrate from unloaded p99 × "
            f"{SLO_UNLOADED_LATENCY_MULTIPLIER:g})"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "Base seed for the sweep: derives one seed per load level, shared "
            "across all policies at that load (default: non-deterministic base)"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configs = select_configs(
        DEFAULT_CONFIGS,
        args.config_index,
        lb_subset_size=args.lb_subset_size,
        scale=args.scale,
        rps=args.rps,
        service_dist=args.service_dist,
        slo_ms=args.slo_ms,
    )
    loads = load_values(args.load_min, args.load_max, args.load_step)
    if not loads:
        raise SystemExit("no load values in sweep range")

    callgraph, load_file = resolve_fixtures(args)
    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")

    slo_by_label: dict[str, float] = {}
    for config in configs:
        service_dist = resolve_config_service_dist(config)
        if config.slo_ms is not None:
            slo_ms = config.slo_ms
            _log(
                f"chain{args.chain} {config.label} service_dist={service_dist} "
                f"SLO={slo_ms:.4f}ms (config)"
            )
        else:
            slo_ms = calibrate_topology_slo(
                binary,
                callgraph=callgraph,
                load_file=load_file,
                api=args.api,
                config=config,
                seed=args.seed,
            )
            _log(
                f"chain{args.chain} {config.label} service_dist={service_dist} "
                f"SLO={slo_ms:.4f}ms "
                f"(n={CALIBRATION_N} processing p99 × "
                f"{SLO_UNLOADED_LATENCY_MULTIPLIER:g})"
            )
        slo_by_label[config.label] = slo_ms

    series = run_load_compare_sweep(
        binary,
        configs,
        loads,
        callgraph=callgraph,
        load_file=load_file,
        api=args.api,
        slo_by_label=slo_by_label,
        n=args.n,
        seed=args.seed,
    )

    output_path = args.output or default_output_path(
        args.chain,
        scale=args.scale,
        lb_subset_size=args.lb_subset_size,
    )
    output_path = output_path_with_comment(output_path, args.comment)
    plot_load_compare(loads, series, output_path=output_path)
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
