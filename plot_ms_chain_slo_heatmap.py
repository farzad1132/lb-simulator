#!/usr/bin/env python3
"""Plot chain3/chain6/chain10 SLO violation probability across microservice load."""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-ms-chain-plot-cache"
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
    MS_APPROX_SCHED_POLICIES,
    MS_LB_POLICIES,
    MS_SCHEDULING_POLICIES,
    MS_SERVICE_DISTS,
    PULL_POLICIES,
    REPO_ROOT,
    ensure_release_binary,
    output_path_with_comment,
    run_ms_simulation,
    validate_prequal_subset,
)
from plotting_primitive import ACM_COMPACT_HALF, SubplotGrid, plot_heatmap

DEFAULT_CHAIN3_CALLGRAPH = REPO_ROOT / "tests" / "chain" / "3" / "callgraph.json"
DEFAULT_CHAIN3_LOAD = REPO_ROOT / "tests" / "chain" / "3" / "load.json"
DEFAULT_CHAIN6_CALLGRAPH = REPO_ROOT / "tests" / "chain" / "6" / "callgraph.json"
DEFAULT_CHAIN6_LOAD = REPO_ROOT / "tests" / "chain" / "6" / "load.json"
DEFAULT_CHAIN10_CALLGRAPH = REPO_ROOT / "tests" / "chain" / "10" / "callgraph.json"
DEFAULT_CHAIN10_LOAD = REPO_ROOT / "tests" / "chain" / "10" / "load.json"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "ms_chain_slo_heatmap.pdf"
DEFAULT_RPS_PER_LOAD_LEVEL = 10_000.0
CALIBRATION_N = 300_000
SLO_UNLOADED_LATENCY_MULTIPLIER = 2.0


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
    lb_policy: str,
    pull_policy: str | None,
    lb_subset_size: int,
    scheduling: str,
    seed: int | None,
    service_dist: str,
    approx_sched: str | None,
) -> float:
    data = run_ms_simulation(
        binary,
        callgraph=callgraph,
        load_file=load_file,
        n=CALIBRATION_N,
        lb_policy=lb_policy,
        pull_policy=pull_policy,
        lb_subset_size=lb_subset_size,
        scheduling=scheduling,
        seed=seed,
        service_dist=service_dist,
        approx_sched=approx_sched,
    )
    return slo_from_unloaded_latency_ms(api_stats(data, api))


def run_chain_sweep(
    binary: Path,
    *,
    chain3_callgraph: Path,
    chain3_load: Path,
    chain6_callgraph: Path,
    chain6_load: Path,
    chain10_callgraph: Path,
    chain10_load: Path,
    api: str,
    loads: list[float],
    rps_per_load_level: float,
    n: int,
    lb_policy: str,
    pull_policy: str | None,
    lb_subset_size: int,
    scheduling: str,
    seed: int | None,
    service_dist: str = "exp",
    approx_sched: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    topologies = [
        (chain3_callgraph, chain3_load, "chain3"),
        (chain6_callgraph, chain6_load, "chain6"),
        (chain10_callgraph, chain10_load, "chain10"),
    ]
    topology_slos: list[float] = []
    for callgraph, load_file, label in topologies:
        slo_ms = calibrate_topology_slo(
            binary,
            callgraph=callgraph,
            load_file=load_file,
            api=api,
            lb_policy=lb_policy,
            pull_policy=pull_policy,
            lb_subset_size=lb_subset_size,
            scheduling=scheduling,
            seed=seed,
            service_dist=service_dist,
            approx_sched=approx_sched,
        )
        topology_slos.append(slo_ms)
        _log(
            f"{label} SLO={slo_ms:.4f}ms "
            f"(from n={CALIBRATION_N} processing p99 × {SLO_UNLOADED_LATENCY_MULTIPLIER:g})"
        )

    probs = np.zeros((3, len(loads)), dtype=float)
    slos = np.zeros((3, len(loads)), dtype=float)

    for col, load in enumerate(tqdm(loads, desc="chain SLO sweep", unit="load")):
        rps = load * rps_per_load_level
        print(f"load={load:g} rps={rps:g}", file=sys.stderr)
        for row, ((callgraph, load_file, label), slo_ms) in enumerate(
            zip(topologies, topology_slos)
        ):
            data = run_ms_simulation(
                binary,
                callgraph=callgraph,
                load_file=load_file,
                n=n,
                lb_policy=lb_policy,
                pull_policy=pull_policy,
                lb_subset_size=lb_subset_size,
                scheduling=scheduling,
                seed=seed,
                rps=rps,
                slo_ms=slo_ms,
                service_dist=service_dist,
                approx_sched=approx_sched,
            )
            stats = api_stats(data, api)
            slos[row, col] = slo_ms
            probs[row, col] = stats["prob_latency_gt_slo"] * 100.0
            _log(
                f"{label} load={load:g} rps={rps:g} SLO={slo_ms:.4f}ms "
                f"violations={probs[row, col]:.2f}%"
            )

    return probs, slos


def plot_chain_heatmap(loads: list[float], probs: np.ndarray, output_path: Path) -> None:
    style = replace(ACM_COMPACT_HALF, aspect_ratio=0.75)
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)
    vmax = max(float(np.nanmax(probs)), 1.0)
    plot_heatmap(
        ax,
        probs,
        x_labels=[f"{load:g}" for load in loads],
        y_labels=["chain3", "chain6", "chain10"],
        style=style,
        vmin=0.0,
        vmax=vmax,
        colorbar_label="% of SLO violations",
        annotation_fmt="{:.1f}",
    )
    grid.configure_ax(ax, xlabel="Load level", grid=False, auto_ticks=False)
    grid.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot chain3/chain6/chain10 microservice SLO violation heatmap.",
    )
    parser.add_argument("--binary", type=Path, default=None,
                        help="Prebuilt ms release binary (skips cargo build --release)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Output PDF path")
    parser.add_argument("--comment", type=str, default=None,
                        help="Suffix appended to output filename before .pdf")
    parser.add_argument("--chain3-callgraph", type=Path, default=DEFAULT_CHAIN3_CALLGRAPH)
    parser.add_argument("--chain3-load", type=Path, default=DEFAULT_CHAIN3_LOAD)
    parser.add_argument("--chain6-callgraph", type=Path, default=DEFAULT_CHAIN6_CALLGRAPH)
    parser.add_argument("--chain6-load", type=Path, default=DEFAULT_CHAIN6_LOAD)
    parser.add_argument("--chain10-callgraph", type=Path, default=DEFAULT_CHAIN10_CALLGRAPH)
    parser.add_argument("--chain10-load", type=Path, default=DEFAULT_CHAIN10_LOAD)
    parser.add_argument("--api", type=str, default="handle")
    parser.add_argument("--load-min", type=float, default=0.1)
    parser.add_argument("--load-max", type=float, default=0.9)
    parser.add_argument("--load-step", type=float, default=0.1)
    parser.add_argument("--rps-per-load-level", type=float, default=DEFAULT_RPS_PER_LOAD_LEVEL)
    parser.add_argument("--n", type=int, default=100000)
    parser.add_argument("--lb-policy", choices=MS_LB_POLICIES, default="power-of-two")
    parser.add_argument(
        "--pull-policy",
        choices=PULL_POLICIES,
        default=None,
        help="Pull-intent server selection for approx (required when --lb-policy approx)",
    )
    parser.add_argument("--lb-subset-size", type=int, default=0)
    parser.add_argument("--scheduling", choices=MS_SCHEDULING_POLICIES, default="fifo")
    parser.add_argument(
        "--service-dist",
        choices=MS_SERVICE_DISTS,
        default="exp",
        help="Service-time distribution (default: exp)",
    )
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--approx-sched",
        choices=MS_APPROX_SCHED_POLICIES,
        default=None,
        help="Approx outbound pull scheduling: fcfs or edf (only valid with --lb-policy approx)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.lb_policy == "approx" and args.pull_policy is None:
        raise SystemExit("--pull-policy is required when --lb-policy approx")
    if args.lb_policy != "approx" and args.pull_policy is not None:
        raise SystemExit("--pull-policy is only valid with --lb-policy approx")
    if args.approx_sched is not None and args.lb_policy != "approx":
        raise SystemExit("--approx-sched is only valid with --lb-policy approx")
    validate_prequal_subset(args.lb_policy, args.lb_subset_size)

    loads = load_values(args.load_min, args.load_max, args.load_step)
    if not loads:
        print("no load values in sweep range", file=sys.stderr)
        sys.exit(1)

    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")
    probs, _slos = run_chain_sweep(
        binary,
        chain3_callgraph=args.chain3_callgraph,
        chain3_load=args.chain3_load,
        chain6_callgraph=args.chain6_callgraph,
        chain6_load=args.chain6_load,
        chain10_callgraph=args.chain10_callgraph,
        chain10_load=args.chain10_load,
        api=args.api,
        loads=loads,
        rps_per_load_level=args.rps_per_load_level,
        n=args.n,
        lb_policy=args.lb_policy,
        pull_policy=args.pull_policy,
        lb_subset_size=args.lb_subset_size,
        scheduling=args.scheduling,
        seed=args.seed,
        service_dist=args.service_dist,
        approx_sched=args.approx_sched,
    )
    output_path = output_path_with_comment(args.output, args.comment)
    plot_chain_heatmap(loads, probs, output_path)
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
