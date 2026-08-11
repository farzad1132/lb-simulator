#!/usr/bin/env python3
"""Standalone cumulative queueing variance line plot for MS configs at a single load.

Requires --chain {3,6,10}. Configs are selected like plot_ms_occupancy_compare.py.
Color encodes config; shared marker/linestyle encodes Theory: Independent
(sum of per-hop queueing variances) vs Simulation
(var of cumulative_queueing_delay_ms) by microservice tier.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
import os
import sys
import tempfile
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-ms-cum-queueing-var-plot-cache"
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
    resolve_config_service_dist,
    select_configs,
)
from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    plot_line,
)

# Shared across configs: Theory vs Simulation distinguished only by style.
THEORY_MARKER = "o"
THEORY_LINESTYLE = "--"
SIMULATION_MARKER = "s"
SIMULATION_LINESTYLE = "-"

sys.path.insert(0, str(REPO_ROOT / "analyze"))
from ms_service_distributions import finalize_violin_y_axis  # noqa: E402

DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"

# Placeholder configs — edit to compare the policies you care about.
DEFAULT_CONFIGS: list[MsExperimentConfig] = [
    #MsExperimentConfig("CPull", "centralized"),
    #MsExperimentConfig("CPush", "cl"),
    MsExperimentConfig("P2C", "power-of-two"),
    #MsExperimentConfig("P2C+EDF", "power-of-two", scheduling="edf"),
    #MsExperimentConfig("LR", "least-request"),
    #MsExperimentConfig("WRR", "round-robin"),
    #MsExperimentConfig("R", "random"),
    #MsExperimentConfig("Approx", "approx", pull_policy="least-request"),
    #MsExperimentConfig("Approx-FCFS", "approx", pull_policy="least-request", approx_sched="fcfs"),
    #MsExperimentConfig("Approx-EDF", "approx", pull_policy="least-request", approx_sched="edf"),
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


def validate_queueing_fields(data: dict, microservices: list[str]) -> None:
    by_ms = data.get("by_microservice")
    if not by_ms:
        raise SystemExit("ms JSON missing by_microservice; rebuild the ms binary")
    for ms in microservices:
        if ms not in by_ms:
            raise SystemExit(f"ms JSON missing by_microservice entry for {ms}")
        ms_stats = by_ms[ms]
        if "queueing_delay_ms" not in ms_stats:
            raise SystemExit(
                "ms JSON missing by_microservice queueing_delay_ms; rebuild the ms binary"
            )
        if "cumulative_queueing_delay_ms" not in ms_stats:
            raise SystemExit(
                "ms JSON missing by_microservice cumulative_queueing_delay_ms; "
                "rebuild the ms binary"
            )


def cumulative_queueing_var_series(
    data: dict,
    microservices: list[str],
) -> tuple[list[float], list[float]]:
    by_ms = data["by_microservice"]
    per_hop_var = [
        float(np.var(by_ms[ms]["queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    theoretical_var = [
        float(sum(per_hop_var[: idx + 1]))
        for idx in range(len(microservices))
    ]
    simulation_var = [
        float(np.var(by_ms[ms]["cumulative_queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    return theoretical_var, simulation_var


def format_run_summary(
    *,
    config: MsExperimentConfig,
    load: float,
    rps: float,
    theoretical_var: list[float],
    simulation_var: list[float],
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
    if config.lb_policy == "approx-share":
        parts.append(f"approx_share={config.approx_share}")
    if config.lb_policy == "centralized" and config.centralized_sched != "fcfs":
        parts.append(f"centralized_sched={config.centralized_sched}")
    if config.scale is not None:
        parts.append(f"scale={config.scale}")
    if config.service_dist is not None:
        parts.append(f"service_dist={config.service_dist}")
    parts.append(f"rps={rps:g}")
    theory_str = ",".join(f"{v:.3f}" for v in theoretical_var)
    sim_str = ",".join(f"{v:.3f}" for v in simulation_var)
    parts.append(f"theory_var=[{theory_str}]")
    parts.append(f"simulation_var=[{sim_str}]")
    return "  ".join(parts)


def run_cum_queueing_var_compare(
    binary: Path,
    configs: list[MsExperimentConfig],
    *,
    load: float,
    callgraph: Path,
    load_file: Path,
    n: int,
    seed: int | None,
    default_service_dist: str = "exp",
) -> tuple[list[str], list[tuple[str, list[float], list[float]]]]:
    """Return (microservices, [(label, theory_var, simulation_var)])."""
    microservices: list[str] | None = None
    series: list[tuple[str, list[float], list[float]]] = []

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
            approx_sched=config.approx_sched,
            approx_share=(
                config.approx_share if config.lb_policy == "approx-share" else None
            ),
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
        validate_queueing_fields(data, microservices)
        theoretical_var, simulation_var = cumulative_queueing_var_series(
            data, microservices
        )
        series.append((config.label, theoretical_var, simulation_var))
        _log(
            format_run_summary(
                config=config,
                load=load,
                rps=rps,
                theoretical_var=theoretical_var,
                simulation_var=simulation_var,
            )
        )

    if microservices is None:
        raise SystemExit("no simulations ran")
    return microservices, series


def plot_cum_queueing_var_lines(
    microservices: list[str],
    series: list[tuple[str, list[float], list[float]]],
    *,
    output_path: Path,
) -> None:
    from matplotlib.lines import Line2D

    if not series:
        raise SystemExit("expected at least 1 config, got 0")
    style = replace(ACM_COMPACT_HALF, aspect_ratio=0.4)
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)

    positions = list(range(len(microservices)))
    all_values: list[float] = []
    config_handles: list[Line2D] = []
    config_labels: list[str] = []
    for cfg_idx, (label, theoretical_var, simulation_var) in enumerate(series):
        color = style.colors[cfg_idx % len(style.colors)]
        all_values.extend(theoretical_var)
        all_values.extend(simulation_var)
        plot_line(
            ax,
            positions,
            theoretical_var,
            style=style,
            show_markers=True,
            color=color,
            marker=THEORY_MARKER,
            linestyle=THEORY_LINESTYLE,
        )
        plot_line(
            ax,
            positions,
            simulation_var,
            style=style,
            show_markers=True,
            color=color,
            marker=SIMULATION_MARKER,
            linestyle=SIMULATION_LINESTYLE,
        )
        config_handles.append(
            Line2D(
                [0],
                [0],
                color=color,
                linestyle="-",
                marker="None",
                linewidth=style.line_width,
            )
        )
        config_labels.append(label)

    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    finalize_violin_y_axis(ax, np.asarray(all_values, dtype=float), style=style)
    if positions:
        ax.set_xlim(positions[0] - 0.5, positions[-1] + 0.5)
    grid.configure_ax(
        ax,
        xlabel="Microservice index",
        ylabel="Cum. Queue. Var.",
        title="",
        show_xlabel=True,
        show_ylabel=True,
        show_title=False,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    style_handles = [
        Line2D(
            [0],
            [0],
            color="black",
            linestyle=THEORY_LINESTYLE,
            marker=THEORY_MARKER,
            markersize=style.marker_size,
            linewidth=style.line_width,
            label="Theory: Independent",
        ),
        Line2D(
            [0],
            [0],
            color="black",
            linestyle=SIMULATION_LINESTYLE,
            marker=SIMULATION_MARKER,
            markersize=style.marker_size,
            linewidth=style.line_width,
            label="Simulation",
        ),
    ]
    ax.legend(
        handles=style_handles,
        fontsize=max(style.legend_size - 1, 5),
        loc="upper left",
        frameon=False,
    )
    if len(series) > 1:
        grid.add_shared_legend(
            position="top",
            handles=config_handles,
            labels=config_labels,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    grid.save(output_path)


def default_output_path(
    chain: int,
    *,
    scale: int | None = None,
    lb_subset_size: int | None = None,
) -> Path:
    name = f"ms_chain{chain}_cumulative_queueing_var"
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
            "Line plot of cumulative queueing variance (Theory: Independent vs "
            "Simulation) for MS experiment configs at a single load."
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
        default=None,
        help=(
            "Override service-time distribution for all configs "
            f"(choices: {', '.join(MS_SERVICE_DISTS)}; "
            "default: per-config or exp)"
        ),
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
        service_dist=args.service_dist,
    )
    if not configs:
        raise SystemExit("no configs selected")

    callgraph, load_file = resolve_fixtures(args)
    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")

    microservices, series = run_cum_queueing_var_compare(
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
        lb_subset_size=args.lb_subset_size,
    )
    output_path = output_path_with_comment(output_path, args.comment)
    plot_cum_queueing_var_lines(
        microservices,
        series,
        output_path=output_path,
    )
    print(f"wrote {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
