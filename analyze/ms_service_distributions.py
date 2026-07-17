#!/usr/bin/env python3
"""Plot per-microservice visit distributions for an ms chain topology."""

from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-ms-analyze-cache"
_MPL_CACHE = _CACHE_ROOT / "matplotlib"
_XDG_CACHE = _CACHE_ROOT / "xdg"
_MPL_CACHE.mkdir(parents=True, exist_ok=True)
_XDG_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))
os.environ.setdefault("XDG_CACHE_HOME", str(_XDG_CACHE))
os.environ.setdefault("MPLBACKEND", "Agg")

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from plot_cdfs import (  # noqa: E402
    MS_APPROX_SCHED_POLICIES,
    MS_LB_POLICIES,
    MS_SCHEDULING_POLICIES,
    PULL_POLICIES,
    ensure_release_binary,
    output_path_with_comment,
    run_ms_simulation,
    validate_prequal_subset,
)
from plotting_primitive import (  # noqa: E402
    ACM_COMPACT_HALF,
    SubplotGrid,
    configure_x_axis_ticks,
    percentile,
    plot_cdf,
    plot_grouped_bars,
)

DEFAULT_CHAIN = 6
DEFAULT_CALLGRAPH = REPO_ROOT / "tests" / "chain" / str(DEFAULT_CHAIN) / "callgraph.json"
OUTPUT_DIR = REPO_ROOT / "output"
OUTPUT_BASENAME = "ms_service_distributions"

def microservice_slo_violation_pct(ms_stats: dict) -> float:
    if "prob_latency_gt_slo" in ms_stats:
        return 100.0 * float(ms_stats["prob_latency_gt_slo"])
    rt = ms_stats.get("response_time_ms") or []
    sd = ms_stats.get("slack_d_ms") or []
    if not rt:
        return 0.0
    if len(rt) != len(sd):
        raise SystemExit("response_time_ms and slack_d_ms length mismatch")
    violations = sum(1 for r, s in zip(rt, sd) if r > s)
    return 100.0 * violations / len(rt)

def resolve_callgraph_path(*, chain: int | None, callgraph: Path | None) -> Path:
    if callgraph is not None:
        path = callgraph if callgraph.is_absolute() else REPO_ROOT / callgraph
        return path.resolve()
    chain_id = DEFAULT_CHAIN if chain is None else chain
    return (REPO_ROOT / "tests" / "chain" / str(chain_id) / "callgraph.json").resolve()


def resolve_load_file_path(callgraph: Path, load_file: Path | None) -> Path:
    if load_file is not None:
        path = load_file if load_file.is_absolute() else REPO_ROOT / load_file
        return path.resolve()
    return (callgraph.parent / "load.json").resolve()


def callgraph_output_slug(callgraph: Path) -> str:
    try:
        rel = callgraph.resolve().relative_to(REPO_ROOT.resolve())
    except ValueError:
        rel = callgraph.resolve()
    parts = list(rel.parts)
    if parts and parts[-1] == "callgraph.json":
        parts = parts[:-1]
    return "_".join(parts)


def default_output_path(callgraph: Path) -> Path:
    slug = callgraph_output_slug(callgraph)
    return OUTPUT_DIR / f"{OUTPUT_BASENAME}_{slug}.pdf"


def microservice_order(data: dict) -> list[str]:
    order = data.get("microservice_order")
    if order is not None:
        return list(order)
    by_ms = data.get("by_microservice", {})
    if not by_ms:
        raise SystemExit("simulation output missing by_microservice")
    raise SystemExit("ms JSON missing microservice_order; rebuild the ms binary")


def _fallback_y_step(span: float, *, min_ticks: int = 5) -> float:
    if span <= 0:
        return 1.0
    target = span / max(min_ticks - 1, 1)
    magnitude = 10 ** math.floor(math.log10(max(target, 1e-12)))
    for mult in (10, 5, 2, 1):
        step = mult * magnitude
        hi = math.ceil(span / step) * step
        tick_count = int(round(hi / step)) + 1 if step > 0 else min_ticks
        if tick_count >= min_ticks:
            return step
    return target


def finalize_violin_y_axis(
    ax,
    combined: np.ndarray,
    *,
    style=ACM_COMPACT_HALF,
    min_ticks: int = 5,
) -> None:
    lo = 0.0
    if len(combined):
        data_lo = float(np.min(combined))
        if data_lo < lo:
            lo = data_lo
    data_hi = float(np.max(combined)) if len(combined) else 1.0
    if data_hi <= lo:
        data_hi = 1.0
    span = data_hi - lo
    step = _fallback_y_step(span, min_ticks=min_ticks)
    hi = math.ceil(data_hi / step) * step
    ticks = list(np.arange(lo, hi + step / 2, step))
    ticks = [float(t) for t in ticks if lo <= t <= hi]
    while len(ticks) < min_ticks:
        step /= 2.0
        hi = math.ceil(data_hi / step) * step
        ticks = list(np.arange(lo, hi + step / 2, step))
        ticks = [float(t) for t in ticks if lo <= t <= hi]
    ax.set_yticks(ticks)
    ax.set_yticklabels(
        [
            str(int(t)) if abs(t - round(t)) < 1e-9 else f"{t:g}"
            for t in ticks
        ],
        fontsize=style.font_size - 1,
    )
    y_pad = style.axis_guard_fraction * (hi - lo) if hi > lo else style.axis_guard_fraction
    ax.set_ylim(lo, hi + y_pad)


VIOLIN_WIDTH = 0.7
VIOLIN_PERCENTILE_MARKERS = (
    (50, "#000000", "-"),
    (90, "#0072B2", "--"),
    (99, "#D55E00", ":"),
)


def plot_metric_violinplot(
    ax,
    data: dict,
    microservices: list[str],
    field: str,
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    violin_data = [
        np.asarray(by_ms[ms][field], dtype=float)
        for ms in microservices
    ]
    positions = list(range(len(microservices)))
    parts = ax.violinplot(
        violin_data,
        positions=positions,
        widths=VIOLIN_WIDTH,
        showmeans=False,
        showmedians=False,
        showextrema=False,
    )
    for idx, body in enumerate(parts["bodies"]):
        color = style.colors[idx % len(style.colors)]
        body.set_facecolor(color)
        body.set_alpha(0.6)
        body.set_edgecolor(color)
        body.set_linewidth(style.line_width * 0.5)

    half = VIOLIN_WIDTH / 2 * 0.6
    line_width = style.line_width * 0.5
    for pct, color, linestyle in VIOLIN_PERCENTILE_MARKERS:
        for idx, samples in enumerate(violin_data):
            if len(samples) == 0:
                continue
            y = percentile(samples, pct)
            ax.hlines(
                y,
                idx - half,
                idx + half,
                colors=color,
                linestyles=linestyle,
                linewidth=line_width,
            )

    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.concatenate([samples for samples in violin_data if len(samples) > 0])
    finalize_violin_y_axis(ax, combined, style=style)

    from matplotlib.lines import Line2D

    handles = [
        Line2D(
            [0],
            [0],
            color=color,
            linestyle=linestyle,
            linewidth=line_width,
            label=f"p{pct}",
        )
        for pct, color, linestyle in VIOLIN_PERCENTILE_MARKERS
    ]
    ax.legend(
        handles=handles,
        fontsize=max(style.font_size - 1, 5),
        loc="best",
        frameon=False,
    )


def plot_cumulative_queueing_violinplot(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    plot_metric_violinplot(
        ax,
        data,
        microservices,
        "cumulative_queueing_delay_ms",
        style=style,
    )


def plot_cumulative_queueing_stddev_bars(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    per_hop_var = [
        float(np.var(by_ms[ms]["queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    theoretical_std = [
        math.sqrt(sum(per_hop_var[: idx + 1]))
        for idx in range(len(microservices))
    ]
    actual_std = [
        float(np.std(by_ms[ms]["cumulative_queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    positions = list(range(len(microservices)))
    plot_grouped_bars(
        ax,
        positions,
        [
            ("Independent", theoretical_std, None),
            ("Actual", actual_std, None),
        ],
        style=style,
    )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.asarray(theoretical_std + actual_std, dtype=float)
    finalize_violin_y_axis(ax, combined, style=style)
    ax.legend(fontsize=style.legend_size, loc="upper left", frameon=False)


def plot_replica_avg_queue_inflight_dots(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    avg_queue_inflight = data["server_avg_queue_inflight"]
    positions = list(range(len(microservices)))
    all_avg: list[float] = []
    for idx, ms in enumerate(microservices):
        by_replica = avg_queue_inflight[ms]
        replicas = sorted(by_replica, key=lambda k: int(k))
        n = len(replicas)
        color = style.colors[idx % len(style.colors)]
        for r in replicas:
            avg = float(by_replica[r])
            all_avg.append(avg)
            jitter = 0.0 if n <= 1 else (int(r) - (n - 1) / 2) * 0.04
            ax.scatter(
                idx + jitter,
                avg,
                color=color,
                s=style.marker_size**2,
                edgecolors="black",
                linewidths=0.4,
                zorder=3,
            )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.asarray(all_avg, dtype=float) if all_avg else np.asarray([0.0, 1.0])
    finalize_violin_y_axis(ax, combined, style=style)


def plot_per_hop_queueing_stddev_bars(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    per_hop_mean = [
        float(np.mean(by_ms[ms]["queueing_delay_ms"]))
        for ms in microservices
    ]
    per_hop_std = [
        float(np.std(by_ms[ms]["queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    positions = list(range(len(microservices)))
    bar_width = style.bar_width_fraction * style.bar_spacing_fraction
    for idx, (pos, height, err) in enumerate(zip(positions, per_hop_mean, per_hop_std)):
        color = style.colors[idx % len(style.colors)]
        ax.bar(
            pos,
            height,
            bar_width,
            yerr=err,
            capsize=3,
            color=color,
            edgecolor="black",
            linewidth=0.6,
            error_kw={"elinewidth": 0.8, "ecolor": "black", "capthick": 0.8},
        )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.asarray(per_hop_mean, dtype=float) + np.asarray(per_hop_std, dtype=float)
    finalize_violin_y_axis(ax, combined, style=style)


def plot_slo_violation_pct_bars(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    violation_pct = [
        microservice_slo_violation_pct(by_ms[ms])
        for ms in microservices
    ]
    positions = list(range(len(microservices)))
    bar_width = style.bar_width_fraction * style.bar_spacing_fraction
    for idx, (pos, height) in enumerate(zip(positions, violation_pct)):
        color = style.colors[idx % len(style.colors)]
        ax.bar(
            pos,
            height,
            bar_width,
            color=color,
            edgecolor="black",
            linewidth=0.6,
        )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    finalize_violin_y_axis(ax, np.asarray(violation_pct, dtype=float), style=style)


def plot_replica_utilization_dots(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    server_util = data["server_utilization_pct"]
    positions = list(range(len(microservices)))
    all_util: list[float] = []
    for idx, ms in enumerate(microservices):
        by_replica = server_util[ms]
        replicas = sorted(by_replica, key=lambda k: int(k))
        n = len(replicas)
        color = style.colors[idx % len(style.colors)]
        for r in replicas:
            util = float(by_replica[r])
            all_util.append(util)
            jitter = 0.0 if n <= 1 else (int(r) - (n - 1) / 2) * 0.04
            ax.scatter(
                idx + jitter,
                util,
                color=color,
                s=style.marker_size**2,
                edgecolors="black",
                linewidths=0.4,
                zorder=3,
            )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.asarray(all_util, dtype=float) if all_util else np.asarray([0.0, 100.0])
    finalize_violin_y_axis(ax, combined, style=style)
    hi = ax.get_ylim()[1]
    ax.set_ylim(0.0, min(hi, 100.0 + style.axis_guard_fraction * 100.0))


def plot_distributions(
    data: dict,
    *,
    microservices: list[str],
    output: Path,
    style=ACM_COMPACT_HALF,
) -> None:
    grid = SubplotGrid(style, layout="4x2")

    plot_cumulative_queueing_violinplot(
        grid.get_ax(0, 0),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(0, 0),
        xlabel="Microservice index",
        ylabel="Cum. Queue. (ms)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    plot_cumulative_queueing_stddev_bars(
        grid.get_ax(0, 1),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(0, 1),
        xlabel="Microservice index",
        ylabel="Cum. Queue. std (ms)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    plot_metric_violinplot(
        grid.get_ax(1, 0),
        data,
        microservices,
        "response_time_ms",
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(1, 0),
        xlabel="Microservice index",
        ylabel="Response Time (ms)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    plot_replica_avg_queue_inflight_dots(
        grid.get_ax(1, 1),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(1, 1),
        xlabel="Microservice index",
        ylabel="Avg occupancy",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    slack_d_ax = grid.get_ax(2, 0)
    slack_d_series: list[tuple[str, np.ndarray]] = []
    for ms in microservices:
        samples = np.asarray(data["by_microservice"][ms]["slack_d_ms"], dtype=float)
        slack_d_series.append((ms, samples))

    slack_d_nonempty = [samples for _, samples in slack_d_series if len(samples) > 0]
    slack_d_xlim = None
    if slack_d_nonempty:
        slack_d_combined = np.concatenate(slack_d_nonempty)
        slack_d_xlim = (
            float(np.min(slack_d_combined)),
            float(np.max(slack_d_combined)),
        )

    for ms_col, (ms, samples) in enumerate(slack_d_series):
        plot_cdf(
            slack_d_ax,
            samples,
            label=ms,
            style=style,
            color_idx=ms_col,
            xlim=slack_d_xlim,
        )

    if slack_d_xlim is not None:
        configure_x_axis_ticks(
            slack_d_ax,
            style=style,
            xlim=slack_d_xlim,
            x_step=_fallback_y_step(slack_d_xlim[1] - slack_d_xlim[0], min_ticks=5),
        )

    grid.configure_ax(
        slack_d_ax,
        xlabel="Slack-d (ms)",
        ylabel="CDF",
        title="",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
    )

    plot_per_hop_queueing_stddev_bars(
        grid.get_ax(2, 1),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(2, 1),
        xlabel="Microservice index",
        ylabel="Queuing (ms)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    plot_replica_utilization_dots(
        grid.get_ax(3, 0),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(3, 0),
        xlabel="Microservice index",
        ylabel="Utilization (%)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    plot_slo_violation_pct_bars(
        grid.get_ax(3, 1),
        data,
        microservices,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(3, 1),
        xlabel="Microservice index",
        ylabel="SLO violations (%)",
        title="SLO violations",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    legend_handles, legend_labels = slack_d_ax.get_legend_handles_labels()
    grid.add_shared_legend(
        handles=legend_handles,
        labels=legend_labels,
        position="top",
        wrap_to_plot_width=False,
        two_rows=True,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    grid.save(output)
    print(f"wrote {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ms simulation and plot per-microservice visit distribution CDFs.",
    )
    parser.add_argument(
        "--chain",
        type=int,
        default=None,
        help=(
            "Chain length under tests/chain/<N>/ "
            f"(default: {DEFAULT_CHAIN} when --callgraph is not set)"
        ),
    )
    parser.add_argument(
        "--callgraph",
        type=Path,
        default=None,
        help=(
            "Path to callgraph.json (relative to repo root or absolute). "
            "Overrides --chain."
        ),
    )
    parser.add_argument(
        "--load-file",
        type=Path,
        default=None,
        help="Path to load.json (default: load.json beside --callgraph)",
    )
    parser.add_argument("--n", type=int, default=100_000_0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rps", type=float, default=None)
    parser.add_argument(
        "--slo",
        type=float,
        default=None,
        help="Override SLO latency threshold in milliseconds (passed to ms as --slo-ms)",
    )
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
        "--approx-sched",
        choices=MS_APPROX_SCHED_POLICIES,
        default=None,
        help="Approx outbound pull scheduling: fcfs or edf (only valid with --lb-policy approx)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output PDF path (default: "
            f"{OUTPUT_DIR}/{OUTPUT_BASENAME}_<callgraph-relative-path>.pdf)"
        ),
    )
    parser.add_argument("--comment", type=str, default=None)
    parser.add_argument("--ms-binary", type=Path, default=None)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--force-fixed-svc",
        action="store_true",
        help="Force constant service times using callgraph means (no exponential sampling)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    callgraph = resolve_callgraph_path(chain=args.chain, callgraph=args.callgraph)
    load_file = resolve_load_file_path(callgraph, args.load_file)
    if not callgraph.is_file():
        raise SystemExit(f"callgraph not found: {callgraph}")
    if not load_file.is_file():
        raise SystemExit(f"load file not found: {load_file}")
    if args.lb_policy == "approx" and args.pull_policy is None:
        raise SystemExit("--pull-policy is required when --lb-policy approx")
    if args.lb_policy != "approx" and args.pull_policy is not None:
        raise SystemExit("--pull-policy is only valid with --lb-policy approx")
    if args.approx_sched is not None and args.lb_policy != "approx":
        raise SystemExit("--approx-sched is only valid with --lb-policy approx")
    validate_prequal_subset(args.lb_policy, args.lb_subset_size)

    binary = args.ms_binary
    if binary is None and not args.no_build:
        binary = ensure_release_binary(REPO_ROOT, None, simulator="ms")
    elif binary is None:
        binary = REPO_ROOT / "target" / "release" / "ms"

    data = run_ms_simulation(
        binary,
        callgraph=callgraph,
        load_file=load_file,
        n=args.n,
        lb_policy=args.lb_policy,
        pull_policy=args.pull_policy,
        lb_subset_size=args.lb_subset_size,
        seed=args.seed,
        rps=args.rps,
        slo_ms=args.slo,
        scheduling=args.scheduling,
        force_fixed_svc=args.force_fixed_svc,
        approx_sched=args.approx_sched,
    )
    if "by_microservice" not in data:
        raise SystemExit("ms JSON missing by_microservice; rebuild the ms binary")
    if "microservice_order" not in data:
        raise SystemExit("ms JSON missing microservice_order; rebuild the ms binary")
    if "server_utilization_pct" not in data:
        raise SystemExit("ms JSON missing server_utilization_pct; rebuild the ms binary")
    if "server_avg_queue_inflight" not in data:
        raise SystemExit("ms JSON missing server_avg_queue_inflight; rebuild the ms binary")
    for ms in microservice_order(data):
        if ms not in data["server_utilization_pct"]:
            raise SystemExit(f"ms JSON missing server_utilization_pct for {ms}")
        if ms not in data["server_avg_queue_inflight"]:
            raise SystemExit(f"ms JSON missing server_avg_queue_inflight for {ms}")
        ms_stats = data["by_microservice"][ms]
        if "slack_d_ms" not in ms_stats:
            raise SystemExit("ms JSON missing by_microservice slack_d_ms; rebuild the ms binary")
        if "cumulative_queueing_delay_ms" not in ms_stats:
            raise SystemExit(
                "ms JSON missing by_microservice cumulative_queueing_delay_ms; "
                "rebuild the ms binary"
            )
        if "prob_latency_gt_slo" not in ms_stats:
            raise SystemExit(
                "ms JSON missing by_microservice prob_latency_gt_slo; "
                "rebuild the ms binary"
            )

    output_base = args.output or default_output_path(callgraph)
    output = output_path_with_comment(output_base, args.comment)
    plot_distributions(data, microservices=microservice_order(data), output=output)


if __name__ == "__main__":
    main()
