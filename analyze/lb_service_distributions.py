#!/usr/bin/env python3
"""Plot client/server hop distributions for the flat LB simulator."""

from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from pathlib import Path

_CACHE_ROOT = Path(tempfile.gettempdir()) / "lb-analyze-cache"
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
    DEFAULT_BIMODAL_MODES,
    DEFAULT_BIMODAL_PROBS,
    LB_APPROX_SCHED_POLICIES,
    LB_POLICIES,
    LB_SERVICE_DISTS,
    PULL_POLICIES,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plotting_primitive import (  # noqa: E402
    ACM_COMPACT_HALF,
    SubplotGrid,
    percentile,
    plot_grouped_bars,
)

OUTPUT_DIR = REPO_ROOT / "output"
OUTPUT_BASENAME = "lb_service_distributions"
ARRIVAL_DISTS = ("exponential", "constant")


def default_output_path(
    *,
    lb_policy: str,
    pull_policy: str | None,
    approx_sched: str | None,
) -> Path:
    """Tag the default PDF so approx/push runs do not overwrite each other."""
    parts = [OUTPUT_BASENAME, lb_policy.replace("-", "")]
    if lb_policy == "approx" and pull_policy is not None:
        parts.append(pull_policy.replace("-", ""))
    if approx_sched is not None:
        parts.append(approx_sched)
    return OUTPUT_DIR / f"{'_'.join(parts)}.pdf"


def hop_slo_violation_pct(hop_stats: dict) -> float:
    if "prob_latency_gt_slo" in hop_stats and hop_stats["prob_latency_gt_slo"] is not None:
        return 100.0 * float(hop_stats["prob_latency_gt_slo"])
    return 0.0


def hop_order(data: dict) -> list[str]:
    order = data.get("hop_order")
    if order is not None:
        return list(order)
    raise SystemExit("lb JSON missing hop_order; rebuild the lb binary")


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
    hops: list[str],
    field: str,
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_hop = data["by_hop"]
    violin_data = [
        np.asarray(by_hop[hop][field], dtype=float)
        for hop in hops
    ]
    positions = list(range(len(hops)))
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
    ax.set_xticklabels(
        [f"{i}\n{hop}" for i, hop in enumerate(hops)],
        fontsize=style.font_size - 1,
    )
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


def _set_hop_xticks(ax, hops: list[str], *, style=ACM_COMPACT_HALF) -> None:
    positions = list(range(len(hops)))
    ax.set_xticks(positions)
    ax.set_xticklabels(
        [f"{i}\n{hop}" for i, hop in enumerate(hops)],
        fontsize=style.font_size - 1,
    )


def plot_cumulative_queueing_violinplot(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    plot_metric_violinplot(
        ax,
        data,
        hops,
        "cumulative_queueing_delay",
        style=style,
    )


def plot_cumulative_queueing_stddev_bars(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_hop = data["by_hop"]
    per_hop_var = [
        float(np.var(by_hop[hop]["queueing_delay"], ddof=0))
        for hop in hops
    ]
    theoretical_std = [
        math.sqrt(sum(per_hop_var[: idx + 1]))
        for idx in range(len(hops))
    ]
    actual_std = [
        float(np.std(by_hop[hop]["cumulative_queueing_delay"], ddof=0))
        for hop in hops
    ]
    positions = list(range(len(hops)))
    plot_grouped_bars(
        ax,
        positions,
        [
            ("Independent", theoretical_std, None),
            ("Actual", actual_std, None),
        ],
        style=style,
    )
    _set_hop_xticks(ax, hops, style=style)
    combined = np.asarray(theoretical_std + actual_std, dtype=float)
    finalize_violin_y_axis(ax, combined, style=style)
    ax.legend(fontsize=style.legend_size, loc="upper left", frameon=False)


def plot_replica_avg_queue_inflight_dots(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    avg_queue_inflight = data["server_avg_queue_inflight"]
    positions = list(range(len(hops)))
    all_avg: list[float] = []
    for idx, hop in enumerate(hops):
        by_replica = avg_queue_inflight.get(hop) or {}
        replicas = sorted(by_replica, key=lambda k: int(k))
        color = style.colors[idx % len(style.colors)]
        for r in replicas:
            avg = float(by_replica[r])
            all_avg.append(avg)
            ax.scatter(
                idx,
                avg,
                color=color,
                s=style.marker_size**2,
                edgecolors="black",
                linewidths=0.4,
                zorder=3,
            )
    _set_hop_xticks(ax, hops, style=style)
    combined = np.asarray(all_avg, dtype=float) if all_avg else np.asarray([0.0, 1.0])
    finalize_violin_y_axis(ax, combined, style=style)


def plot_per_hop_queueing_stddev_bars(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_hop = data["by_hop"]
    per_hop_mean = [
        float(np.mean(by_hop[hop]["queueing_delay"]))
        for hop in hops
    ]
    per_hop_std = [
        float(np.std(by_hop[hop]["queueing_delay"], ddof=0))
        for hop in hops
    ]
    positions = list(range(len(hops)))
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
    _set_hop_xticks(ax, hops, style=style)
    combined = np.asarray(per_hop_mean, dtype=float) + np.asarray(per_hop_std, dtype=float)
    finalize_violin_y_axis(ax, combined, style=style)


def plot_slo_violation_pct_bars(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_hop = data["by_hop"]
    violation_pct = [
        hop_slo_violation_pct(by_hop[hop])
        for hop in hops
    ]
    positions = list(range(len(hops)))
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
    _set_hop_xticks(ax, hops, style=style)
    finalize_violin_y_axis(ax, np.asarray(violation_pct, dtype=float), style=style)


def plot_replica_utilization_dots(
    ax,
    data: dict,
    hops: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    server_util = data["server_utilization_pct"]
    positions = list(range(len(hops)))
    all_util: list[float] = []
    for idx, hop in enumerate(hops):
        by_replica = server_util.get(hop) or {}
        replicas = sorted(by_replica, key=lambda k: int(k))
        color = style.colors[idx % len(style.colors)]
        for r in replicas:
            util = float(by_replica[r])
            all_util.append(util)
            ax.scatter(
                idx,
                util,
                color=color,
                s=style.marker_size**2,
                edgecolors="black",
                linewidths=0.4,
                zorder=3,
            )
    _set_hop_xticks(ax, hops, style=style)
    combined = np.asarray(all_util, dtype=float) if all_util else np.asarray([0.0, 100.0])
    finalize_violin_y_axis(ax, combined, style=style)
    hi = ax.get_ylim()[1]
    ax.set_ylim(0.0, min(hi, 100.0 + style.axis_guard_fraction * 100.0))


def plot_distributions(
    data: dict,
    *,
    hops: list[str],
    output: Path,
    style=ACM_COMPACT_HALF,
) -> None:
    grid = SubplotGrid(style, layout="4x2")

    plot_cumulative_queueing_violinplot(
        grid.get_ax(0, 0),
        data,
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(0, 0),
        xlabel="Hop index",
        ylabel="Cum. Queue. (s)",
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
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(0, 1),
        xlabel="Hop index",
        ylabel="Cum. Queue. std (s)",
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
        hops,
        "response_time",
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(1, 0),
        xlabel="Hop index",
        ylabel="Response Time (s)",
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
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(1, 1),
        xlabel="Hop index",
        ylabel="Avg occupancy",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    # No slack-d for LB; hide the MS-equivalent panel.
    grid.get_ax(2, 0).set_visible(False)

    plot_per_hop_queueing_stddev_bars(
        grid.get_ax(2, 1),
        data,
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(2, 1),
        xlabel="Hop index",
        ylabel="Queuing (s)",
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
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(3, 0),
        xlabel="Hop index",
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
        hops,
        style=style,
    )
    grid.configure_ax(
        grid.get_ax(3, 1),
        xlabel="Hop index",
        ylabel="SLO violations (%)",
        title="SLO violations",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    output.parent.mkdir(parents=True, exist_ok=True)
    grid.save(output)
    print(f"wrote {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run lb simulation and plot client/server hop distributions "
            "(index 0=client LB queue, 1=server)."
        ),
    )
    parser.add_argument("--load", type=float, default=0.8)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--servers", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default="power-of-two")
    parser.add_argument(
        "--pull-policy",
        choices=PULL_POLICIES,
        default=None,
        help="Required when --lb-policy approx",
    )
    parser.add_argument("--lb-subset-size", type=int, default=0)
    parser.add_argument(
        "--service-dist",
        choices=LB_SERVICE_DISTS,
        default="exponential",
        help="Service-time distribution (default: exponential)",
    )
    parser.add_argument(
        "--arrival",
        choices=ARRIVAL_DISTS,
        default="exponential",
        help="Inter-arrival distribution (default: exponential)",
    )
    parser.add_argument(
        "--slo",
        type=float,
        default=None,
        help="SLO latency threshold in seconds (enables SLO violation panel)",
    )
    parser.add_argument(
        "--approx-sched",
        choices=LB_APPROX_SCHED_POLICIES,
        default=None,
        help=(
            "Approx unbound queue scheduling: fcfs "
            "(omit for bound 1:1 pulls; only valid with --lb-policy approx)"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output PDF path (default: "
            f"{OUTPUT_DIR}/{OUTPUT_BASENAME}_<policy>[_pull][_sched].pdf)"
        ),
    )
    parser.add_argument("--comment", type=str, default=None)
    parser.add_argument("--lb-binary", type=Path, default=None)
    parser.add_argument("--no-build", action="store_true")
    return parser.parse_args()


def validate_lb_hop_data(data: dict) -> None:
    for key in (
        "by_hop",
        "hop_order",
        "server_utilization_pct",
        "server_avg_queue_inflight",
    ):
        if key not in data:
            raise SystemExit(f"lb JSON missing {key}; rebuild the lb binary")
    for hop in hop_order(data):
        if hop not in data["by_hop"]:
            raise SystemExit(f"lb JSON missing by_hop.{hop}; rebuild the lb binary")
        hop_stats = data["by_hop"][hop]
        for field in (
            "response_time",
            "queueing_delay",
            "cumulative_queueing_delay",
            "processing_time",
        ):
            if field not in hop_stats:
                raise SystemExit(
                    f"lb JSON missing by_hop.{hop}.{field}; rebuild the lb binary"
                )
        if hop not in data["server_utilization_pct"]:
            raise SystemExit(f"lb JSON missing server_utilization_pct for {hop}")
        if hop not in data["server_avg_queue_inflight"]:
            raise SystemExit(f"lb JSON missing server_avg_queue_inflight for {hop}")


def main() -> None:
    args = parse_args()
    if args.lb_policy == "approx" and args.pull_policy is None:
        raise SystemExit("--pull-policy is required when --lb-policy approx")
    if args.lb_policy != "approx" and args.pull_policy is not None:
        raise SystemExit("--pull-policy is only valid with --lb-policy approx")
    if args.approx_sched is not None and args.lb_policy != "approx":
        raise SystemExit("--approx-sched is only valid with --lb-policy approx")
    if args.lb_policy == "prequal" and args.lb_subset_size > 0:
        raise SystemExit("--lb-subset-size is not supported with --lb-policy prequal")

    binary = args.lb_binary
    if binary is None and not args.no_build:
        binary = ensure_release_binary(REPO_ROOT, None, simulator="lb")
    elif binary is None:
        binary = REPO_ROOT / "target" / "release" / "lb"

    service_modes = None
    service_mode_probs = None
    if args.service_dist == "bimodal":
        service_modes = list(DEFAULT_BIMODAL_MODES)
        service_mode_probs = list(DEFAULT_BIMODAL_PROBS)

    data = run_simulation(
        binary,
        load=args.load,
        n=args.n,
        service_dist=args.service_dist,
        arrival=args.arrival,
        servers=args.servers,
        concurrency=args.concurrency,
        clients=args.clients,
        lb_policy=args.lb_policy,
        pull_policy=args.pull_policy,
        lb_subset_size=args.lb_subset_size,
        service_modes=service_modes,
        service_mode_probs=service_mode_probs,
        seed=args.seed,
        slo=args.slo,
        approx_sched=args.approx_sched,
    )
    validate_lb_hop_data(data)

    output_base = args.output or default_output_path(
        lb_policy=args.lb_policy,
        pull_policy=args.pull_policy,
        approx_sched=args.approx_sched,
    )
    output = output_path_with_comment(output_base, args.comment)
    plot_distributions(data, hops=hop_order(data), output=output)


if __name__ == "__main__":
    main()
