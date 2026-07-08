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
    LB_POLICIES,
    MS_SCHEDULING_POLICIES,
    ensure_release_binary,
    output_path_with_comment,
    run_ms_simulation,
)
from plotting_primitive import (  # noqa: E402
    ACM_COMPACT_HALF,
    SubplotGrid,
    percentile,
    plot_cdf,
    plot_grouped_bars,
)

DEFAULT_CHAIN3_CALLGRAPH = REPO_ROOT / "tests" / "chain" / "6" / "callgraph.json"
DEFAULT_CHAIN3_LOAD = REPO_ROOT / "tests" / "chain" / "6" / "load.json"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "ms_service_distributions_chain6.pdf"

ROW_SPECS = (
    ("processing_time_ms", "Norm. Processing Time", True),
    ("queueing_delay_ms", "Norm. Queueing", True),
    ("slack_d_ms", "Slack-d (ms)", False),
)


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


def plot_cumulative_queueing_violinplot(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    violin_data = [
        np.asarray(by_ms[ms]["cumulative_queueing_delay_ms"], dtype=float)
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
            ("Theoretical", theoretical_std, None),
            ("Actual", actual_std, None),
        ],
        style=style,
    )
    ax.set_xticks(positions)
    ax.set_xticklabels([str(i) for i in positions], fontsize=style.font_size - 1)
    combined = np.asarray(theoretical_std + actual_std, dtype=float)
    finalize_violin_y_axis(ax, combined, style=style)
    ax.legend(fontsize=style.legend_size, loc="upper left")


def plot_per_hop_queueing_stddev_bars(
    ax,
    data: dict,
    microservices: list[str],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    by_ms = data["by_microservice"]
    per_hop_std = [
        float(np.std(by_ms[ms]["queueing_delay_ms"], ddof=0))
        for ms in microservices
    ]
    positions = list(range(len(microservices)))
    bar_width = style.bar_width_fraction * style.bar_spacing_fraction
    for idx, (pos, height) in enumerate(zip(positions, per_hop_std)):
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
    finalize_violin_y_axis(ax, np.asarray(per_hop_std, dtype=float), style=style)


def plot_distributions(
    data: dict,
    *,
    microservices: list[str],
    output: Path,
    style=ACM_COMPACT_HALF,
) -> None:
    p99 = float(data["total_processing_p99_ms"])
    if p99 <= 0.0:
        raise SystemExit("total_processing_p99_ms must be positive")

    grid = SubplotGrid(style, layout="3x2")
    nrows, ncols = 3, 2

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

    for idx, (field, xlabel, normalize) in enumerate(ROW_SPECS):
        row, col = divmod(idx, ncols)
        row += 1
        ax = grid.get_ax(row, col)
        series: list[tuple[str, np.ndarray]] = []
        for ms in microservices:
            samples = np.asarray(data["by_microservice"][ms][field], dtype=float)
            if normalize:
                samples = samples / p99
            series.append((ms, samples))

        nonempty = [samples for _, samples in series if len(samples) > 0]
        xlim = None
        if nonempty:
            combined = np.concatenate(nonempty)
            xlim = (float(np.min(combined)), float(np.max(combined)))

        for ms_col, (ms, samples) in enumerate(series):
            plot_cdf(
                ax,
                samples,
                label=ms,
                style=style,
                color_idx=ms_col,
                xlim=xlim,
                xlabel=xlabel if row == nrows - 1 else None,
            )

        grid.configure_ax(
            ax,
            xlabel=xlabel,
            ylabel="CDF" if col == 0 else "",
            show_xlabel=True,
            show_ylabel=col == 0,
            show_title=True,
            show_xticklabels=True,
            show_yticklabels=col == 0,
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
        ylabel="Queue. std (ms)",
        show_xlabel=True,
        show_ylabel=True,
        show_title=True,
        show_xticklabels=True,
        show_yticklabels=True,
        auto_ticks=False,
    )

    cdf_ax = grid.get_ax(1, 0)
    legend_handles, legend_labels = cdf_ax.get_legend_handles_labels()
    grid.add_shared_legend(
        handles=legend_handles,
        labels=legend_labels,
        position="top",
        wrap_to_plot_width=False,
        two_rows=True,
    )
    """ grid.fig.suptitle(
        f"Normalized by total processing p99 = {p99:.3f} ms",
        fontsize=style.font_size,
        y=1.02,
    ) """
    output.parent.mkdir(parents=True, exist_ok=True)
    grid.save(output)
    print(f"wrote {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ms chain-3 and plot per-microservice visit distribution CDFs.",
    )
    parser.add_argument("--callgraph", type=Path, default=DEFAULT_CHAIN3_CALLGRAPH)
    parser.add_argument("--load-file", type=Path, default=DEFAULT_CHAIN3_LOAD)
    parser.add_argument("--n", type=int, default=100_000_0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rps", type=float, default=None)
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default="power-of-two")
    parser.add_argument("--lb-subset-size", type=int, default=0)
    parser.add_argument("--scheduling", choices=MS_SCHEDULING_POLICIES, default="fifo")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--comment", type=str, default=None)
    parser.add_argument("--ms-binary", type=Path, default=None)
    parser.add_argument("--no-build", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    binary = args.ms_binary
    if binary is None and not args.no_build:
        binary = ensure_release_binary(REPO_ROOT, None, simulator="ms")
    elif binary is None:
        binary = REPO_ROOT / "target" / "release" / "ms"

    data = run_ms_simulation(
        binary,
        callgraph=args.callgraph,
        load_file=args.load_file,
        n=args.n,
        lb_policy=args.lb_policy,
        lb_subset_size=args.lb_subset_size,
        seed=args.seed,
        rps=args.rps,
        scheduling=args.scheduling,
    )
    if "by_microservice" not in data:
        raise SystemExit("ms JSON missing by_microservice; rebuild the ms binary")
    if "microservice_order" not in data:
        raise SystemExit("ms JSON missing microservice_order; rebuild the ms binary")
    if "total_processing_p99_ms" not in data:
        raise SystemExit("ms JSON missing total_processing_p99_ms; rebuild the ms binary")
    for ms in microservice_order(data):
        ms_stats = data["by_microservice"][ms]
        if "slack_d_ms" not in ms_stats:
            raise SystemExit("ms JSON missing by_microservice slack_d_ms; rebuild the ms binary")
        if "cumulative_queueing_delay_ms" not in ms_stats:
            raise SystemExit(
                "ms JSON missing by_microservice cumulative_queueing_delay_ms; "
                "rebuild the ms binary"
            )

    output = output_path_with_comment(args.output, args.comment)
    plot_distributions(data, microservices=microservice_order(data), output=output)


if __name__ == "__main__":
    main()
