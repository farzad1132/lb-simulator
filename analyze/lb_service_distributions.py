#!/usr/bin/env python3
"""Plot LB task distribution CDFs across service-time and arrival distributions."""

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
    LB_POLICIES,
    ensure_release_binary,
    output_path_with_comment,
    run_simulation,
)
from plotting_primitive import (  # noqa: E402
    ACM_COMPACT_HALF,
    SubplotGrid,
    configure_x_axis_ticks,
    distinct_series_styles,
    plot_cdf,
)

DEFAULT_OUTPUT = REPO_ROOT / "output" / "lb_service_distributions.pdf"

REQUIRED_JSON_KEYS = (
    "inter_arrival",
    "inter_departure",
    "processing_times",
    "queueing_delays",
    "e2e",
)

ROW_SPECS = (
    ("inter_arrival", "Inter-arrival (s)"),
    ("inter_departure", "Inter-departure (s)"),
    ("processing_times", "Processing time (s)"),
    ("queueing_delays", "Queueing delay (s)"),
    ("e2e", "E2E latency (s)"),
)

# (service_dist, arrival, title, service_modes, service_mode_probs)
DIST_SPECS = (
    ("constant", "constant", "Const svc / Const arr", None, None),
    ("constant", "exponential", "Const svc / Exp arr", None, None),
    ("exponential", "constant", "Exp svc / Const arr", None, None),
    ("exponential", "exponential", "Exp svc / Exp arr", None, None),
    ("bimodal", "constant", "Bimodal svc / Const arr", [0.5, 5.5], [0.9, 0.1]),
    ("bimodal", "exponential", "Bimodal svc / Exp arr", [0.5, 5.5], [0.9, 0.1]),
)

ZERO_BASED_FIELDS = frozenset({"inter_arrival", "inter_departure"})


def row_xlim(field: str, combined: np.ndarray) -> tuple[float, float]:
    hi = float(np.max(combined))
    if field in ZERO_BASED_FIELDS:
        return 0.0, hi
    return float(np.min(combined)), hi


def _fallback_x_step(span: float) -> float:
    if span <= 0:
        return 1.0
    raw = span / 5.0
    magnitude = 10 ** math.floor(math.log10(max(raw, 1e-12)))
    for mult in (1, 2, 5, 10):
        step = mult * magnitude
        if span / step <= 6:
            return step
    return raw


def finalize_subplot_x_axis(
    ax,
    combined: np.ndarray,
    xlim: tuple[float, float],
    *,
    style=ACM_COMPACT_HALF,
) -> None:
    lo, hi = xlim
    configure_x_axis_ticks(ax, x_data=combined, style=style, xlim=xlim)
    ticks = [float(t) for t in ax.get_xticks() if lo <= float(t) <= hi]
    if len(ticks) < 3 and hi > lo:
        step = _fallback_x_step(hi - lo)
        hi = math.ceil(hi / step) * step
        ticks = list(np.arange(lo, hi + step / 2, step))
        ticks = [float(t) for t in ticks if lo <= t <= hi]
    if ticks:
        ax.set_xticks(ticks)
        ax.set_xticklabels(
            [f"{t:g}" for t in ticks],
            fontsize=style.font_size - 1,
        )
    ax.set_xlim(lo, hi)


def validate_lb_data(data: dict) -> None:
    missing = [key for key in REQUIRED_JSON_KEYS if key not in data]
    if missing:
        raise SystemExit(
            f"lb JSON missing required keys: {', '.join(missing)}; rebuild the lb binary"
        )


def run_all_simulations(args: argparse.Namespace, binary: Path) -> list[tuple[str, dict]]:
    if args.lb_policy == "approx" and args.pull_policy is None:
        raise SystemExit("--pull-policy is required when --lb-policy approx")
    if args.lb_policy != "approx" and args.pull_policy is not None:
        raise SystemExit("--pull-policy is only valid with --lb-policy approx")
    results: list[tuple[str, dict]] = []
    for service_dist, arrival, title, modes, probs in DIST_SPECS:
        data = run_simulation(
            binary,
            load=args.load,
            n=args.n,
            service_dist=service_dist,
            arrival=arrival,
            servers=args.servers,
            concurrency=args.concurrency,
            clients=args.clients,
            lb_policy=args.lb_policy,
            pull_policy=args.pull_policy,
            lb_subset_size=args.lb_subset_size,
            service_modes=modes,
            service_mode_probs=probs,
            seed=args.seed,
        )
        validate_lb_data(data)
        results.append((title, data))
    return results


def plot_distributions(
    results: list[tuple[str, dict]],
    *,
    output: Path,
    style=ACM_COMPACT_HALF,
) -> None:
    grid = SubplotGrid(style, layout="3x2")
    ncols = 2

    for idx, (field, xlabel) in enumerate(ROW_SPECS):
        row, col = divmod(idx, ncols)
        ax = grid.get_ax(row, col)
        series: list[tuple[str, np.ndarray]] = [
            (title, np.asarray(data[field], dtype=float))
            for title, data in results
        ]

        nonempty = [samples for _, samples in series if len(samples) > 0]
        if not nonempty:
            continue
        combined = np.concatenate(nonempty)
        xlim = row_xlim(field, combined)
        series_styles = distinct_series_styles(len(series), style)

        for dist_col, (title, samples) in enumerate(series):
            line_style = series_styles[dist_col]
            plot_cdf(
                ax,
                samples,
                label=title,
                style=style,
                color_idx=dist_col,
                xlim=xlim,
                show_markers=True,
                markevery=max(1, len(samples) // 10),
                color=line_style["color"],
                marker=line_style["marker"],
                linestyle=line_style["linestyle"],
            )

        finalize_subplot_x_axis(ax, combined, xlim, style=style)

        grid.configure_ax(
            ax,
            xlabel=xlabel,
            ylabel="CDF" if col == 0 else "",
            show_xlabel=True,
            show_ylabel=col == 0,
            show_title=False,
            show_xticklabels=True,
            show_yticklabels=col == 0,
            auto_ticks=False,
        )

    grid.get_ax(2, 1).set_visible(False)

    grid.add_shared_legend(position="top", two_rows=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    grid.save(output)
    print(f"wrote {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run lb simulator and plot task distribution CDFs for all "
            "service-time × arrival distribution combinations."
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
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--comment", type=str, default=None)
    parser.add_argument("--lb-binary", type=Path, default=None)
    parser.add_argument("--no-build", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    binary = args.lb_binary
    if binary is None and not args.no_build:
        binary = ensure_release_binary(REPO_ROOT, None, simulator="lb")
    elif binary is None:
        binary = REPO_ROOT / "target" / "release" / "lb"

    results = run_all_simulations(args, binary)
    output = output_path_with_comment(args.output, args.comment)
    plot_distributions(results, output=output)


if __name__ == "__main__":
    main()
