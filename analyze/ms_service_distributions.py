#!/usr/bin/env python3
"""Plot per-microservice visit distributions for an ms chain topology."""

from __future__ import annotations

import argparse
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
    ensure_release_binary,
    output_path_with_comment,
    run_ms_simulation,
)
from plotting_primitive import ACM_COMPACT_HALF, SubplotGrid, plot_cdf  # noqa: E402

DEFAULT_CHAIN3_CALLGRAPH = REPO_ROOT / "tests" / "chain" / "6" / "callgraph.json"
DEFAULT_CHAIN3_LOAD = REPO_ROOT / "tests" / "chain" / "6" / "load.json"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "ms_service_distributions_chain6.pdf"

ROW_SPECS = (
    ("inter_arrival_ms", "Inter-arrival (ms)", False),
    ("inter_departure_ms", "Inter-departure (ms)", False),
    ("processing_time_ms", "Norm. Processing Time", True),
    ("queueing_delay_ms", "Norm. Queueing", True),
)


def microservice_order(data: dict) -> list[str]:
    by_ms = data.get("by_microservice", {})
    if not by_ms:
        raise SystemExit("simulation output missing by_microservice")
    return sorted(by_ms.keys())


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

    grid = SubplotGrid(style, layout="2x2")
    nrows, ncols = 2, 2
    for idx, (field, xlabel, normalize) in enumerate(ROW_SPECS):
        row, col = divmod(idx, ncols)
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

    grid.add_shared_legend(position="top", wrap_to_plot_width=False, two_rows=True)
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
    parser.add_argument("--n", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rps", type=float, default=None)
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default="power-of-two")
    parser.add_argument("--lb-subset-size", type=int, default=0)
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
    )
    if "by_microservice" not in data:
        raise SystemExit("ms JSON missing by_microservice; rebuild the ms binary")
    if "total_processing_p99_ms" not in data:
        raise SystemExit("ms JSON missing total_processing_p99_ms; rebuild the ms binary")

    output = output_path_with_comment(args.output, args.comment)
    plot_distributions(data, microservices=microservice_order(data), output=output)


if __name__ == "__main__":
    main()
