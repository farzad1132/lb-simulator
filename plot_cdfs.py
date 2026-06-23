#!/usr/bin/env python3
"""Run the lb simulator and plot normalized e2e latency CDF."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Optional

from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    plot_cdf,
)

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "e2e_cdf.pdf"
REQUIRED_JSON_KEYS = ("utilization_pct", "normalized_e2e")


def _print_subprocess_failure(
    label: str,
    cmd: list[str],
    *,
    returncode: Optional[int] = None,
    stdout: str = "",
    stderr: str = "",
) -> None:
    print(f"{label} failed", file=sys.stderr)
    print(f"  command: {shlex.join(cmd)}", file=sys.stderr)
    if returncode is not None:
        print(f"  exit code: {returncode}", file=sys.stderr)
    if stderr:
        print("  stderr:", file=sys.stderr)
        print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")
    else:
        print("  stderr: (empty)", file=sys.stderr)
    if stdout:
        print("  stdout:", file=sys.stderr)
        print(stdout, file=sys.stderr, end="" if stdout.endswith("\n") else "\n")
    else:
        print("  stdout: (empty)", file=sys.stderr)


def run_subprocess(
    cmd: list[str],
    *,
    label: str,
    cwd: Optional[Path] = None,
    env: Optional[dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=cwd,
            env=env,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"{label}: command not found: {cmd[0]}") from exc
    except subprocess.CalledProcessError as exc:
        _print_subprocess_failure(
            label,
            cmd,
            returncode=exc.returncode,
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
        )
        raise SystemExit(f"{label} failed with exit code {exc.returncode}") from exc


def ensure_release_binary(repo_root: Path, binary: Path | None) -> Path:
    if binary is None:
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(repo_root / "target")
        run_subprocess(
            ["cargo", "build", "--release"],
            label="cargo build",
            cwd=repo_root,
            env=env,
        )
        return repo_root / "target" / "release" / "lb"
    return binary


def _parse_simulation_json(cmd: list[str], stdout: str) -> dict:
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        _print_subprocess_failure(
            "simulator (invalid JSON)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit("simulator did not emit valid JSON") from exc

    missing = [key for key in REQUIRED_JSON_KEYS if key not in data]
    if missing:
        _print_subprocess_failure(
            "simulator (missing JSON keys)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit(f"simulator JSON missing required keys: {', '.join(missing)}")

    return data


def run_simulation(
    binary: Path,
    *,
    arrival_mean: float,
    service_mean: float,
    n: int,
    service_dist: str,
) -> dict:
    cmd = [
        str(binary),
        "--format",
        "json",
        "--arrival-mean",
        str(arrival_mean),
        "--service-mean",
        str(service_mean),
        "--n",
        str(n),
        "--service-dist",
        service_dist,
    ]
    result = run_subprocess(cmd, label="simulator")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    return _parse_simulation_json(cmd, result.stdout)


def plot_e2e_cdf(
    data: dict,
    output_path: Path,
    *,
    arrival_mean: float,
    service_mean: float,
    marks: Optional[list[float]] = None,
) -> None:
    style = ACM_COMPACT_HALF
    load = service_mean / arrival_mean
    grid = SubplotGrid(style, layout="1x1")
    plot_cdf(
        grid.get_ax(0, 0),
        data["normalized_e2e"],
        style=style,
        thresholds=marks,
        log_x=True,
        xlim=(1.0, 1000.0),
        xlabel="E2E Slowdown",
    )
    grid.configure_labels(
        pattern="leftmost_y_bottom_x",
        ylabel="CDF",
        title=f"load = {load:g}",
    )
    grid.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run lb simulator and plot normalized e2e latency CDF.",
    )
    parser.add_argument("--binary", type=Path, default=None,
                        help="Prebuilt release binary (skips cargo build --release)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Output PDF path")
    parser.add_argument("--arrival-mean", type=float, default=1.0)
    parser.add_argument("--service-mean", type=float, default=0.8)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--service-dist", choices=["exponential", "constant"],
                        default="exponential")
    parser.add_argument(
        "--mark", type=float, action="append", default=None,
        help="Slowdown value(s) to annotate on the CDF (e.g. --mark 5 --mark 10)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    binary = ensure_release_binary(REPO_ROOT, args.binary)
    data = run_simulation(
        binary,
        arrival_mean=args.arrival_mean,
        service_mean=args.service_mean,
        n=args.n,
        service_dist=args.service_dist,
    )
    if not data["normalized_e2e"]:
        print("no completed tasks", file=sys.stderr)
        sys.exit(1)
    plot_e2e_cdf(
        data,
        args.output,
        arrival_mean=args.arrival_mean,
        service_mean=args.service_mean,
        marks=args.mark,
    )
    print(
        f"wrote {args.output} (utilization: {data['utilization_pct']:.2f}%)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
