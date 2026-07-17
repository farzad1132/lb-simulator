#!/usr/bin/env python3
"""Run the lb or ms simulator and plot e2e latency CDF."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from plotting_primitive import (
    ACM_COMPACT_HALF,
    SubplotGrid,
    plot_cdf,
)

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_MS_BINARY = REPO_ROOT / "target" / "release" / "ms"
DEFAULT_OUTPUT = REPO_ROOT / "output" / "e2e_cdf.pdf"
DEFAULT_MS_OUTPUT = REPO_ROOT / "output" / "e2e_cdf_ms.pdf"
LB_REQUIRED_JSON_KEYS = ("utilization_pct", "e2e")
MS_REQUIRED_JSON_KEYS = ("microservice_utilization_pct", "by_api")
MS_API_REQUIRED_KEYS = ("e2e_ms", "slo_latency_ms", "unloaded_latency_p99_ms")
SERVICE_MEAN = 1.0
LB_POLICIES = (
    "random",
    "power-of-two",
    "least-request",
    "round-robin",
    "centralized",
    "approx",
    "prequal",
)
PULL_POLICIES = ("random", "power-of-two", "least-request", "round-robin")
MS_LB_POLICIES = (
    "random",
    "power-of-two",
    "least-request",
    "round-robin",
    "centralized",
    "approx",
    "prequal",
    "cl",
    "cl-lr",
    "corr",
)
MS_SCHEDULING_POLICIES = ("fifo", "edf")
MS_APPROX_SCHED_POLICIES = ("fcfs", "edf")
SIMULATORS = ("lb", "ms")


@dataclass
class CdfPanel:
    e2e: list[float]
    slo_latency: Optional[float]
    title: str


def arrival_mean_from_load(
    load: float,
    servers: int,
    concurrency: int,
    service_mean: float = SERVICE_MEAN,
    clients: int = 1,
) -> float:
    """Aggregate inter-arrival mean for target load (per-client mean is × clients)."""
    capacity = max(servers, 1) * max(concurrency, 1)
    return service_mean / (load * capacity)


def bimodal_service_mean(modes: list[float], probs: list[float]) -> float:
    """Expected service time for a bimodal mixture of exponentials."""
    return sum(m * p for m, p in zip(modes, probs))


def _sanitize_comment(comment: str) -> str:
    comment = comment.strip().replace("/", "_").replace("\\", "_")
    return re.sub(r"\s+", "_", comment)


def output_path_with_comment(path: Path, comment: str | None) -> Path:
    if not comment:
        return path
    safe = _sanitize_comment(comment)
    if not safe:
        return path
    return path.with_name(f"{path.stem}_{safe}{path.suffix}")


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


def ensure_release_binary(
    repo_root: Path,
    binary: Path | None,
    *,
    simulator: str = "lb",
) -> Path:
    if binary is None:
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(repo_root / "target")
        run_subprocess(
            ["cargo", "build", "--release"],
            label="cargo build",
            cwd=repo_root,
            env=env,
        )
        return repo_root / "target" / "release" / simulator
    return binary


def _parse_lb_json(cmd: list[str], stdout: str) -> dict:
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        _print_subprocess_failure(
            "simulator (invalid JSON)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit("simulator did not emit valid JSON") from exc

    missing = [key for key in LB_REQUIRED_JSON_KEYS if key not in data]
    if missing:
        _print_subprocess_failure(
            "simulator (missing JSON keys)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit(f"simulator JSON missing required keys: {', '.join(missing)}")

    return data


def _parse_ms_json(cmd: list[str], stdout: str) -> dict:
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        _print_subprocess_failure(
            "simulator (invalid JSON)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit("simulator did not emit valid JSON") from exc

    missing = [key for key in MS_REQUIRED_JSON_KEYS if key not in data]
    if missing:
        _print_subprocess_failure(
            "simulator (missing JSON keys)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit(f"simulator JSON missing required keys: {', '.join(missing)}")

    by_api = data["by_api"]
    if not isinstance(by_api, dict):
        _print_subprocess_failure(
            "simulator (invalid by_api)",
            cmd,
            stdout=stdout,
        )
        raise SystemExit("simulator JSON by_api must be an object")

    for api, stats in by_api.items():
        if not isinstance(stats, dict):
            raise SystemExit(f"simulator JSON by_api[{api!r}] must be an object")
        missing_api = [key for key in MS_API_REQUIRED_KEYS if key not in stats]
        if missing_api:
            _print_subprocess_failure(
                "simulator (missing JSON keys)",
                cmd,
                stdout=stdout,
            )
            raise SystemExit(
                f"simulator JSON by_api[{api!r}] missing required keys: "
                f"{', '.join(missing_api)}"
            )

    return data


def run_simulation(
    binary: Path,
    *,
    load: float,
    n: int,
    service_dist: str,
    arrival: str = "exponential",
    servers: int = 1,
    concurrency: int = 1,
    clients: int = 1,
    lb_policy: str = "power-of-two",
    pull_policy: str | None = None,
    lb_subset_size: int = 0,
    service_modes: list[float] | None = None,
    service_mode_probs: list[float] | None = None,
    seed: int | None = None,
    slo: float | None = None,
    expresslane: bool = False,
    express_size: int | None = None,
    express_del_th: float | None = None,
    express_th: int | None = None,
    ideal: bool = False,
    shed_delay: float | None = None,
    approx_sched: str | None = None,
) -> dict:
    cmd = [
        str(binary),
        "--format",
        "json",
        "--load",
        str(load),
        "--n",
        str(n),
        "--service-dist",
        service_dist,
        "--arrival",
        arrival,
        "--servers",
        str(servers),
        "--concurrency",
        str(concurrency),
        "--clients",
        str(clients),
        "--lb-policy",
        lb_policy,
        "--lb-subset-size",
        str(lb_subset_size),
    ]
    if pull_policy is not None:
        cmd.extend(["--pull-policy", pull_policy])
    if seed is not None:
        cmd.extend(["--seed", str(seed)])
    if service_modes is not None:
        cmd.extend(["--service-modes", ",".join(str(m) for m in service_modes)])
    if service_mode_probs is not None:
        cmd.extend(["--service-mode-probs", ",".join(str(p) for p in service_mode_probs)])
    if slo is not None:
        cmd.extend(["--slo", str(slo)])
    if expresslane:
        cmd.append("--expresslane")
        cmd.extend(["--express-size", str(express_size)])
        cmd.extend(["--express-del-th", str(express_del_th)])
        if express_th is not None:
            cmd.extend(["--express-th", str(express_th)])
        if ideal:
            cmd.append("--ideal")
    if shed_delay is not None:
        cmd.extend(["--shed-delay", str(shed_delay)])
    if approx_sched is not None:
        cmd.extend(["--approx-sched", approx_sched])
    result = run_subprocess(cmd, label="simulator")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    return _parse_lb_json(cmd, result.stdout)


def run_ms_simulation(
    binary: Path,
    *,
    callgraph: Path,
    load_file: Path,
    n: int,
    lb_policy: str = "power-of-two",
    pull_policy: str | None = None,
    lb_subset_size: int = 0,
    seed: int | None = None,
    rps: float | None = None,
    slo_ms: float | None = None,
    scheduling: str = "fifo",
    force_fixed_svc: bool = False,
    approx_sched: str | None = None,
) -> dict:
    cmd = [
        str(binary),
        "--format",
        "json",
        "--callgraph",
        str(callgraph),
        "--load-file",
        str(load_file),
        "--n",
        str(n),
        "--lb-policy",
        lb_policy,
        "--lb-subset-size",
        str(lb_subset_size),
        "--scheduling",
        scheduling,
    ]
    if pull_policy is not None:
        cmd.extend(["--pull-policy", pull_policy])
    if seed is not None:
        cmd.extend(["--seed", str(seed)])
    if rps is not None:
        cmd.extend(["--rps", str(rps)])
    if slo_ms is not None:
        cmd.extend(["--slo-ms", str(slo_ms)])
    if force_fixed_svc:
        cmd.append("--force-fixed-svc")
    if approx_sched is not None:
        cmd.extend(["--approx-sched", approx_sched])
    result = run_subprocess(cmd, label="simulator")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    return _parse_ms_json(cmd, result.stdout)


def load_api_names(load_file: Path) -> list[str]:
    try:
        data = json.loads(load_file.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"invalid JSON in load file {load_file}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"load file {load_file} must be a JSON object")
    return sorted(data.keys())


def select_ms_panels(data: dict, *, api: str | None, load_file: Path) -> list[CdfPanel]:
    by_api = data["by_api"]
    if api is not None:
        if api not in by_api:
            valid = ", ".join(sorted(by_api.keys())) or "(none)"
            raise SystemExit(f"API {api!r} not in simulation output; valid APIs: {valid}")
        api_names = [api]
    else:
        api_names = load_api_names(load_file)
        missing = [name for name in api_names if name not in by_api]
        if missing:
            valid = ", ".join(sorted(by_api.keys())) or "(none)"
            raise SystemExit(
                f"API(s) from load file missing in simulation output: {', '.join(missing)}; "
                f"valid APIs: {valid}"
            )

    panels = []
    for name in api_names:
        stats = by_api[name]
        panels.append(
            CdfPanel(
                e2e=stats["e2e_ms"],
                slo_latency=stats["slo_latency_ms"],
                title=f"api = {name}",
            )
        )
    return panels


def plot_e2e_cdf_panels(
    panels: list[CdfPanel],
    output_path: Path,
    *,
    xlabel: str,
    marks: list[float] | None = None,
) -> None:
    if not panels:
        raise SystemExit("no panels to plot")

    style = ACM_COMPACT_HALF
    n = len(panels)
    layout = "1x1" if n == 1 else f"{n}x1"
    grid = SubplotGrid(style, layout=layout)

    for idx, panel in enumerate(panels):
        row = idx if n > 1 else 0
        col = 0
        ax = grid.get_ax(row, col)
        thresholds: list[float] = []
        if panel.slo_latency is not None:
            thresholds.append(panel.slo_latency)
        if marks:
            thresholds.extend(marks)
        plot_cdf(
            ax,
            panel.e2e,
            style=style,
            thresholds=thresholds,
            log_x=True,
            xlabel=xlabel if row == n - 1 else "",
        )
        grid.configure_ax(
            ax,
            xlabel=xlabel if row == n - 1 else "",
            ylabel="CDF" if col == 0 else "",
            title=panel.title,
            show_xlabel=(row == n - 1),
            show_ylabel=(col == 0),
            show_xticklabels=(row == n - 1),
            show_yticklabels=(col == 0),
        )

    grid.save(output_path)


def plot_e2e_cdf(
    data: dict,
    output_path: Path,
    *,
    load: float,
    slo: Optional[float] = None,
    marks: Optional[list[float]] = None,
) -> None:
    slo_latency = data.get("slo_latency", slo)
    plot_e2e_cdf_panels(
        [CdfPanel(data["e2e"], slo_latency, f"load = {load:g}")],
        output_path,
        xlabel="E2E latency (s)",
        marks=marks,
    )


def format_ms_utilization(microservice_utilization_pct: dict) -> str:
    parts = [
        f"{ms}={pct:.1f}%"
        for ms, pct in sorted(microservice_utilization_pct.items())
    ]
    return " ".join(parts)


def resolve_lb_policy(simulator: str, lb_policy: str | None) -> str:
    if lb_policy is not None:
        return lb_policy
    return "power-of-two"


def validate_lb_args(args: argparse.Namespace) -> None:
    if args.callgraph is not None:
        raise SystemExit("--callgraph is only valid with --simulator ms")
    if args.load_file is not None:
        raise SystemExit("--load-file is only valid with --simulator ms")
    if args.api is not None:
        raise SystemExit("--api is only valid with --simulator ms")
    if args.slo is not None:
        raise SystemExit("--slo is only valid with --simulator lb")


def validate_prequal_subset(lb_policy: str, lb_subset_size: int) -> None:
    if lb_policy == "prequal" and lb_subset_size > 0:
        raise SystemExit("--lb-subset-size is not supported with --lb-policy prequal")


def validate_ms_args(args: argparse.Namespace) -> None:
    if args.callgraph is None:
        raise SystemExit("--callgraph is required with --simulator ms")
    if args.load_file is None:
        raise SystemExit("--load-file is required with --simulator ms")


def resolve_output_path(args: argparse.Namespace) -> Path:
    if args.output is not None:
        return args.output
    if args.simulator == "ms":
        return DEFAULT_MS_OUTPUT
    return DEFAULT_OUTPUT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run lb or ms simulator and plot e2e latency CDF.",
    )
    parser.add_argument(
        "--simulator",
        choices=SIMULATORS,
        default="lb",
        help="Which simulator to run (default: lb)",
    )
    parser.add_argument("--binary", type=Path, default=None,
                        help="Prebuilt release binary (skips cargo build --release)")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output PDF path (default: output/e2e_cdf.pdf or output/e2e_cdf_ms.pdf)")
    parser.add_argument(
        "--comment", type=str, default=None,
        help="Suffix appended to output filename before .pdf (e.g. e2e_cdf_foo.pdf)",
    )
    parser.add_argument("--callgraph", type=Path, default=None,
                        help="Callgraph JSON (required with --simulator ms)")
    parser.add_argument("--load-file", type=Path, default=None,
                        help="Per-API load JSON with rps and slo_ms (required with --simulator ms)")
    parser.add_argument("--api", type=str, default=None,
                        help="Plot only this API (ms mode); omit to plot all APIs from load file")
    parser.add_argument("--load", type=float, default=0.8)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument("--service-dist", choices=["exponential", "constant", "bimodal"],
                        default="exponential")
    parser.add_argument(
        "--service-modes", type=float, nargs=2, metavar=("M0", "M1"),
        help="Exponential means for bimodal modes (required with --service-dist bimodal)",
    )
    parser.add_argument(
        "--service-mode-probs", type=float, nargs=2, metavar=("P0", "P1"),
        help="Mode selection probabilities (required with --service-dist bimodal)",
    )
    parser.add_argument("--servers", type=int, default=1,
                        help="Number of servers (passed to lb simulator)")
    parser.add_argument("--concurrency", type=int, default=1,
                        help="Concurrent tasks per server (passed to lb simulator)")
    parser.add_argument("--clients", type=int, default=1,
                        help="Number of independent clients (passed to lb simulator)")
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default=None,
                        help="Load-balancing policy (default: power-of-two)")
    parser.add_argument("--lb-subset-size", type=int, default=0,
                        help="Replicas each LB can route to (0 = all; passed to lb/ms simulator)")
    parser.add_argument(
        "--seed", type=int, default=None,
        help="RNG seed for reproducible simulation (passed to lb/ms simulator)",
    )
    parser.add_argument(
        "--mark", type=float, action="append", default=None,
        help="Additional latency threshold(s) to annotate on the CDF "
             "(seconds for lb, ms for ms; e.g. --mark 10 --mark 30)",
    )
    parser.add_argument(
        "--slo", type=float, default=None,
        help="SLO latency threshold in seconds (lb mode only; marked on CDF when set)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_path = output_path_with_comment(resolve_output_path(args), args.comment)
    lb_policy = resolve_lb_policy(args.simulator, args.lb_policy)

    if args.simulator == "lb":
        validate_lb_args(args)
        validate_prequal_subset(lb_policy, args.lb_subset_size)
        binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="lb")
        data = run_simulation(
            binary,
            load=args.load,
            n=args.n,
            service_dist=args.service_dist,
            servers=args.servers,
            concurrency=args.concurrency,
            clients=args.clients,
            lb_policy=lb_policy,
            lb_subset_size=args.lb_subset_size,
            service_modes=args.service_modes,
            service_mode_probs=args.service_mode_probs,
            seed=args.seed,
            slo=args.slo,
        )
        if not data["e2e"]:
            print("no completed tasks", file=sys.stderr)
            sys.exit(1)
        plot_e2e_cdf(
            data,
            output_path,
            load=args.load,
            slo=args.slo,
            marks=args.mark,
        )
        print(
            f"wrote {output_path} (utilization: {data['utilization_pct']:.2f}%)",
            file=sys.stderr,
        )
        return

    validate_ms_args(args)
    if lb_policy not in MS_LB_POLICIES:
        raise SystemExit(
            f"--lb-policy {lb_policy} is not supported with --simulator ms "
            f"(choices: {', '.join(MS_LB_POLICIES)})"
        )
    validate_prequal_subset(lb_policy, args.lb_subset_size)
    binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="ms")
    data = run_ms_simulation(
        binary,
        callgraph=args.callgraph,
        load_file=args.load_file,
        n=args.n,
        lb_policy=lb_policy,
        lb_subset_size=args.lb_subset_size,
        seed=args.seed,
    )
    panels = select_ms_panels(data, api=args.api, load_file=args.load_file)
    for panel in panels:
        if not panel.e2e:
            print(f"no completed requests for {panel.title}", file=sys.stderr)
            sys.exit(1)
    plot_e2e_cdf_panels(
        panels,
        output_path,
        xlabel="E2E latency (ms)",
        marks=args.mark,
    )
    api_names = [panel.title.removeprefix("api = ") for panel in panels]
    util = format_ms_utilization(data["microservice_utilization_pct"])
    print(
        f"wrote {output_path} (apis: {', '.join(api_names)}; utilization: {util})",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
