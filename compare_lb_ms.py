#!/usr/bin/env python3
"""Compare lb and ms simulators on equivalent client-server topologies."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from plotting_primitive import ACM_COMPACT_HALF, SubplotGrid, percentile, plot_cdf

from plot_cdfs import (
    _parse_lb_json,
    _parse_ms_json,
    ensure_release_binary,
    run_subprocess,
)

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_LB_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_MS_BINARY = REPO_ROOT / "target" / "release" / "ms"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output"

SERVICE_MEAN_SECS = 1.0
SERVICE_MEAN_MS = 1000.0
API_NAME = "handle"
SERVICE_NAME = "server"
UTIL_TOLERANCE_PCT = 1.0
LATENCY_TOLERANCES: dict[float, float] = {
    50.0: 0.03,
    90.0: 0.05,
    99.0: 0.06,
}
LATENCY_PERCENTILES = (50.0, 90.0, 99.0)
LB_POLICIES = ("random", "power-of-two", "least-request", "round-robin")


@dataclass(frozen=True)
class Scenario:
    name: str
    cpu: int
    replicas: int
    fixture_dir: Path

    @property
    def servers(self) -> int:
        return self.replicas

    @property
    def concurrency(self) -> int:
        return max(self.cpu // self.replicas, 1)

    @property
    def callgraph(self) -> Path:
        return self.fixture_dir / "callgraph.json"

    @property
    def load_file(self) -> Path:
        return self.fixture_dir / "load.json"


SCENARIOS: dict[str, Scenario] = {
    "single": Scenario(
        name="single_replica",
        cpu=4,
        replicas=1,
        fixture_dir=REPO_ROOT / "tests" / "client_server" / "single_replica",
    ),
    "multi": Scenario(
        name="multi_replica",
        cpu=4,
        replicas=4,
        fixture_dir=REPO_ROOT / "tests" / "client_server" / "multi_replica",
    ),
}


def rps_from_load(load: float, cpu: int, service_mean_secs: float = SERVICE_MEAN_SECS) -> float:
    return load * cpu / service_mean_secs


def run_lb(
    binary: Path,
    *,
    n: int,
    load: float,
    scenario: Scenario,
    lb_policy: str,
) -> dict:
    cmd = [
        str(binary),
        "--format",
        "json",
        "--n",
        str(n),
        "--load",
        str(load),
        "--servers",
        str(scenario.servers),
        "--concurrency",
        str(scenario.concurrency),
        "--clients",
        "1",
        "--lb-policy",
        lb_policy,
    ]
    result = run_subprocess(cmd, label="lb simulator")
    return _parse_lb_json(cmd, result.stdout)


def run_ms(
    binary: Path,
    *,
    n: int,
    scenario: Scenario,
    lb_policy: str,
) -> dict:
    cmd = [
        str(binary),
        "--format",
        "json",
        "--n",
        str(n),
        "--callgraph",
        str(scenario.callgraph),
        "--load-file",
        str(scenario.load_file),
        "--lb-policy",
        lb_policy,
    ]
    result = run_subprocess(cmd, label="ms simulator")
    return _parse_ms_json(cmd, result.stdout)


def ms_e2e_seconds(ms_data: dict) -> list[float]:
    e2e_ms = ms_data["by_api"][API_NAME]["e2e_ms"]
    return [value / 1000.0 for value in e2e_ms]


@dataclass
class ComparisonResult:
    scenario: str
    passed: bool
    failures: list[str]
    lb_utilization_pct: float
    ms_utilization_pct: float
    latency_rows: list[tuple[float, float, float, float, bool]]


def compare_scenario(
    scenario: Scenario,
    *,
    lb_data: dict,
    ms_data: dict,
) -> ComparisonResult:
    failures: list[str] = []

    lb_util = float(lb_data["utilization_pct"])
    ms_util = float(ms_data["microservice_utilization_pct"][SERVICE_NAME])
    util_diff = abs(lb_util - ms_util)
    if util_diff > UTIL_TOLERANCE_PCT:
        failures.append(
            f"utilization diff {util_diff:.4f}pp exceeds {UTIL_TOLERANCE_PCT}pp "
            f"(lb={lb_util:.4f}%, ms={ms_util:.4f}%)"
        )

    lb_e2e = lb_data["e2e"]
    ms_e2e = ms_e2e_seconds(ms_data)

    latency_rows: list[tuple[float, float, float, float, bool]] = []
    for pct in LATENCY_PERCENTILES:
        lb_val = percentile(lb_e2e, pct)
        ms_val = percentile(ms_e2e, pct)
        tolerance = LATENCY_TOLERANCES[pct]
        if lb_val == 0.0:
            rel_diff = abs(ms_val - lb_val)
            ok = rel_diff <= tolerance
        else:
            rel_diff = abs(ms_val - lb_val) / lb_val
            ok = rel_diff <= tolerance
        latency_rows.append((pct, lb_val, ms_val, rel_diff, ok))
        if not ok:
            failures.append(
                f"p{pct:g} e2e rel diff {rel_diff:.4f} exceeds {tolerance:.2%} "
                f"(lb={lb_val:.6f}s, ms={ms_val:.6f}s)"
            )

    return ComparisonResult(
        scenario=scenario.name,
        passed=len(failures) == 0,
        failures=failures,
        lb_utilization_pct=lb_util,
        ms_utilization_pct=ms_util,
        latency_rows=latency_rows,
    )


def print_result(result: ComparisonResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    print(f"\n=== {result.scenario} [{status}] ===")
    print(
        f"  utilization: lb={result.lb_utilization_pct:.4f}%  "
        f"ms={result.ms_utilization_pct:.4f}%  "
        f"diff={abs(result.lb_utilization_pct - result.ms_utilization_pct):.4f}pp"
    )
    print("  e2e latency (seconds):")
    for pct, lb_val, ms_val, rel_diff, ok in result.latency_rows:
        mark = "ok" if ok else "FAIL"
        print(
            f"    p{pct:g}: lb={lb_val:.6f}  ms={ms_val:.6f}  "
            f"rel_diff={rel_diff:.4f}  [{mark}]"
        )
    for failure in result.failures:
        print(f"  - {failure}", file=sys.stderr)


def plot_comparison(
    scenario: Scenario,
    *,
    lb_e2e: list[float],
    ms_e2e: list[float],
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"lb_ms_compare_{scenario.name}.pdf"

    style = ACM_COMPACT_HALF
    grid = SubplotGrid(style, layout="1x1")
    ax = grid.get_ax(0, 0)
    plot_cdf(ax, lb_e2e, label="lb", style=style, color_idx=0, log_x=True)
    plot_cdf(ax, ms_e2e, label="ms", style=style, color_idx=1, log_x=True)
    grid.configure_ax(
        ax,
        xlabel="latency (s)",
        ylabel="CDF",
        title=f"e2e latency ({scenario.name})",
        show_xlabel=True,
        show_ylabel=True,
        show_xticklabels=True,
        show_yticklabels=True,
    )
    grid.save(output_path)
    print(f"  wrote CDF plot to {output_path}")
    return output_path


def resolve_scenarios(name: str) -> list[Scenario]:
    if name == "all":
        return [SCENARIOS["single"], SCENARIOS["multi"]]
    if name not in SCENARIOS:
        raise SystemExit(f"unknown scenario {name!r}; choose from single, multi, all")
    return [SCENARIOS[name]]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare lb and ms on equivalent client-server topologies.",
    )
    parser.add_argument(
        "--scenario",
        choices=("single", "multi", "all"),
        default="all",
        help="Which fixture scenario to run (default: all)",
    )
    parser.add_argument("--n", type=int, default=200_000, help="Total requests per run")
    parser.add_argument(
        "--load",
        type=float,
        default=0.8,
        help="Target utilization for lb (ms load.json rps must match)",
    )
    parser.add_argument(
        "--lb-policy",
        choices=LB_POLICIES,
        default="power-of-two",
        help="Load-balancing policy for both simulators",
    )
    parser.add_argument(
        "--lb-binary",
        type=Path,
        default=None,
        help="Path to lb binary (default: build target/release/lb)",
    )
    parser.add_argument(
        "--ms-binary",
        type=Path,
        default=None,
        help="Path to ms binary (default: build target/release/ms)",
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Do not run cargo build --release",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Write overlay CDF plots to output/",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for optional CDF plots",
    )
    args = parser.parse_args()

    if args.lb_policy not in LB_POLICIES:
        raise SystemExit(f"invalid lb policy: {args.lb_policy}")

    lb_binary = args.lb_binary
    ms_binary = args.ms_binary
    if not args.no_build:
        if lb_binary is None:
            lb_binary = ensure_release_binary(REPO_ROOT, None, simulator="lb")
        if ms_binary is None:
            ms_binary = ensure_release_binary(REPO_ROOT, None, simulator="ms")
    else:
        lb_binary = lb_binary or DEFAULT_LB_BINARY
        ms_binary = ms_binary or DEFAULT_MS_BINARY

    if not lb_binary.is_file():
        raise SystemExit(f"lb binary not found: {lb_binary}")
    if not ms_binary.is_file():
        raise SystemExit(f"ms binary not found: {ms_binary}")

    scenarios = resolve_scenarios(args.scenario)
    all_passed = True

    for scenario in scenarios:
        expected_rps = rps_from_load(args.load, scenario.cpu)
        print(
            f"\nRunning {scenario.name}: cpu={scenario.cpu}, replicas={scenario.replicas}, "
            f"lb servers={scenario.servers} concurrency={scenario.concurrency}, "
            f"expected rps={expected_rps:.4f}"
        )

        lb_data = run_lb(
            lb_binary,
            n=args.n,
            load=args.load,
            scenario=scenario,
            lb_policy=args.lb_policy,
        )
        ms_data = run_ms(
            ms_binary,
            n=args.n,
            scenario=scenario,
            lb_policy=args.lb_policy,
        )

        result = compare_scenario(scenario, lb_data=lb_data, ms_data=ms_data)
        print_result(result)
        if not result.passed:
            all_passed = False

        if args.plot:
            plot_comparison(
                scenario,
                lb_e2e=lb_data["e2e"],
                ms_e2e=ms_e2e_seconds(ms_data),
                output_dir=args.output_dir,
            )

    if all_passed:
        print("\nAll scenarios passed.")
        return 0

    print("\nOne or more scenarios failed.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
