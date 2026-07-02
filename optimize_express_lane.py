#!/usr/bin/env python3
"""Grid-search express lane parameters and log human-readable progress."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from itertools import product
from pathlib import Path

from tqdm import tqdm

from express_lane_grid import (
    express_del_th_values,
    express_size_values,
    express_th_values,
    format_run_summary,
)
from plot_cdfs import (
    LB_POLICIES,
    REPO_ROOT,
    _sanitize_comment,
    ensure_release_binary,
    run_simulation,
)
from plot_lb_sweep import METRIC_CHOICES, extract_metric, parse_metric

DEFAULT_BINARY = REPO_ROOT / "target" / "release" / "lb"
DEFAULT_LOG_DIR = REPO_ROOT / "optimizer_logs"

RESULTS_HEADER = "run  express_size  express_del_th  express_th"
RESULTS_SEP = "---  ------------  --------------  ----------"
RESULTS_ROW_RE = re.compile(
    r"^\s*(\d+)\s+(\d+)\s+([\d.]+)\s+(\d+)\s+([\d.]+)\s*(.*)$"
)
HEADER_KV_RE = re.compile(r"^(\w[\w ]*):\s*(.+)$")


@dataclass
class GridPoint:
    express_size: int
    express_del_th: float
    express_th: int


@dataclass
class RunResult:
    run: int
    point: GridPoint
    metric_value: float
    new_optimum: bool = False


@dataclass
class OptimumEvent:
    run: int
    point: GridPoint
    metric_value: float
    previous: float | None


@dataclass
class SearchState:
    started_at: str
    comment: str | None
    metric: str
    objective: str
    base_kwargs: dict
    express_sizes: list[int]
    express_del_ths: list[float]
    express_ths: list[int]
    results: list[RunResult] = field(default_factory=list)
    optimum_history: list[OptimumEvent] = field(default_factory=list)


def objective_for_metric(metric: str) -> str:
    kind, _ = parse_metric(metric)
    return "maximize" if kind == "utilization" else "minimize"


def is_better(metric_value: float, best: float | None, objective: str) -> bool:
    if best is None:
        return True
    if objective == "maximize":
        return metric_value > best
    return metric_value < best


def metric_column_name(metric: str) -> str:
    kind, pct = parse_metric(metric)
    if kind == "utilization":
        return "utilization"
    if kind == "slo-violation":
        return "slo_violation"
    return f"p{int(pct)}"


def log_filename(*, comment: str | None, n: int, started_at: datetime) -> str:
    stamp = started_at.strftime("%Y%m%d_%H%M%S")
    parts: list[str] = [stamp]
    if comment:
        safe = _sanitize_comment(comment)
        if safe:
            parts.append(safe)
    parts.append(f"n{n}")
    return f"express_lane_{'_'.join(parts)}.log"


def format_grid_list(values: list[int] | list[float]) -> str:
    return ", ".join(f"{v:g}" if isinstance(v, float) else str(v) for v in values)


def parse_grid_list(text: str) -> list[float]:
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return []
    if all("." not in p and "e" not in p.lower() for p in parts):
        return [float(int(p)) for p in parts]
    return [float(p) for p in parts]


def current_best(state: SearchState) -> RunResult | None:
    if not state.results:
        return None
    objective = state.objective
    best = state.results[0]
    for result in state.results[1:]:
        if is_better(result.metric_value, best.metric_value, objective):
            best = result
    return best


def note_for_result(result: RunResult, state: SearchState) -> str:
    parts: list[str] = []
    best = current_best(state)
    if best is not None and result.run == best.run:
        parts.append("* best")
    if result.new_optimum:
        parts.append("NEW OPTIMUM")
    return "  ".join(parts)


def format_log(state: SearchState) -> str:
    col = metric_column_name(state.metric)
    total = len(state.express_sizes) * len(state.express_del_ths) * len(state.express_ths)
    completed = len(state.results)
    lines: list[str] = [
        "Express lane grid search",
        "=" * 24,
        f"started: {state.started_at}",
    ]
    if state.comment:
        lines.append(f"comment: {state.comment}")
    lines.extend([
        f"objective: {state.objective}",
        f"metric: {state.metric}",
        "",
        "Simulation:",
        f"  load={state.base_kwargs['load']:g}",
        f"  servers={state.base_kwargs['servers']}",
        f"  clients={state.base_kwargs['clients']}",
        f"  concurrency={state.base_kwargs['concurrency']}",
        f"  lb_policy={state.base_kwargs['lb_policy']}",
        f"  lb_subset_size={state.base_kwargs['lb_subset_size']}",
        f"  n={state.base_kwargs['n']}",
        f"  service_dist={state.base_kwargs['service_dist']}",
        "",
        "Grid:",
        f"  express_size: {format_grid_list(state.express_sizes)}",
        f"  express_del_th: {format_grid_list(state.express_del_ths)}",
        f"  express_th: {format_grid_list(state.express_ths)}",
        f"  progress: {completed} / {total}",
        "",
        "Current optimum",
        "-" * 15,
    ])

    best = current_best(state)
    if best is None:
        lines.append("  (none yet)")
    else:
        p = best.point
        lines.append(
            f"  express_size={p.express_size}  express_del_th={p.express_del_th:g}  "
            f"express_th={p.express_th}  {col}={best.metric_value:.6f}"
        )

    lines.extend(["", "Optimum history", "-" * 15])
    if not state.optimum_history:
        lines.append("  (none yet)")
    else:
        for event in state.optimum_history:
            p = event.point
            prev = (
                f"  (was {event.previous:.6f})"
                if event.previous is not None
                else "  (initial best)"
            )
            lines.append(
                f"run {event.run:3d}: size={p.express_size}  del_th={p.express_del_th:g}  "
                f"th={p.express_th}  {col}={event.metric_value:.6f}  "
                f"** NEW OPTIMUM **{prev}"
            )

    lines.extend([
        "",
        f"Results ({completed}/{total})",
        "-" * 20,
        f"{RESULTS_HEADER}  {col:>9}  note",
        f"{RESULTS_SEP}  {'-' * 9}  ----",
    ])

    for result in state.results:
        p = result.point
        note = note_for_result(result, state)
        lines.append(
            f"{result.run:4d}  {p.express_size:14d}  {p.express_del_th:14g}  "
            f"{p.express_th:10d}  {result.metric_value:9.6f}  {note}"
        )

    lines.append("")
    return "\n".join(lines)


def rewrite_log(path: Path, state: SearchState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_log(state), encoding="utf-8")


def parse_log(text: str) -> SearchState:
    lines = text.splitlines()
    header: dict[str, str] = {}
    in_results = False
    results: list[RunResult] = []
    optimum_history: list[OptimumEvent] = []
    express_sizes: list[int] = []
    express_del_ths: list[float] = []
    express_ths: list[int] = []
    base_kwargs: dict = {}

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("run ") and "express_size" in stripped:
            in_results = True
            continue
        if stripped.startswith("---") and in_results:
            continue
        if in_results:
            match = RESULTS_ROW_RE.match(line)
            if match:
                run = int(match.group(1))
                point = GridPoint(
                    express_size=int(match.group(2)),
                    express_del_th=float(match.group(3)),
                    express_th=int(match.group(4)),
                )
                metric_value = float(match.group(5))
                note = match.group(6).strip()
                results.append(
                    RunResult(
                        run=run,
                        point=point,
                        metric_value=metric_value,
                        new_optimum="NEW OPTIMUM" in note,
                    )
                )
            continue

        if stripped.startswith("run ") and ":" in stripped and "size=" in stripped:
            event_match = re.match(
                r"run\s+(\d+):\s+size=(\d+)\s+del_th=([\d.]+)\s+th=(\d+)\s+"
                r"[\w]+=([\d.]+)\s+\*\* NEW OPTIMUM \*\*(.*)$",
                stripped,
            )
            if event_match:
                previous = None
                tail = event_match.group(6).strip()
                prev_match = re.search(r"\(was ([\d.]+)\)", tail)
                if prev_match:
                    previous = float(prev_match.group(1))
                optimum_history.append(
                    OptimumEvent(
                        run=int(event_match.group(1)),
                        point=GridPoint(
                            express_size=int(event_match.group(2)),
                            express_del_th=float(event_match.group(3)),
                            express_th=int(event_match.group(4)),
                        ),
                        metric_value=float(event_match.group(5)),
                        previous=previous,
                    )
                )
            continue

        kv = HEADER_KV_RE.match(stripped)
        if not kv:
            continue
        key = kv.group(1).strip().replace(" ", "_")
        value = kv.group(2).strip()
        header[key] = value

    started_at = header.get("started", datetime.now().isoformat(sep=" ", timespec="seconds"))
    comment = header.get("comment")
    metric = header.get("metric", "p99")
    objective = header.get("objective", objective_for_metric(metric))

    sim_prefix = "  "
    for raw in lines:
        if not raw.startswith(sim_prefix):
            continue
        inner = raw[len(sim_prefix):]
        if "=" not in inner:
            continue
        key, val = inner.split("=", 1)
        key = key.strip()
        val = val.strip()
        if key == "load":
            base_kwargs["load"] = float(val)
        elif key == "servers":
            base_kwargs["servers"] = int(val)
        elif key == "clients":
            base_kwargs["clients"] = int(val)
        elif key == "concurrency":
            base_kwargs["concurrency"] = int(val)
        elif key == "lb_policy":
            base_kwargs["lb_policy"] = val
        elif key == "lb_subset_size":
            base_kwargs["lb_subset_size"] = int(val)
        elif key == "n":
            base_kwargs["n"] = int(val)
        elif key == "service_dist":
            base_kwargs["service_dist"] = val

    for raw in lines:
        stripped = raw.strip()
        if stripped.startswith("express_size:"):
            express_sizes = [int(v) for v in parse_grid_list(stripped.split(":", 1)[1])]
        elif stripped.startswith("express_del_th:"):
            express_del_ths = parse_grid_list(stripped.split(":", 1)[1])
        elif stripped.startswith("express_th:"):
            express_ths = [int(v) for v in parse_grid_list(stripped.split(":", 1)[1])]

    base_kwargs.setdefault("service_modes", None)
    base_kwargs.setdefault("service_mode_probs", None)
    base_kwargs.setdefault("seed", None)
    base_kwargs.setdefault("slo", None)

    if not express_sizes and results:
        express_sizes = sorted({r.point.express_size for r in results})
    if not express_del_ths and results:
        express_del_ths = sorted({r.point.express_del_th for r in results})
    if not express_ths and results:
        express_ths = sorted({r.point.express_th for r in results})

    return SearchState(
        started_at=started_at,
        comment=comment,
        metric=metric,
        objective=objective,
        base_kwargs=base_kwargs,
        express_sizes=express_sizes,
        express_del_ths=express_del_ths,
        express_ths=express_ths,
        results=results,
        optimum_history=optimum_history,
    )


def add_grid_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--express-size",
        type=int,
        nargs="*",
        default=None,
        help="Express pool size(s); overrides min/max/step",
    )
    parser.add_argument("--express-size-min", type=int, default=0)
    parser.add_argument("--express-size-max", type=int, default=4)
    parser.add_argument("--express-size-step", type=int, default=1)
    parser.add_argument(
        "--express-del-th",
        type=float,
        nargs="*",
        default=None,
        help="Express delay threshold(s); overrides min/max/step",
    )
    parser.add_argument("--express-del-th-min", type=float, default=0)
    parser.add_argument("--express-del-th-max", type=float, default=10)
    parser.add_argument("--express-del-th-step", type=float, default=1)
    parser.add_argument(
        "--express-th",
        type=int,
        nargs="*",
        default=None,
        help="Express queue depth threshold(s); overrides min/max/step",
    )
    parser.add_argument("--express-th-min", type=int, default=0)
    parser.add_argument("--express-th-max", type=int, default=6)
    parser.add_argument("--express-th-step", type=int, default=1)


def add_sim_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--load", type=float, default=0.8)
    parser.add_argument("--servers", type=int, default=10)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--lb-subset-size", type=int, default=0)
    parser.add_argument("--n", type=int, default=1_000_000)
    parser.add_argument(
        "--service-dist",
        choices=["exponential", "constant", "bimodal"],
        default="exponential",
    )
    parser.add_argument(
        "--service-modes",
        type=float,
        nargs=2,
        metavar=("M0", "M1"),
    )
    parser.add_argument(
        "--service-mode-probs",
        type=float,
        nargs=2,
        metavar=("P0", "P1"),
    )
    parser.add_argument("--lb-policy", choices=LB_POLICIES, default="power-of-two")
    parser.add_argument("--slo", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Grid-search express lane parameters; write human-readable progress logs.",
    )
    parser.add_argument(
        "--metric",
        default="p99",
        help=f"Objective metric: {', '.join(METRIC_CHOICES)}, or p{{N}} (default: p99)",
    )
    parser.add_argument("--binary", type=Path, default=None)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument(
        "--comment",
        type=str,
        default=None,
        help="Label included in the log filename",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
        help="Directory for optimizer logs (default: optimizer_logs/)",
    )
    parser.add_argument(
        "--resume",
        type=Path,
        default=None,
        help="Resume from an existing log file",
    )
    add_sim_args(parser)
    add_grid_args(parser)
    return parser.parse_args()


def build_state_from_args(args: argparse.Namespace) -> SearchState:
    parse_metric(args.metric)
    if parse_metric(args.metric)[0] == "slo-violation" and args.slo is None:
        raise SystemExit("--slo is required when --metric slo-violation")

    return SearchState(
        started_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
        comment=args.comment,
        metric=args.metric,
        objective=objective_for_metric(args.metric),
        base_kwargs={
            "load": args.load,
            "n": args.n,
            "service_dist": args.service_dist,
            "servers": args.servers,
            "concurrency": args.concurrency,
            "clients": args.clients,
            "lb_policy": args.lb_policy,
            "lb_subset_size": args.lb_subset_size,
            "service_modes": args.service_modes,
            "service_mode_probs": args.service_mode_probs,
            "seed": args.seed,
            "slo": args.slo,
        },
        express_sizes=express_size_values(args),
        express_del_ths=express_del_th_values(args, drop_invalid=True),
        express_ths=express_th_values(args),
    )


def completed_triples(state: SearchState) -> set[tuple[int, float, int]]:
    return {
        (r.point.express_size, r.point.express_del_th, r.point.express_th)
        for r in state.results
    }


def run_grid_search(
    binary: Path,
    state: SearchState,
    log_path: Path,
) -> None:
    grid = list(product(state.express_sizes, state.express_del_ths, state.express_ths))
    done = completed_triples(state)
    next_run = max((r.run for r in state.results), default=0) + 1
    remaining = [
        GridPoint(express_size=s, express_del_th=d, express_th=t)
        for s, d, t in grid
        if (s, d, t) not in done
    ]

    col = metric_column_name(state.metric)
    for point in tqdm(remaining, desc="express lane grid search", unit="run"):
        sim_kwargs = {
            **state.base_kwargs,
            "expresslane": True,
            "express_size": point.express_size,
            "express_del_th": point.express_del_th,
            "express_th": point.express_th,
        }
        data = run_simulation(binary, **sim_kwargs)
        if not data["e2e"]:
            raise SystemExit("simulator returned no completed tasks")

        metric_value = extract_metric(data, state.metric, slo=state.base_kwargs.get("slo"))
        summary = format_run_summary(
            sim_kwargs=sim_kwargs,
            metric_name=state.metric,
            metric_value=metric_value,
            data=data,
        )
        tqdm.write(summary)

        best = current_best(state)
        previous_best = best.metric_value if best is not None else None
        new_optimum = is_better(metric_value, previous_best, state.objective)

        result = RunResult(
            run=next_run,
            point=point,
            metric_value=metric_value,
            new_optimum=new_optimum,
        )
        state.results.append(result)

        if new_optimum:
            state.optimum_history.append(
                OptimumEvent(
                    run=next_run,
                    point=point,
                    metric_value=metric_value,
                    previous=previous_best,
                )
            )
            prev_text = (
                f" (was {previous_best:.6f})"
                if previous_best is not None
                else " (initial best)"
            )
            tqdm.write(
                f"** NEW OPTIMUM ** run {next_run}: size={point.express_size}  "
                f"del_th={point.express_del_th:g}  th={point.express_th}  "
                f"{col}={metric_value:.6f}{prev_text}"
            )

        rewrite_log(log_path, state)
        next_run += 1

    best = current_best(state)
    if best is None:
        print("no results", file=sys.stderr)
        return
    p = best.point
    print(
        f"best: express_size={p.express_size} express_del_th={p.express_del_th:g} "
        f"express_th={p.express_th} {col}={best.metric_value:.6f}",
        file=sys.stderr,
    )
    print(f"log: {log_path}", file=sys.stderr)


def main() -> None:
    args = parse_args()

    if args.no_build:
        binary = args.binary or DEFAULT_BINARY
    else:
        binary = ensure_release_binary(REPO_ROOT, args.binary, simulator="lb")
    if not binary.is_file():
        raise SystemExit(f"lb binary not found: {binary}")

    if args.resume:
        if not args.resume.is_file():
            raise SystemExit(f"resume log not found: {args.resume}")
        state = parse_log(args.resume.read_text(encoding="utf-8"))
        log_path = args.resume
        print(f"resuming {log_path} ({len(state.results)} completed)", file=sys.stderr)
    else:
        state = build_state_from_args(args)
        started = datetime.now()
        log_path = args.log_dir / log_filename(
            comment=args.comment,
            n=args.n,
            started_at=started,
        )
        rewrite_log(log_path, state)
        print(f"logging to {log_path}", file=sys.stderr)

    run_grid_search(binary, state, log_path)


if __name__ == "__main__":
    main()
