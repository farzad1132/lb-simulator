"""Shared express-lane parameter grid helpers for sweep and optimizer scripts."""

from __future__ import annotations

import argparse
import sys

from plot_lb_sweep import parse_metric, range_values


def express_size_values(args: argparse.Namespace) -> list[int]:
    if args.express_size is not None:
        values = list(args.express_size)
    else:
        values = range_values(
            args.express_size_min,
            args.express_size_max,
            args.express_size_step,
            value_type=int,
            step_flag="--express-size-step",
        )
    max_size = args.servers - 1
    if max_size < 1:
        raise SystemExit(f"--servers must be >= 2 for express lane (got {args.servers})")
    filtered = [v for v in values if 1 <= v <= max_size]
    if not filtered:
        raise SystemExit(
            f"no valid express-size values in [1, {max_size}]; "
            f"check --express-size or --express-size-min/max/step"
        )
    if len(filtered) < len(values):
        dropped = [v for v in values if v not in filtered]
        print(
            f"dropping invalid express-size values (must be in [1, {max_size}]): {dropped}",
            file=sys.stderr,
        )
    return filtered


def express_del_th_values(
    args: argparse.Namespace,
    *,
    drop_invalid: bool = False,
) -> list[float]:
    if args.express_del_th is not None:
        values = [float(v) for v in args.express_del_th]
    else:
        values = range_values(
            args.express_del_th_min,
            args.express_del_th_max,
            args.express_del_th_step,
            value_type=float,
            step_flag="--express-del-th-step",
        )
    if not values:
        raise SystemExit("no express-del-th values in sweep range")
    if drop_invalid:
        filtered = [v for v in values if v > 0]
        if not filtered:
            raise SystemExit("no positive express-del-th values in sweep range")
        if len(filtered) < len(values):
            dropped = [v for v in values if v not in filtered]
            print(
                f"dropping invalid express-del-th values (must be positive): {dropped}",
                file=sys.stderr,
            )
        return filtered
    for value in values:
        if value <= 0:
            raise SystemExit(f"--express-del-th values must be positive (got {value:g})")
    return values


def express_th_values(args: argparse.Namespace) -> list[int]:
    if args.express_th is not None:
        values = list(args.express_th)
    else:
        values = range_values(
            args.express_th_min,
            args.express_th_max,
            args.express_th_step,
            value_type=int,
            step_flag="--express-th-step",
        )
    if not values:
        raise SystemExit("no express-th values in sweep range")
    for value in values:
        if value < 0:
            raise SystemExit(f"--express-th values must be non-negative (got {value})")
    return values


def format_run_summary(
    *,
    sim_kwargs: dict,
    metric_name: str,
    metric_value: float,
    data: dict,
) -> str:
    parts = [
        f"policy={sim_kwargs['lb_policy']}",
        f"load={sim_kwargs['load']:g}",
        f"servers={sim_kwargs['servers']}",
        f"express_size={sim_kwargs['express_size']}",
        f"express_del_th={sim_kwargs['express_del_th']:g}",
    ]
    if sim_kwargs.get("express_th") is not None:
        parts.append(f"express_th={sim_kwargs['express_th']}")
    if sim_kwargs.get("ideal"):
        parts.append("ideal")
    kind, pct = parse_metric(metric_name)
    if kind == "utilization":
        parts.append(f"utilization={metric_value:.1f}%")
    elif kind == "slo-violation":
        parts.append(f"P(latency>SLO)={metric_value:.6f}")
    else:
        parts.append(f"p{int(pct)}={metric_value:.6f}s")
    parts.append(f"utilization={data['utilization_pct']:.1f}%")
    return "  ".join(parts)
