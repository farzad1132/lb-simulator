"""Shared experiment config types for lb load-compare plotting scripts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ExperimentConfig:
    label: str
    lb_policy: str
    clients: int
    servers: int
    concurrency: int = 1
    lb_subset_size: int = 0  # 0 = full pool; ignored by centralized policy
    pull_policy: str | None = None  # required when lb_policy == "approx"
    expresslane: bool = False
    express_size: int | None = None
    express_del_th: float | None = None
    express_th: int | None = None
    ideal: bool = False


def uses_pull_policy(config: ExperimentConfig) -> bool:
    return config.lb_policy == "approx"


def uses_express_lane(config: ExperimentConfig) -> bool:
    return config.expresslane or any(
        (
            config.express_size is not None,
            config.express_del_th is not None,
            config.express_th is not None,
            config.ideal,
        )
    )


def validate_config(config: ExperimentConfig) -> None:
    label = config.label
    if uses_pull_policy(config):
        if config.pull_policy is None:
            raise SystemExit(
                f"config {label!r}: pull_policy is required when lb_policy is approx"
            )
    elif config.pull_policy is not None:
        raise SystemExit(
            f"config {label!r}: pull_policy is only valid when lb_policy is approx"
        )
    if not uses_express_lane(config):
        return
    if config.lb_policy in ("centralized", "approx"):
        raise SystemExit(
            f"config {label!r}: expresslane is incompatible with {config.lb_policy} policy"
        )
    if config.express_size is None:
        raise SystemExit(f"config {label!r}: express_size is required for express lane")
    max_size = config.servers - 1
    if max_size < 1:
        raise SystemExit(
            f"config {label!r}: servers must be >= 2 for express lane (got {config.servers})"
        )
    if not 1 <= config.express_size <= max_size:
        raise SystemExit(
            f"config {label!r}: express_size must be in [1, {max_size}] "
            f"(got {config.express_size})"
        )
    if config.ideal:
        if config.express_del_th is None:
            raise SystemExit(
                f"config {label!r}: express_del_th is required when ideal=True"
            )
        if config.express_th is not None:
            raise SystemExit(
                f"config {label!r}: ideal is not compatible with express_th"
            )
    elif config.express_del_th is None and config.express_th is None:
        raise SystemExit(
            f"config {label!r}: at least one of express_del_th or express_th is required "
            "for express lane"
        )


def select_configs(
    configs: list[ExperimentConfig],
    config_index: list[int] | None,
) -> list[ExperimentConfig]:
    if config_index is None:
        selected = list(configs)
    else:
        selected = []
        for idx in config_index:
            if idx < 0 or idx >= len(configs):
                raise SystemExit(
                    f"--config-index {idx} out of range (0 .. {len(configs) - 1})"
                )
            selected.append(configs[idx])
    for config in selected:
        validate_config(config)
    return selected
