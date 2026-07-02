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


def select_configs(
    configs: list[ExperimentConfig],
    config_index: list[int] | None,
) -> list[ExperimentConfig]:
    if config_index is None:
        return list(configs)
    selected: list[ExperimentConfig] = []
    for idx in config_index:
        if idx < 0 or idx >= len(configs):
            raise SystemExit(
                f"--config-index {idx} out of range (0 .. {len(configs) - 1})"
            )
        selected.append(configs[idx])
    return selected
