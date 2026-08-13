# JBSQ policy (bounded central pull)

This document describes the **jbsq** load-balancing policy for the `ms` simulator: a centralized pull queue where each replica keeps up to `--jbsq-n` pulled requests locally.

See also:

- [microservice-simulation.md](microservice-simulation.md) — general `ms` simulator
- [microservice-simulation.md § Centralized](microservice-simulation.md#centralized-policy-pull-based-layer) — pull-on-idle baseline
- [scheduling.md](scheduling.md#centralized-pull-queue-scheduling---centralized-sched) — shared pull-queue FCFS/EDF
- [lb-vs-ms.md](lb-vs-ms.md) — feature comparison

## Overview

**JBSQ** reuses the centralized shared-outbound topology (`DownstreamBalancer` + `OutboundGateway` per downstream target). The only behavioral difference is the pull threshold:

| | Centralized | JBSQ |
|--|--|--|
| Pull when | `in_flight < max_concurrency` | upstream occupancy + `pending_pulls < n` |
| Pulled work at replica | Start immediately (no local queue) | Enqueue on the replica queue, then drain |
| Warm start | `concurrency` pulls/replica | `n` pulls/replica |
| Param | — | `--jbsq-n` **required**, no default, `>= 1` |

Upstream occupancy counts `Upstream` items in the local replica queue plus `in_flight` (DownstreamReturn work is excluded from the pull gate).

`n` may exceed per-replica concurrency (`cpu / replicas`); excess pulled requests sit in the local queue and are subject to `--scheduling`. Effective in-service concurrency remains `max_concurrency`.

When every replica for a target has `occupancy + pending_pulls >= n`, further outbound calls queue in the shared `DownstreamBalancer` (same as centralized under backlog). `--centralized-sched` orders **only** that shared pull queue; it does **not** change replica local queues (those use `--scheduling`). Under pull-ahead (`n > 1`), the LB queue may backlog less often than centralized, so EDF can have little SLO impact even when dispatch order is correct.

Ingress stays push power-of-two on `EdgeBalancer`. `lb --lb-policy jbsq` is rejected (`jbsq` is ms-only).

## CLI

```bash
./target/release/ms \
  --callgraph tests/chain/3/callgraph.json \
  --load-file tests/chain/3/load.json \
  --lb-policy jbsq \
  --jbsq-n 2 \
  --n 10000
```

| Flag | Default | Description |
|------|---------|-------------|
| `--lb-policy jbsq` | — | Enable jbsq |
| `--jbsq-n` | (none) | Max pulled upstream occupancy per replica; required with jbsq |
| `--centralized-sched` | `fcfs` | Shared `DownstreamBalancer` pull-queue discipline (`fcfs` or `edf`); LB queue only — not replica queues |
| `--lb-subset-size` | `0` | Optional partition subsets; same constraints as centralized |

## Architecture

```
User → EdgeBalancer(handle) → frontend/*
                                │
frontend/* ──▶ OutboundGateway ──▶ DownstreamBalancer(backend1) ──pull──▶ backend1/*
backend1/* ──▶ OutboundGateway ──▶ DownstreamBalancer(backend2) ──pull──▶ backend2/*
```

Replicas pull while `jbsq_occupancy + pending_pulls < n`, track outstanding pulls in `pending_pulls`, enqueue arrivals with `slot_release`, and re-pull after local completion until full again.

## Tests

| Test | Role |
|------|------|
| [`tests/ms_jbsq.rs`](../tests/ms_jbsq.rs) | CLI smoke, `--jbsq-n` validation, lb reject, subset smoke |
| [`tests/ms_jbsq_audit.rs`](../tests/ms_jbsq_audit.rs) | Occupancy-bound audit (`n=1`, `n=2`) |
| [`tests/ms_jbsq_sched_audit.rs`](../tests/ms_jbsq_sched_audit.rs) | Trace-based `--centralized-sched` FCFS/EDF on shared pull queue |
| [`src/ms_jbsq_audit.rs`](../src/ms_jbsq_audit.rs) | Occupancy event recorder + validators |

## Source files

| File | Role |
|------|------|
| [`src/policy.rs`](../src/policy.rs) | `LoadBalancePolicyKind::Jbsq`, `validate_jbsq_n`, `uses_central_pull_queue` |
| [`src/microservice/replica.rs`](../src/microservice/replica.rs) | Pull gate, local enqueue, warm/re-pull |
| [`src/microservice/balancer.rs`](../src/microservice/balancer.rs) | Shared pull queue (same path as centralized) |
| [`src/microservice/simulate.rs`](../src/microservice/simulate.rs) | Wiring, initial `n` pulls, `MsArgs.jbsq_n` |
