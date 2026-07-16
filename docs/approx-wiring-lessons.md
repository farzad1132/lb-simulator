# Lessons learned: ms approx pull port wiring

This document records a latent wiring bug exposed when approx moved to **bound intents** and **fatal pull abort**. The goal is to avoid repeating the same mistake when adding multi-target nexosim port fan-out elsewhere.

See also: [approx-policy.md](approx-policy.md) (protocol and counter semantics).

## Summary

In the `ms` simulator, downstream replicas send `ReplicaPull` messages back to upstream `ReplicaBalancer` models via `approx_pull_outputs`. For a period, pulls were wired with a **pre-sized `Vec<Output>`** indexed by `rb_id`. In practice, every slot shared the same underlying connection: a pull intended for `frontend/3` was delivered to `frontend/9` instead.

The bug was invisible under **FIFO unbound** approx (wrong target often returned early on an empty queue). It surfaced under **bound pulls** (`request_id` lookup) and became a hard failure once we replaced silent recovery with `fatal_pull_abort`.

**Fix (current code):** one fresh `Output` per upstream `rb_id` in a `HashMap`, each connected with `&pending.mailbox` — not a cloned `Address`, and not a reused vec slot.

Relevant wiring: [`src/microservice/simulate.rs`](../src/microservice/simulate.rs) (approx pull outputs), [`src/microservice/replica.rs`](../src/microservice/replica.rs) (drain + `pending_pulls`).

## Symptoms

Integration test `ms_approx_completes_on_chain_topology` panicked under bound approx:

```text
FATAL approx pull abort (ms): bound call not found (
  rb_id=9, microservice_id=frontend, server_idx=4,
  target=backend1, request_id=3,
  queue_len=0, queued_request_ids=[]
)
```

Debug logging showed the mismatch clearly:

| Step | Expected | Observed |
|------|----------|----------|
| Outbound enqueue | `frontend/3` enqueues `request_id=3` | Correct |
| Pull intent drain | `intent.sender_id=3` on `backend1/4` | Correct |
| Pull handler | `ReplicaBalancer` for `frontend/3` (`rb_id=3`) | **`frontend/9` (`rb_id=9`)** |
| Queue lookup | Call present on `frontend/3` | Empty on `frontend/9` → panic |

The wiring table printed the *intended* mapping (`approx_pull_outputs[3] → frontend/3`), but runtime delivery did not match.

## Root cause

### Anti-pattern: pre-sized `Vec<Output>`

Broken pattern (do not use):

```rust
let mut approx_pull_outputs = vec![Output::default(); total_rb_count];
for pending in &pending_replica_balancers {
    approx_pull_outputs[pending.rb_id]
        .connect(ReplicaBalancer::pull, rb_address);
}
```

Each `Output::default()` in the vec appeared to be a separate port, and `connect` was called on different indices. At runtime, **`send` on any index delivered to the last connected target** (the highest-index upstream balancer in the connect loop, typically `frontend/9` in chain-3 tests).

Index keys in messages (`intent.sender_id`, `pending.rb_id`) were correct; the **port objects were not independent**.

### Correct pattern: one `Output` per logical target

Current pattern in `simulate.rs`:

```rust
let mut approx_pull_outputs = HashMap::new();
for pending in &pending_replica_balancers {
    let mut output = Output::default();
    output.connect(ReplicaBalancer::pull, &pending.mailbox);
    approx_pull_outputs.insert(pending.rb_id, output);
}
```

Each upstream balancer gets its own `Output` instance. `Replica::drain_pull_intents_async` looks up `approx_pull_outputs.get_mut(&intent.sender_id)` and sends on that dedicated port.

### Prefer `&Mailbox` over cloned `Address`

Replica balancer outbound wiring already used the working pattern:

```rust
outbound.connect(ReplicaBalancer::outbound, &mailbox);
```

Approx pull and `release_outbound` originally used `mailbox.address()` clones stored in a side map, sometimes **before** the target model was registered on the bench. That is fragile compared with connecting through the owning `Mailbox` reference held in `PendingReplicaBalancer`.

**Rule:** when the target mailbox is still available at wiring time, connect with `&mailbox`, not a stored `Address`.

## Why FIFO approx hid the bug

Under unbound FIFO pull:

1. Downstream sent a pull to the **wrong** upstream balancer.
2. The handler did `if queue.is_empty() { return; }` and `pop_front()` — no `request_id` check.
3. If the wrong queue was empty, the pull was **silently dropped**.
4. If the wrong queue had *some* call, FIFO could dispatch an unrelated request (silent misrouting).

Bound intents plus fatal abort turned step 3 into an explicit invariant violation with a detailed panic — which is what we want for simulator bugs.

## Related fix: `pending_pulls` on ms replicas

Separately from port wiring, ms downstream replicas needed the same concurrency gate as `lb` servers:

- Increment `pending_pulls` when popping an intent and sending a pull.
- Decrement when the approx-dispatched hop arrives (`slot_release` set), **before** `begin_service`.
- Drain at most **one** intent per `drain_pull_intents_async` call (`in_flight + pending_pulls < max_concurrency`).

Without this, a `while` drain loop could issue multiple pulls before any hop arrived, bypassing per-replica concurrency (documented in [approx-policy.md](approx-policy.md)).

## Why fatal pull abort was the right call

The old `pull_aborted` path recovered from failed bound lookups and let the simulation continue. That masked:

- Miswired pull ports (this bug).
- Any future 1:1 intent↔queue violation.

`fatal_pull_abort` in [`src/approx.rs`](../src/approx.rs) treats a failed bound lookup as a **simulator invariant violation**, logs structured context (`rb_id`, `request_id`, queue snapshot), and stops immediately. For a discrete-event simulator, that is preferable to silently completing the wrong workload.

## Debugging playbook

If `fatal_pull_abort (ms)` reports `bound call not found` with `queue_len=0`:

1. **Compare sender to handler.** Log `intent.sender_id` at drain time and `self.rb_id` in `ReplicaBalancer::pull`. They must match the same upstream `(microservice, server_idx)`.
2. **Confirm enqueue site.** Trace `ReplicaBalancer::outbound` approx enqueue for that `request_id`; the handler `rb_id` must be the same replica.
3. **Inspect port wiring.** Ensure each upstream `rb_id` has its own `Output` (HashMap entry), not a shared vec slot.
4. **Prefer mailbox connects.** Verify `connect(..., &pending.mailbox)` rather than stale address clones.
5. **Reproduce at low `n`.** Failures often appeared between `n=5` (pass) and `n=6` (fail) once concurrency overlapped.

For `lb`, the same indexing discipline applies to `pull_outputs[lb_id]`, but the client count is small and wiring happens in a simpler loop — the vec pitfall has not manifested there. Prefer the HashMap + fresh `Output` pattern if fan-out grows.

## Checklist for new multi-target nexosim ports

When adding “many outputs indexed by model id” wiring:

- [ ] **Do not** pre-size `vec![Output::default(); n]` and connect multiple indices in a loop unless you have verified each slot is independent (we did not).
- [ ] **Do** create `Output::default()` immediately before each `connect`, or store in a `HashMap<id, Output>`.
- [ ] **Do** connect with `&Mailbox` when the mailbox outlives the wiring phase.
- [ ] **Do** add a smoke test that exercises **multiple upstream balancers** under concurrent load (not just single-client / single-server).
- [ ] **Do** use bound identifiers in tests so misrouting fails loudly, not silently.
- [ ] **Do** log or assert `sender_id` ↔ handler id in debug builds when first integrating a fan-out port.
- [ ] **Do** run [`tests/ms_approx_pull_audit.rs`](../tests/ms_approx_pull_audit.rs) after changing approx pull wiring; it checks intent delivery, queue depth, FIFO pops, and bound pull routing via [`ApproxPullAudit`](../src/approx_audit.rs).

## References

| Topic | Location |
|-------|----------|
| Intent binding invariant | [approx-policy.md § Intent binding invariant](approx-policy.md) |
| No-bind pull fulfillment (`--no-bind`) | [approx-policy.md § No-bind mode](approx-policy.md#no-bind-mode---no-bind) |
| `fatal_pull_abort` | [`src/approx.rs`](../src/approx.rs) |
| Pull drain + `pending_pulls` | [`src/microservice/replica.rs`](../src/microservice/replica.rs) |
| Port wiring | [`src/microservice/simulate.rs`](../src/microservice/simulate.rs) (~966–978) |
| Integration test | [`tests/ms_approx.rs`](../tests/ms_approx.rs) |
| Pull invariant audit test | [`tests/ms_approx_pull_audit.rs`](../tests/ms_approx_pull_audit.rs) |
