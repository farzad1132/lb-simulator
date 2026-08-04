//! Shared approx sidecars: grouping helpers and dual-mode server-side actor.
//!
//! - **Push (ingress):** forward to an owned replica's local queue (no sidecar wait
//!   queue). `share=1` is identity; `share>1` joins the least-occupancy replica
//!   (`queue.len() + in_flight`).
//! - **Pull (approx-share):** shared intent queue; capacity-gated `ReplicaPull` replies.

use super::balancer::ReplicaPull;
use super::hop::ReplicaInput;
use crate::approx::PullIntent;
use crate::approx_audit::ApproxPullAudit;
use crate::policy::ApproxSchedKind;
use crate::scheduling::edf_insert_index;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Number of sidecars for `replicas` with `share` replicas per sidecar.
pub fn n_sidecars(replicas: usize, share: u32) -> usize {
    let share = share.max(1) as usize;
    if replicas == 0 {
        return 0;
    }
    replicas.div_ceil(share)
}

/// Sidecar id owning `replica_idx` when each sidecar covers `share` replicas.
pub fn sidecar_id(replica_idx: usize, share: u32) -> usize {
    let share = share.max(1) as usize;
    replica_idx / share
}

/// Replica indices owned by `sidecar_idx`.
pub fn sidecar_replicas(sidecar_idx: usize, replicas: usize, share: u32) -> Vec<usize> {
    let share = share.max(1) as usize;
    let start = sidecar_idx * share;
    if start >= replicas {
        return Vec::new();
    }
    let end = (start + share).min(replicas);
    (start..end).collect()
}

/// Capacity / lifecycle update from a replica to its server sidecar.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum SidecarCapacityEvent {
    /// Reserved pull slot arrived at the replica; clear pending reservation.
    PullArrived { server_idx: usize },
    /// Local service completed (pull path); try draining more intents.
    Completed { server_idx: usize },
    /// Replica reports `queue.len() + in_flight` for least-occupancy join.
    Occupancy { server_idx: usize, level: u32 },
}

/// Shared server-side sidecar: ingress fan-out + pull approx-share intent queue.
#[derive(Deserialize, Serialize)]
pub struct ApproxServerSidecar {
    #[serde(skip)]
    microservice_id: String,
    sidecar_id: usize,
    #[serde(skip)]
    replica_indices: Vec<usize>,
    /// Last known `queue.len() + in_flight` per owned replica (plus optimistic ingress).
    #[serde(skip)]
    occupancy: HashMap<usize, u32>,
    #[serde(skip)]
    pending_pulls: HashMap<usize, u32>,
    #[serde(skip)]
    max_concurrency: HashMap<usize, u32>,
    #[serde(skip)]
    pull_intent_queue: VecDeque<PullIntent>,
    /// Ingress dispatch to owned replicas.
    pub upstream_outputs: HashMap<usize, Output<ReplicaInput>>,
    pub approx_pull_outputs: HashMap<usize, Output<ReplicaPull>>,
    #[serde(skip)]
    pull_audit: Option<Arc<ApproxPullAudit>>,
    #[serde(skip)]
    approx_sched: Option<ApproxSchedKind>,
}

impl ApproxServerSidecar {
    pub fn new(
        microservice_id: String,
        sidecar_id: usize,
        replica_indices: Vec<usize>,
        max_concurrency: u32,
        pull_audit: Option<Arc<ApproxPullAudit>>,
        approx_sched: Option<ApproxSchedKind>,
    ) -> Self {
        let mut occupancy = HashMap::new();
        let mut pending_pulls = HashMap::new();
        let mut max_conc = HashMap::new();
        for &idx in &replica_indices {
            occupancy.insert(idx, 0);
            pending_pulls.insert(idx, 0);
            max_conc.insert(idx, max_concurrency.max(1));
        }
        Self {
            microservice_id,
            sidecar_id,
            replica_indices,
            occupancy,
            pending_pulls,
            max_concurrency: max_conc,
            pull_intent_queue: VecDeque::new(),
            upstream_outputs: HashMap::new(),
            approx_pull_outputs: HashMap::new(),
            pull_audit,
            approx_sched,
        }
    }

    fn has_capacity(&self, server_idx: usize) -> bool {
        let occ = self.occupancy.get(&server_idx).copied().unwrap_or(0);
        let pending = self.pending_pulls.get(&server_idx).copied().unwrap_or(0);
        let max_c = self.max_concurrency.get(&server_idx).copied().unwrap_or(1);
        occ + pending < max_c
    }

    /// Least `queue+in_flight`; ties break to lowest replica index.
    fn select_replica(&self) -> usize {
        let mut best = self.replica_indices[0];
        let mut best_occ = self.occupancy.get(&best).copied().unwrap_or(0);
        for &rid in &self.replica_indices[1..] {
            let occ = self.occupancy.get(&rid).copied().unwrap_or(0);
            if occ < best_occ {
                best = rid;
                best_occ = occ;
            }
        }
        best
    }

    /// Pull mode (approx-share): pop one intent and reply with ReplicaPull.
    async fn drain_intent_for_replica(&mut self, server_idx: usize) {
        if !self.has_capacity(server_idx) {
            return;
        }
        let queue_len_before = self.pull_intent_queue.len();
        let pending_pulls_before = self.pending_pulls.get(&server_idx).copied().unwrap_or(0);
        let in_flight_before = self.occupancy.get(&server_idx).copied().unwrap_or(0);
        let max_concurrency = self.max_concurrency.get(&server_idx).copied().unwrap_or(1);
        let Some(intent) = self.pull_intent_queue.pop_front() else {
            return;
        };
        if let Some(audit) = &self.pull_audit {
            // downstream_server is sidecar id so queue-depth keys match IntentQueued.
            audit.record_intent_drained(
                &self.microservice_id,
                self.sidecar_id,
                intent.sender_id,
                intent.request_id,
                queue_len_before,
                pending_pulls_before,
                in_flight_before,
                max_concurrency,
            );
        }
        *self.pending_pulls.entry(server_idx).or_insert(0) += 1;
        if let Some(output) = self.approx_pull_outputs.get_mut(&intent.sender_id) {
            output
                .send(ReplicaPull {
                    target_microservice: self.microservice_id.clone(),
                    server_idx,
                    intent_target_idx: self.sidecar_id,
                    request_id: intent.request_id,
                })
                .await;
        } else {
            *self.pending_pulls.entry(server_idx).or_insert(0) = self
                .pending_pulls
                .get(&server_idx)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1);
        }
    }

    async fn drain_capacity_for_replica(&mut self, server_idx: usize) {
        while self.has_capacity(server_idx) && !self.pull_intent_queue.is_empty() {
            let before = self.pull_intent_queue.len();
            self.drain_intent_for_replica(server_idx).await;
            if self.pull_intent_queue.len() == before {
                break;
            }
        }
    }

    async fn drain_idle_replicas(&mut self) {
        let replicas = self.replica_indices.clone();
        for server_idx in replicas {
            self.drain_capacity_for_replica(server_idx).await;
        }
    }
}

#[Model]
impl ApproxServerSidecar {
    /// Ingress: enqueue on the least-loaded owned replica (identity when share=1).
    pub async fn receive_upstream(&mut self, msg: ReplicaInput, _cx: &Context<Self>) {
        let ReplicaInput::Upstream(hop) = msg else {
            return;
        };
        if self.replica_indices.is_empty() {
            return;
        }
        let server_idx = self.select_replica();
        *self.occupancy.entry(server_idx).or_insert(0) += 1;
        if let Some(output) = self.upstream_outputs.get_mut(&server_idx) {
            output.send(ReplicaInput::Upstream(hop)).await;
        } else {
            *self.occupancy.entry(server_idx).or_insert(0) =
                self.occupancy.get(&server_idx).copied().unwrap_or(1).saturating_sub(1);
        }
    }

    pub async fn receive_pull_intent(&mut self, intent: PullIntent, _cx: &Context<Self>) {
        let queue_len_before = self.pull_intent_queue.len();
        if let Some(audit) = &self.pull_audit {
            // Record sidecar id in downstream_server for topology audits; drain events use replica.
            audit.record_intent_queued(
                &self.microservice_id,
                self.sidecar_id,
                intent.sender_id,
                intent.request_id,
                intent.deadline,
                queue_len_before,
            );
        }
        if self
            .approx_sched
            .is_some_and(|s| s.intent_queue_uses_edf())
        {
            let insert_at = edf_insert_index(
                self.pull_intent_queue.iter().map(|i| i.deadline),
                intent.deadline,
            );
            self.pull_intent_queue.insert(insert_at, intent);
        } else {
            self.pull_intent_queue.push_back(intent);
        }
        self.drain_idle_replicas().await;
    }

    pub async fn capacity_event(&mut self, event: SidecarCapacityEvent, _cx: &Context<Self>) {
        match event {
            SidecarCapacityEvent::PullArrived { server_idx } => {
                if let Some(p) = self.pending_pulls.get_mut(&server_idx) {
                    *p = p.saturating_sub(1);
                }
            }
            SidecarCapacityEvent::Completed { server_idx } => {
                self.drain_capacity_for_replica(server_idx).await;
            }
            SidecarCapacityEvent::Occupancy { server_idx, level } => {
                self.occupancy.insert(server_idx, level);
                self.drain_capacity_for_replica(server_idx).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grouping_ceil_remainder() {
        // 10 replicas, share 3 → sidecars for [0,1,2], [3,4,5], [6,7,8], [9]
        assert_eq!(n_sidecars(10, 3), 4);
        assert_eq!(sidecar_id(0, 3), 0);
        assert_eq!(sidecar_id(2, 3), 0);
        assert_eq!(sidecar_id(3, 3), 1);
        assert_eq!(sidecar_id(9, 3), 3);
        assert_eq!(sidecar_replicas(0, 10, 3), vec![0, 1, 2]);
        assert_eq!(sidecar_replicas(3, 10, 3), vec![9]);
    }

    #[test]
    fn share_one_is_identity() {
        assert_eq!(n_sidecars(5, 1), 5);
        for i in 0..5 {
            assert_eq!(sidecar_id(i, 1), i);
            assert_eq!(sidecar_replicas(i, 5, 1), vec![i]);
        }
    }

    #[test]
    fn select_replica_least_occupancy() {
        let mut sc = ApproxServerSidecar::new("ms".into(), 0, vec![0, 1, 2], 1, None, None);
        sc.occupancy.insert(0, 2);
        sc.occupancy.insert(1, 0);
        sc.occupancy.insert(2, 1);
        assert_eq!(sc.select_replica(), 1);
        sc.occupancy.insert(1, 3);
        // tie 0 and 2 at level 2 → lowest index
        sc.occupancy.insert(0, 2);
        sc.occupancy.insert(2, 2);
        assert_eq!(sc.select_replica(), 0);
    }
}
