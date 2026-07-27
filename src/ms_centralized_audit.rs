use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::subset::{self, SubsetPolicyKind};

/// Records MS centralized enqueue/dispatch events for post-hoc subset invariant checks.
#[derive(Default)]
pub struct MsCentralizedAudit {
    next_seq: AtomicU64,
    events: Mutex<Vec<RecordedEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    seq: u64,
    kind: CentralizedEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CentralizedEventKind {
    CallEnqueued {
        target: String,
        lb_id: usize,
        caller_server: usize,
        request_id: u64,
        queue_len_before: usize,
    },
    CallDispatched {
        target: String,
        lb_id: usize,
        server_idx: usize,
        request_id: u64,
    },
}

impl MsCentralizedAudit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: CentralizedEventKind) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push(RecordedEvent { seq, kind });
    }

    pub fn record_call_enqueued(
        &self,
        target: &str,
        lb_id: usize,
        caller_server: usize,
        request_id: u64,
        queue_len_before: usize,
    ) {
        self.record(CentralizedEventKind::CallEnqueued {
            target: target.to_string(),
            lb_id,
            caller_server,
            request_id,
            queue_len_before,
        });
    }

    pub fn record_call_dispatched(
        &self,
        target: &str,
        lb_id: usize,
        server_idx: usize,
        request_id: u64,
    ) {
        self.record(CentralizedEventKind::CallDispatched {
            target: target.to_string(),
            lb_id,
            server_idx,
            request_id,
        });
    }

    pub fn events(&self) -> Vec<CentralizedEventKind> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|e| e.kind.clone())
            .collect()
    }

    fn events_for_target(&self, target: &str) -> Vec<CentralizedEventKind> {
        self.events()
            .into_iter()
            .filter(|kind| match kind {
                CentralizedEventKind::CallEnqueued { target: t, .. }
                | CentralizedEventKind::CallDispatched { target: t, .. } => t == target,
            })
            .collect()
    }

    fn expected_subsets(n_servers: usize, subset_size: u32) -> Vec<HashSet<usize>> {
        let k = if subset_size == 0 {
            n_servers
        } else {
            (subset_size as usize).min(n_servers).max(1)
        };
        let subset_count = n_servers / k;
        (0..subset_count)
            .map(|lb_id| {
                subset::assign_subset(
                    SubsetPolicyKind::Deterministic,
                    n_servers,
                    lb_id,
                    subset_size,
                )
                .into_iter()
                .collect()
            })
            .collect()
    }

    /// Assert observed dispatch servers per LB form a partition matching `assign_subset`.
    pub fn validate_disjoint_subsets(
        &self,
        target: &str,
        n_servers: usize,
        subset_size: u32,
    ) -> Result<(), String> {
        let expected = Self::expected_subsets(n_servers, subset_size);

        let mut observed: HashMap<usize, HashSet<usize>> = HashMap::new();
        for kind in self.events_for_target(target) {
            if let CentralizedEventKind::CallDispatched {
                lb_id, server_idx, ..
            } = kind
            {
                observed.entry(lb_id).or_default().insert(server_idx);
            }
        }

        if observed.is_empty() {
            return Err(format!(
                "target {target}: no centralized dispatches recorded"
            ));
        }

        if observed.len() != expected.len() {
            return Err(format!(
                "target {target}: expected {} LBs with dispatches, saw {}",
                expected.len(),
                observed.len()
            ));
        }

        for (lb_id, servers) in &observed {
            let Some(want) = expected.get(*lb_id) else {
                return Err(format!(
                    "target {target}: unexpected lb_id {lb_id} in dispatches"
                ));
            };
            if !servers.is_subset(want) {
                return Err(format!(
                    "target {target}: lb {lb_id} dispatched outside its subset: observed={servers:?} expected_subset={want:?}"
                ));
            }
        }

        let mut all: HashSet<usize> = HashSet::new();
        for (lb_id, servers) in &observed {
            for &s in servers {
                if !all.insert(s) {
                    return Err(format!(
                        "target {target}: server {s} observed under multiple LBs (including lb {lb_id})"
                    ));
                }
            }
        }

        for i in 0..expected.len() {
            for j in (i + 1)..expected.len() {
                let overlap: Vec<_> = expected[i].intersection(&expected[j]).copied().collect();
                if !overlap.is_empty() {
                    return Err(format!(
                        "target {target}: expected subsets {i} and {j} overlap: {overlap:?}"
                    ));
                }
            }
        }

        Ok(())
    }

    /// Assert every dispatch stays in its LB subset, and enqueue/dispatch lb_ids match
    /// the expected centralized-per-subset topology (`n_lbs = n_servers / k`).
    pub fn validate_centralized_per_subset(
        &self,
        target: &str,
        n_servers: usize,
        subset_size: u32,
    ) -> Result<(), String> {
        self.validate_disjoint_subsets(target, n_servers, subset_size)?;

        let k = if subset_size == 0 {
            n_servers
        } else {
            (subset_size as usize).min(n_servers).max(1)
        };
        let n_lbs = n_servers / k;

        let mut enqueue_lbs = HashSet::new();
        let mut dispatch_lbs = HashSet::new();
        let mut enqueued: HashSet<(usize, u64)> = HashSet::new();

        for kind in self.events_for_target(target) {
            match kind {
                CentralizedEventKind::CallEnqueued {
                    lb_id, request_id, ..
                } => {
                    enqueue_lbs.insert(lb_id);
                    enqueued.insert((lb_id, request_id));
                }
                CentralizedEventKind::CallDispatched {
                    lb_id,
                    server_idx,
                    request_id,
                    ..
                } => {
                    dispatch_lbs.insert(lb_id);
                    if !enqueued.contains(&(lb_id, request_id)) {
                        return Err(format!(
                            "target {target}: request {request_id} dispatched by lb {lb_id} without a matching enqueue"
                        ));
                    }
                    let subset = subset::assign_subset(
                        SubsetPolicyKind::Deterministic,
                        n_servers,
                        lb_id,
                        subset_size,
                    );
                    if !subset.contains(&server_idx) {
                        return Err(format!(
                            "target {target}: lb {lb_id} dispatched request {request_id} to server {server_idx} outside subset {subset:?}"
                        ));
                    }
                }
            }
        }

        if enqueue_lbs.len() != n_lbs {
            return Err(format!(
                "target {target}: expected enqueues on {n_lbs} LBs, saw {}",
                enqueue_lbs.len()
            ));
        }
        if dispatch_lbs.len() != n_lbs {
            return Err(format!(
                "target {target}: expected dispatches on {n_lbs} LBs, saw {}",
                dispatch_lbs.len()
            ));
        }

        Ok(())
    }

    /// Assert every enqueue has `lb_id == caller_server % S` where `S = n_servers / k`.
    pub fn validate_caller_lb_mapping(
        &self,
        target: &str,
        n_servers: usize,
        subset_size: u32,
    ) -> Result<(), String> {
        let k = if subset_size == 0 {
            n_servers
        } else {
            (subset_size as usize).min(n_servers).max(1)
        };
        let n_lbs = n_servers / k;
        if n_lbs == 0 {
            return Err(format!("target {target}: subset count is zero"));
        }

        let mut saw_enqueue = false;
        for kind in self.events_for_target(target) {
            if let CentralizedEventKind::CallEnqueued {
                lb_id,
                caller_server,
                request_id,
                ..
            } = kind
            {
                saw_enqueue = true;
                let expected_lb = caller_server % n_lbs;
                if lb_id != expected_lb {
                    return Err(format!(
                        "target {target}: request {request_id} enqueued on lb {lb_id} but caller_server {caller_server} maps to lb {expected_lb}"
                    ));
                }
            }
        }

        if !saw_enqueue {
            return Err(format!("target {target}: no enqueues recorded"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caller_mapping_rejects_mismatch() {
        let audit = MsCentralizedAudit::new();
        // n=10, k=5 → S=2; caller 1 should map to lb 1
        audit.record_call_enqueued("backend1", 0, 1, 1, 0);
        let err = audit
            .validate_caller_lb_mapping("backend1", 10, 5)
            .unwrap_err();
        assert!(err.contains("maps to lb 1"));
    }

    #[test]
    fn caller_mapping_accepts_modulo() {
        let audit = MsCentralizedAudit::new();
        audit.record_call_enqueued("backend1", 0, 0, 1, 0);
        audit.record_call_enqueued("backend1", 1, 1, 2, 0);
        audit.record_call_enqueued("backend1", 0, 2, 3, 0);
        audit
            .validate_caller_lb_mapping("backend1", 10, 5)
            .expect("mapping ok");
    }
}
