use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::subset::{self, SubsetPolicyKind};

/// Records centralized enqueue/dispatch events for post-hoc subset invariant checks.
#[derive(Default)]
pub struct LbCentralizedAudit {
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
    TaskEnqueued {
        lb_id: usize,
        task_id: u64,
        queue_len_before: usize,
    },
    TaskDispatched {
        lb_id: usize,
        server_idx: usize,
        task_id: u64,
    },
}

impl LbCentralizedAudit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: CentralizedEventKind) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push(RecordedEvent { seq, kind });
    }

    pub fn record_task_enqueued(&self, lb_id: usize, task_id: u64, queue_len_before: usize) {
        self.record(CentralizedEventKind::TaskEnqueued {
            lb_id,
            task_id,
            queue_len_before,
        });
    }

    pub fn record_task_dispatched(&self, lb_id: usize, server_idx: usize, task_id: u64) {
        self.record(CentralizedEventKind::TaskDispatched {
            lb_id,
            server_idx,
            task_id,
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

    /// Assert observed dispatch servers per LB form a partition matching `assign_subset`.
    pub fn validate_disjoint_subsets(
        &self,
        n_servers: usize,
        subset_size: u32,
    ) -> Result<(), String> {
        let expected: Vec<HashSet<usize>> = {
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
        };

        let mut observed: HashMap<usize, HashSet<usize>> = HashMap::new();
        for kind in self.events() {
            if let CentralizedEventKind::TaskDispatched {
                lb_id, server_idx, ..
            } = kind
            {
                observed.entry(lb_id).or_default().insert(server_idx);
            }
        }

        if observed.is_empty() {
            return Err("no centralized dispatches recorded".into());
        }

        if observed.len() != expected.len() {
            return Err(format!(
                "expected {} LBs with dispatches, saw {}",
                expected.len(),
                observed.len()
            ));
        }

        for (lb_id, servers) in &observed {
            let Some(want) = expected.get(*lb_id) else {
                return Err(format!("unexpected lb_id {lb_id} in dispatches"));
            };
            if !servers.is_subset(want) {
                return Err(format!(
                    "lb {lb_id} dispatched outside its subset: observed={servers:?} expected_subset={want:?}"
                ));
            }
        }

        let mut all: HashSet<usize> = HashSet::new();
        for (lb_id, servers) in &observed {
            for &s in servers {
                if !all.insert(s) {
                    return Err(format!(
                        "server {s} observed under multiple LBs (including lb {lb_id})"
                    ));
                }
            }
        }

        // Expected partitions themselves must be pairwise disjoint.
        for i in 0..expected.len() {
            for j in (i + 1)..expected.len() {
                let overlap: Vec<_> = expected[i].intersection(&expected[j]).copied().collect();
                if !overlap.is_empty() {
                    return Err(format!(
                        "expected subsets {i} and {j} overlap: {overlap:?}"
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
        n_servers: usize,
        subset_size: u32,
    ) -> Result<(), String> {
        self.validate_disjoint_subsets(n_servers, subset_size)?;

        let k = if subset_size == 0 {
            n_servers
        } else {
            (subset_size as usize).min(n_servers).max(1)
        };
        let n_lbs = n_servers / k;

        let mut enqueue_lbs = HashSet::new();
        let mut dispatch_lbs = HashSet::new();
        let mut enqueued: HashSet<(usize, u64)> = HashSet::new();

        for kind in self.events() {
            match kind {
                CentralizedEventKind::TaskEnqueued { lb_id, task_id, .. } => {
                    enqueue_lbs.insert(lb_id);
                    enqueued.insert((lb_id, task_id));
                }
                CentralizedEventKind::TaskDispatched {
                    lb_id,
                    server_idx,
                    task_id,
                } => {
                    dispatch_lbs.insert(lb_id);
                    if !enqueued.contains(&(lb_id, task_id)) {
                        return Err(format!(
                            "task {task_id} dispatched by lb {lb_id} without a matching enqueue"
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
                            "lb {lb_id} dispatched task {task_id} to server {server_idx} outside subset {subset:?}"
                        ));
                    }
                }
            }
        }

        if enqueue_lbs.len() != n_lbs {
            return Err(format!(
                "expected enqueues on {n_lbs} LBs, saw {}",
                enqueue_lbs.len()
            ));
        }
        if dispatch_lbs.len() != n_lbs {
            return Err(format!(
                "expected dispatches on {n_lbs} LBs, saw {}",
                dispatch_lbs.len()
            ));
        }

        Ok(())
    }
}
