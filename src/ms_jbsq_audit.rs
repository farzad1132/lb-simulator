use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Records MS jbsq pull occupancy events for post-hoc invariant checks.
#[derive(Default)]
pub struct MsJbsqAudit {
    next_seq: AtomicU64,
    events: Mutex<Vec<RecordedEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    seq: u64,
    kind: JbsqEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JbsqEventKind {
    PullSent {
        microservice_id: String,
        server_idx: usize,
        occupancy_before: u32,
        pending_before: u32,
        n: u32,
    },
    PullArrived {
        microservice_id: String,
        server_idx: usize,
        queue_before: u32,
        in_flight_before: u32,
        pending_before: u32,
        n: u32,
    },
}

impl MsJbsqAudit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: JbsqEventKind) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push(RecordedEvent { seq, kind });
    }

    pub fn record_pull_sent(
        &self,
        microservice_id: &str,
        server_idx: usize,
        occupancy_before: u32,
        pending_before: u32,
        n: u32,
    ) {
        self.record(JbsqEventKind::PullSent {
            microservice_id: microservice_id.to_string(),
            server_idx,
            occupancy_before,
            pending_before,
            n,
        });
    }

    pub fn record_pull_arrived(
        &self,
        microservice_id: &str,
        server_idx: usize,
        queue_before: u32,
        in_flight_before: u32,
        pending_before: u32,
        n: u32,
    ) {
        self.record(JbsqEventKind::PullArrived {
            microservice_id: microservice_id.to_string(),
            server_idx,
            queue_before,
            in_flight_before,
            pending_before,
            n,
        });
    }

    /// Never send a pull when `occupancy + pending >= n`.
    /// After each arrival, reconstructed occupancy never exceeds `n`.
    pub fn validate(&self) -> Result<(), String> {
        let events = self.events.lock().unwrap();
        if events.is_empty() {
            return Err("jbsq audit recorded no events".into());
        }

        // Key: (ms, server) -> (queue, in_flight, pending)
        let mut state: HashMap<(String, usize), (u32, u32, u32)> = HashMap::new();
        let mut saw_local_queue = false;

        for event in events.iter() {
            match &event.kind {
                JbsqEventKind::PullSent {
                    microservice_id,
                    server_idx,
                    occupancy_before,
                    pending_before,
                    n,
                } => {
                    if *occupancy_before + *pending_before >= *n {
                        return Err(format!(
                            "PullSent when at capacity: ms={microservice_id} server={server_idx} \
                             occupancy={occupancy_before} pending={pending_before} n={n} (seq={})",
                            event.seq
                        ));
                    }
                    let key = (microservice_id.clone(), *server_idx);
                    let entry = state.entry(key).or_insert((0, 0, 0));
                    entry.2 = entry.2.saturating_add(1);
                }
                JbsqEventKind::PullArrived {
                    microservice_id,
                    server_idx,
                    queue_before,
                    in_flight_before,
                    pending_before,
                    n,
                } => {
                    if *queue_before > 0 {
                        saw_local_queue = true;
                    }
                    let occupancy_before = *queue_before + *in_flight_before;
                    // After accepting this pull into the local queue, occupancy grows by 1
                    // (pending decreases by 1, queue increases by 1).
                    let occupancy_after = occupancy_before + 1;
                    if occupancy_after > *n {
                        return Err(format!(
                            "PullArrived exceeds n: ms={microservice_id} server={server_idx} \
                             queue={queue_before} in_flight={in_flight_before} \
                             occupancy_after={occupancy_after} n={n} (seq={})",
                            event.seq
                        ));
                    }
                    let key = (microservice_id.clone(), *server_idx);
                    let entry = state.entry(key).or_insert((0, 0, 0));
                    entry.2 = pending_before.saturating_sub(1);
                    entry.0 = *queue_before + 1;
                    entry.1 = *in_flight_before;
                }
            }
        }

        if !saw_local_queue {
            // Soft signal for tests that want n > concurrency; callers may ignore.
            let _ = saw_local_queue;
        }

        Ok(())
    }

    /// True if any pull arrived while the replica already held upstream work
    /// (queued or in service), so the new request must sit on the local queue.
    pub fn observed_local_queueing(&self) -> bool {
        self.events.lock().unwrap().iter().any(|e| {
            matches!(
                &e.kind,
                JbsqEventKind::PullArrived {
                    queue_before,
                    in_flight_before,
                    ..
                } if *queue_before > 0 || *in_flight_before > 0
            )
        })
    }

    pub fn pull_sent_count(&self) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.kind, JbsqEventKind::PullSent { .. }))
            .count()
    }

    pub fn pull_arrived_count(&self) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.kind, JbsqEventKind::PullArrived { .. }))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_rejects_pull_at_capacity() {
        let audit = MsJbsqAudit::new();
        audit.record_pull_sent("backend1", 0, 2, 0, 2);
        let err = audit.validate().unwrap_err();
        assert!(err.contains("PullSent when at capacity"));
    }

    #[test]
    fn validate_accepts_under_capacity_pull_and_arrival() {
        let audit = MsJbsqAudit::new();
        audit.record_pull_sent("backend1", 0, 0, 0, 2);
        audit.record_pull_arrived("backend1", 0, 0, 0, 1, 2);
        audit.validate().unwrap();
    }
}
