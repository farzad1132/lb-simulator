use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Records approx pull/intent events during an `lb` simulation run for post-hoc invariant checks.
#[derive(Default)]
pub struct LbPullAudit {
    next_seq: AtomicU64,
    events: Mutex<Vec<RecordedEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    seq: u64,
    kind: LbPullEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LbPullEventKind {
    TaskEnqueued {
        lb_id: usize,
        task_id: u64,
        queue_len_before: usize,
    },
    IntentSent {
        lb_id: usize,
        target_server: usize,
        request_id: u64,
    },
    IntentQueued {
        server_idx: usize,
        sender_lb_id: usize,
        request_id: u64,
        queue_len_before: usize,
    },
    IntentDrained {
        server_idx: usize,
        sender_lb_id: usize,
        intent_request_id: u64,
        queue_len_before: usize,
        pending_pulls_before: u32,
        in_flight_before: u32,
        max_concurrency: u32,
    },
    PullFulfilled {
        lb_id: usize,
        server_idx: usize,
        intent_request_id: Option<u64>,
        pulled_task_id: u64,
        queue_len_before: usize,
        queue_head_task_id: Option<u64>,
    },
}

impl LbPullAudit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: LbPullEventKind) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push(RecordedEvent { seq, kind });
    }

    pub fn record_task_enqueued(&self, lb_id: usize, task_id: u64, queue_len_before: usize) {
        self.record(LbPullEventKind::TaskEnqueued {
            lb_id,
            task_id,
            queue_len_before,
        });
    }

    pub fn record_intent_sent(&self, lb_id: usize, target_server: usize, request_id: u64) {
        self.record(LbPullEventKind::IntentSent {
            lb_id,
            target_server,
            request_id,
        });
    }

    pub fn record_intent_queued(
        &self,
        server_idx: usize,
        sender_lb_id: usize,
        request_id: u64,
        queue_len_before: usize,
    ) {
        self.record(LbPullEventKind::IntentQueued {
            server_idx,
            sender_lb_id,
            request_id,
            queue_len_before,
        });
    }

    pub fn record_intent_drained(
        &self,
        server_idx: usize,
        sender_lb_id: usize,
        intent_request_id: u64,
        queue_len_before: usize,
        pending_pulls_before: u32,
        in_flight_before: u32,
        max_concurrency: u32,
    ) {
        self.record(LbPullEventKind::IntentDrained {
            server_idx,
            sender_lb_id,
            intent_request_id,
            queue_len_before,
            pending_pulls_before,
            in_flight_before,
            max_concurrency,
        });
    }

    pub fn record_pull_fulfilled(
        &self,
        lb_id: usize,
        server_idx: usize,
        intent_request_id: Option<u64>,
        pulled_task_id: u64,
        queue_len_before: usize,
        queue_head_task_id: Option<u64>,
    ) {
        self.record(LbPullEventKind::PullFulfilled {
            lb_id,
            server_idx,
            intent_request_id,
            pulled_task_id,
            queue_len_before,
            queue_head_task_id,
        });
    }

    pub fn pull_fulfilled_events(&self) -> Vec<(Option<u64>, u64, Option<u64>)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.kind {
                LbPullEventKind::PullFulfilled {
                    intent_request_id,
                    pulled_task_id,
                    queue_head_task_id,
                    ..
                } => Some((
                    *intent_request_id,
                    *pulled_task_id,
                    *queue_head_task_id,
                )),
                _ => None,
            })
            .collect()
    }

    /// Shared invariants for bound and no-bind approx runs.
    pub fn validate_common(&self) -> Result<(), String> {
        let events = self.events.lock().unwrap();
        if events.is_empty() {
            return Err("no lb pull audit events recorded".into());
        }

        let mut intent_queue_depth: HashMap<usize, usize> = HashMap::new();
        let mut fifo_queues: HashMap<usize, VecDeque<(usize, u64)>> = HashMap::new();
        let mut task_queue_depth: HashMap<usize, usize> = HashMap::new();

        let mut sent = 0usize;
        let mut queued = 0usize;
        let mut drained = 0usize;
        let mut fulfilled = 0usize;

        for recorded in events.iter() {
            match &recorded.kind {
                LbPullEventKind::TaskEnqueued {
                    lb_id,
                    task_id: _,
                    queue_len_before,
                } => {
                    let expected = task_queue_depth.get(lb_id).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "task queue depth mismatch on TaskEnqueued (lb_id={lb_id}): \
                             expected {expected}, got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    task_queue_depth.insert(*lb_id, expected + 1);
                }
                LbPullEventKind::IntentSent { .. } => sent += 1,
                LbPullEventKind::IntentQueued {
                    server_idx,
                    sender_lb_id,
                    request_id,
                    queue_len_before,
                } => {
                    let expected = intent_queue_depth.get(server_idx).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "intent queue depth mismatch on IntentQueued (server={server_idx}): \
                             expected {expected}, got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    intent_queue_depth.insert(*server_idx, expected + 1);
                    fifo_queues
                        .entry(*server_idx)
                        .or_default()
                        .push_back((*sender_lb_id, *request_id));
                    queued += 1;
                }
                LbPullEventKind::IntentDrained {
                    server_idx,
                    sender_lb_id,
                    intent_request_id,
                    queue_len_before,
                    pending_pulls_before,
                    in_flight_before,
                    max_concurrency,
                } => {
                    if *in_flight_before + *pending_pulls_before >= *max_concurrency {
                        return Err(format!(
                            "IntentDrained while at capacity (server={server_idx}): \
                             in_flight={in_flight_before} pending={pending_pulls_before} \
                             max={max_concurrency} (seq={})",
                            recorded.seq
                        ));
                    }
                    let expected = intent_queue_depth.get(server_idx).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "intent queue depth mismatch on IntentDrained (server={server_idx}): \
                             expected {expected}, got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    if expected == 0 {
                        return Err(format!(
                            "IntentDrained from empty queue (server={server_idx}) (seq={})",
                            recorded.seq
                        ));
                    }
                    intent_queue_depth.insert(*server_idx, expected - 1);

                    let fifo = fifo_queues.get_mut(server_idx).expect("fifo queue present");
                    let front = fifo.pop_front().ok_or_else(|| {
                        format!(
                            "IntentDrained with no fifo head (server={server_idx}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if front != (*sender_lb_id, *intent_request_id) {
                        return Err(format!(
                            "intent queue pop was not FIFO (server={server_idx}): \
                             expected {front:?}, got sender_lb_id={sender_lb_id} \
                             intent_request_id={intent_request_id} (seq={})",
                            recorded.seq
                        ));
                    }
                    drained += 1;
                }
                LbPullEventKind::PullFulfilled {
                    lb_id,
                    pulled_task_id: _,
                    queue_len_before,
                    ..
                } => {
                    let expected = task_queue_depth.get(lb_id).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "task queue depth mismatch on PullFulfilled (lb_id={lb_id}): \
                             expected {expected}, got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    if expected == 0 {
                        return Err(format!(
                            "PullFulfilled from empty queue (lb_id={lb_id}) (seq={})",
                            recorded.seq
                        ));
                    }
                    task_queue_depth.insert(*lb_id, expected - 1);
                    fulfilled += 1;
                }
            }
        }

        if sent == 0 {
            return Err("no IntentSent events".into());
        }
        if sent != queued {
            return Err(format!(
                "intent delivery mismatch: sent {sent} intents, queued {queued}"
            ));
        }
        if sent != drained {
            return Err(format!(
                "intent drain mismatch: sent {sent} intents, drained {drained}"
            ));
        }
        if sent != fulfilled {
            return Err(format!(
                "pull fulfillment mismatch: sent {sent} intents, fulfilled {fulfilled} pulls"
            ));
        }

        for (&server_idx, &depth) in &intent_queue_depth {
            if depth != 0 {
                return Err(format!(
                    "non-empty intent queue at end of run (server={server_idx})"
                ));
            }
        }
        for (&lb_id, &depth) in &task_queue_depth {
            if depth != 0 {
                return Err(format!(
                    "non-empty task queue at end of run (lb_id={lb_id})"
                ));
            }
        }

        Ok(())
    }

    pub fn validate_bound(&self) -> Result<(), String> {
        self.validate_common()?;
        let events = self.events.lock().unwrap();
        let mut task_queues: HashMap<usize, VecDeque<u64>> = HashMap::new();
        for recorded in events.iter() {
            match &recorded.kind {
                LbPullEventKind::TaskEnqueued { lb_id, task_id, .. } => {
                    task_queues.entry(*lb_id).or_default().push_back(*task_id);
                }
                LbPullEventKind::PullFulfilled {
                    lb_id,
                    intent_request_id,
                    pulled_task_id,
                    ..
                } => {
                    let intent_id = intent_request_id.ok_or_else(|| {
                        format!(
                            "bound pull missing intent_request_id (seq={})",
                            recorded.seq
                        )
                    })?;
                    if intent_id != *pulled_task_id {
                        return Err(format!(
                            "bound pull mismatch (seq={}): intent_request_id={intent_id}, \
                             pulled_task_id={pulled_task_id}",
                            recorded.seq
                        ));
                    }
                    let task_q = task_queues.get_mut(lb_id).ok_or_else(|| {
                        format!("bound pull with no replay queue (lb_id={lb_id}) (seq={})", recorded.seq)
                    })?;
                    let idx = task_q.iter().position(|id| id == pulled_task_id).ok_or_else(|| {
                        format!(
                            "bound pulled task not in replay queue (lb_id={lb_id}, \
                             pulled_task_id={pulled_task_id}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    task_q.remove(idx);
                }
                _ => {}
            }
        }
        for (&lb_id, task_q) in &task_queues {
            if !task_q.is_empty() {
                return Err(format!(
                    "non-empty task replay queue after bound validation (lb_id={lb_id})"
                ));
            }
        }
        Ok(())
    }

    pub fn validate_no_bind(&self) -> Result<(), String> {
        self.validate_common()?;
        let events = self.events.lock().unwrap();
        let mut task_queues: HashMap<usize, VecDeque<u64>> = HashMap::new();
        let mut saw_mismatch = false;
        for recorded in events.iter() {
            match &recorded.kind {
                LbPullEventKind::TaskEnqueued { lb_id, task_id, .. } => {
                    task_queues.entry(*lb_id).or_default().push_back(*task_id);
                }
                LbPullEventKind::PullFulfilled {
                    lb_id,
                    intent_request_id,
                    pulled_task_id,
                    queue_head_task_id,
                    ..
                } => {
                    let head = queue_head_task_id.ok_or_else(|| {
                        format!(
                            "PullFulfilled missing queue_head_task_id (lb_id={lb_id}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if head != *pulled_task_id {
                        return Err(format!(
                            "pulled task is not queue head (lb_id={lb_id}): \
                             head={head}, pulled={pulled_task_id} (seq={})",
                            recorded.seq
                        ));
                    }
                    let task_q = task_queues.get_mut(lb_id).ok_or_else(|| {
                        format!("no-bind pull with no replay queue (lb_id={lb_id}) (seq={})", recorded.seq)
                    })?;
                    let front = task_q.pop_front().ok_or_else(|| {
                        format!(
                            "no-bind pull with empty replay queue (lb_id={lb_id}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if front != *pulled_task_id {
                        return Err(format!(
                            "task queue pop was not FCFS (lb_id={lb_id}): \
                             expected {front}, got {pulled_task_id} (seq={})",
                            recorded.seq
                        ));
                    }
                    if let Some(intent_id) = intent_request_id {
                        if intent_id != pulled_task_id {
                            saw_mismatch = true;
                        }
                    }
                }
                _ => {}
            }
        }
        for (&lb_id, task_q) in &task_queues {
            if !task_q.is_empty() {
                return Err(format!(
                    "non-empty task replay queue after no-bind validation (lb_id={lb_id})"
                ));
            }
        }
        if !saw_mismatch {
            return Err(
                "no-bind trace never exercised intent_request_id != pulled_task_id; \
                 no-bind semantics may not be active"
                    .into(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_no_bind_accepts_intent_mismatch() {
        let audit = LbPullAudit::new();
        audit.record_task_enqueued(0, 3, 0);
        audit.record_task_enqueued(0, 5, 1);
        audit.record_intent_sent(0, 0, 3);
        audit.record_intent_sent(0, 1, 5);
        audit.record_intent_queued(1, 0, 5, 0);
        audit.record_intent_drained(1, 0, 5, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, 1, Some(5), 3, 2, Some(3));
        audit.record_intent_queued(0, 0, 3, 0);
        audit.record_intent_drained(0, 0, 3, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, 0, Some(3), 5, 1, Some(5));
        audit.validate_no_bind().expect("valid no-bind sequence");
    }

    #[test]
    fn validate_no_bind_rejects_wrong_queue_head() {
        let audit = LbPullAudit::new();
        audit.record_task_enqueued(0, 3, 0);
        audit.record_intent_sent(0, 0, 3);
        audit.record_intent_queued(0, 0, 3, 0);
        audit.record_intent_drained(0, 0, 3, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, 0, Some(3), 99, 1, Some(3));
        let err = audit.validate_no_bind().unwrap_err();
        assert!(
            err.contains("not queue head") || err.contains("not FCFS"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_bound_rejects_intent_mismatch() {
        let audit = LbPullAudit::new();
        audit.record_task_enqueued(0, 3, 0);
        audit.record_intent_sent(0, 0, 3);
        audit.record_intent_queued(0, 0, 3, 0);
        audit.record_intent_drained(0, 0, 3, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, 0, Some(5), 3, 1, Some(3));
        let err = audit.validate_bound().unwrap_err();
        assert!(err.contains("bound pull mismatch"), "unexpected error: {err}");
    }
}
