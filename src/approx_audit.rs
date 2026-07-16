use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

type OutboundQueueKey = (usize, String);

/// Records approx pull/intent events during a simulation run for post-hoc invariant checks.
#[derive(Default)]
pub struct ApproxPullAudit {
    next_seq: AtomicU64,
    events: Mutex<Vec<RecordedEvent>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedEvent {
    seq: u64,
    kind: ApproxPullEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApproxPullEventKind {
    /// Upstream balancer chose a downstream target and sent a bound pull intent.
    IntentSent {
        sender_rb_id: usize,
        sender_ms: String,
        sender_server: usize,
        target_ms: String,
        target_server: usize,
        request_id: u64,
    },
    /// Downstream replica received a pull intent and pushed it onto the intent queue.
    IntentQueued {
        downstream_ms: String,
        downstream_server: usize,
        sender_rb_id: usize,
        request_id: u64,
        queue_len_before: usize,
    },
    /// Downstream replica popped the front intent and sent a bound pull upstream.
    IntentDrained {
        downstream_ms: String,
        downstream_server: usize,
        sender_rb_id: usize,
        request_id: u64,
        queue_len_before: usize,
        pending_pulls_before: u32,
        in_flight_before: u32,
        max_concurrency: u32,
    },
    /// Upstream balancer fulfilled a pull and removed a queued outbound call.
    PullFulfilled {
        handler_rb_id: usize,
        handler_ms: String,
        handler_server: usize,
        target_ms: String,
        pull_from_server: usize,
        intent_request_id: u64,
        pulled_request_id: u64,
        queue_len_before: usize,
        queue_head_request_id: Option<u64>,
    },
}

impl ApproxPullAudit {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record(&self, kind: ApproxPullEventKind) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        self.events.lock().unwrap().push(RecordedEvent { seq, kind });
    }

    pub fn record_intent_sent(
        &self,
        sender_rb_id: usize,
        sender_ms: &str,
        sender_server: usize,
        target_ms: &str,
        target_server: usize,
        request_id: u64,
    ) {
        self.record(ApproxPullEventKind::IntentSent {
            sender_rb_id,
            sender_ms: sender_ms.to_string(),
            sender_server,
            target_ms: target_ms.to_string(),
            target_server,
            request_id,
        });
    }

    pub fn record_intent_queued(
        &self,
        downstream_ms: &str,
        downstream_server: usize,
        sender_rb_id: usize,
        request_id: u64,
        queue_len_before: usize,
    ) {
        self.record(ApproxPullEventKind::IntentQueued {
            downstream_ms: downstream_ms.to_string(),
            downstream_server,
            sender_rb_id,
            request_id,
            queue_len_before,
        });
    }

    pub fn record_intent_drained(
        &self,
        downstream_ms: &str,
        downstream_server: usize,
        sender_rb_id: usize,
        request_id: u64,
        queue_len_before: usize,
        pending_pulls_before: u32,
        in_flight_before: u32,
        max_concurrency: u32,
    ) {
        self.record(ApproxPullEventKind::IntentDrained {
            downstream_ms: downstream_ms.to_string(),
            downstream_server,
            sender_rb_id,
            request_id,
            queue_len_before,
            pending_pulls_before,
            in_flight_before,
            max_concurrency,
        });
    }

    pub fn record_pull_fulfilled(
        &self,
        handler_rb_id: usize,
        handler_ms: &str,
        handler_server: usize,
        target_ms: &str,
        pull_from_server: usize,
        intent_request_id: u64,
        pulled_request_id: u64,
        queue_len_before: usize,
        queue_head_request_id: Option<u64>,
    ) {
        self.record(ApproxPullEventKind::PullFulfilled {
            handler_rb_id,
            handler_ms: handler_ms.to_string(),
            handler_server,
            target_ms: target_ms.to_string(),
            pull_from_server,
            intent_request_id,
            pulled_request_id,
            queue_len_before,
            queue_head_request_id,
        });
    }

    pub fn pull_fulfilled_events(&self) -> Vec<(u64, u64, Option<u64>)> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.kind {
                ApproxPullEventKind::PullFulfilled {
                    intent_request_id,
                    pulled_request_id,
                    queue_head_request_id,
                    ..
                } => Some((
                    *intent_request_id,
                    *pulled_request_id,
                    *queue_head_request_id,
                )),
                _ => None,
            })
            .collect()
    }

    /// Shared invariants for bound and no-bind approx runs.
    pub fn validate_common(&self) -> Result<(), String> {
        let events = self.events.lock().unwrap();
        if events.is_empty() {
            return Err("no approx pull audit events recorded".into());
        }

        let mut intent_queue_depth: HashMap<(String, usize), usize> = HashMap::new();
        let mut intent_fifo_queues: HashMap<(String, usize), VecDeque<(usize, u64)>> =
            HashMap::new();
        let mut outbound_queue_depth: HashMap<OutboundQueueKey, usize> = HashMap::new();

        let mut sent = 0usize;
        let mut queued = 0usize;
        let mut drained = 0usize;
        let mut fulfilled = 0usize;

        for recorded in events.iter() {
            match &recorded.kind {
                ApproxPullEventKind::IntentSent {
                    sender_rb_id,
                    target_ms,
                    ..
                } => {
                    let key = (*sender_rb_id, target_ms.clone());
                    let depth = outbound_queue_depth.get(&key).copied().unwrap_or(0);
                    outbound_queue_depth.insert(key, depth + 1);
                    sent += 1;
                }
                ApproxPullEventKind::IntentQueued {
                    downstream_ms,
                    downstream_server,
                    sender_rb_id,
                    request_id,
                    queue_len_before,
                } => {
                    let key = (downstream_ms.clone(), *downstream_server);
                    let expected = intent_queue_depth.get(&key).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "intent queue depth mismatch on IntentQueued \
                             ({downstream_ms}/{downstream_server}): expected {expected}, \
                             got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    intent_queue_depth.insert(key.clone(), expected + 1);
                    intent_fifo_queues
                        .entry(key.clone())
                        .or_default()
                        .push_back((*sender_rb_id, *request_id));
                    queued += 1;
                }
                ApproxPullEventKind::IntentDrained {
                    downstream_ms,
                    downstream_server,
                    sender_rb_id,
                    request_id,
                    queue_len_before,
                    pending_pulls_before,
                    in_flight_before,
                    max_concurrency,
                } => {
                    if *in_flight_before + *pending_pulls_before >= *max_concurrency {
                        return Err(format!(
                            "IntentDrained while at capacity \
                             ({downstream_ms}/{downstream_server}): \
                             in_flight={in_flight_before} pending={pending_pulls_before} \
                             max={max_concurrency} (seq={})",
                            recorded.seq
                        ));
                    }
                    let key = (downstream_ms.clone(), *downstream_server);
                    let expected = intent_queue_depth.get(&key).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "intent queue depth mismatch on IntentDrained \
                             ({downstream_ms}/{downstream_server}): expected {expected}, \
                             got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    if expected == 0 {
                        return Err(format!(
                            "IntentDrained from empty queue \
                             ({downstream_ms}/{downstream_server}) (seq={})",
                            recorded.seq
                        ));
                    }
                    intent_queue_depth.insert(key.clone(), expected - 1);

                    let fifo = intent_fifo_queues.get_mut(&key).expect("fifo queue present");
                    let front = fifo.pop_front().ok_or_else(|| {
                        format!(
                            "IntentDrained with no fifo head \
                             ({downstream_ms}/{downstream_server}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if front != (*sender_rb_id, *request_id) {
                        return Err(format!(
                            "intent queue pop was not FIFO \
                             ({downstream_ms}/{downstream_server}): expected {:?}, \
                             got sender_rb_id={sender_rb_id} request_id={request_id} (seq={})",
                            front, recorded.seq
                        ));
                    }
                    drained += 1;
                }
                ApproxPullEventKind::PullFulfilled {
                    handler_rb_id,
                    target_ms,
                    pulled_request_id: _,
                    queue_len_before,
                    ..
                } => {
                    let key = (*handler_rb_id, target_ms.clone());
                    let expected = outbound_queue_depth.get(&key).copied().unwrap_or(0);
                    if *queue_len_before != expected {
                        return Err(format!(
                            "outbound queue depth mismatch on PullFulfilled \
                             (rb_id={handler_rb_id}, target={target_ms}): expected {expected}, \
                             got {queue_len_before} (seq={})",
                            recorded.seq
                        ));
                    }
                    if expected == 0 {
                        return Err(format!(
                            "PullFulfilled from empty outbound queue \
                             (rb_id={handler_rb_id}, target={target_ms}) (seq={})",
                            recorded.seq
                        ));
                    }
                    outbound_queue_depth.insert(key, expected - 1);
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

        for ((downstream_ms, downstream_server), depth) in &intent_queue_depth {
            if *depth != 0 {
                return Err(format!(
                    "non-empty intent queue at end of run \
                     ({downstream_ms}/{downstream_server})"
                ));
            }
        }
        for ((handler_rb_id, target_ms), depth) in &outbound_queue_depth {
            if *depth != 0 {
                return Err(format!(
                    "non-empty outbound queue at end of run \
                     (rb_id={handler_rb_id}, target={target_ms})"
                ));
            }
        }

        Ok(())
    }

    pub fn validate_bound(&self) -> Result<(), String> {
        self.validate_common()?;
        let events = self.events.lock().unwrap();

        let mut outbound_replay_queues: HashMap<OutboundQueueKey, VecDeque<u64>> = HashMap::new();
        let mut drained: Vec<(usize, u64)> = Vec::new();
        let mut fulfilled: Vec<(usize, u64, u64)> = Vec::new();

        for recorded in events.iter() {
            match &recorded.kind {
                ApproxPullEventKind::IntentSent {
                    sender_rb_id,
                    target_ms,
                    request_id,
                    ..
                } => {
                    outbound_replay_queues
                        .entry((*sender_rb_id, target_ms.clone()))
                        .or_default()
                        .push_back(*request_id);
                }
                ApproxPullEventKind::IntentDrained {
                    sender_rb_id,
                    request_id,
                    ..
                } => drained.push((*sender_rb_id, *request_id)),
                ApproxPullEventKind::PullFulfilled {
                    handler_rb_id,
                    target_ms,
                    intent_request_id,
                    pulled_request_id,
                    ..
                } => {
                    if intent_request_id != pulled_request_id {
                        return Err(format!(
                            "bound pull mismatch (seq={}): intent_request_id={intent_request_id}, \
                             pulled_request_id={pulled_request_id}",
                            recorded.seq
                        ));
                    }
                    let key = (*handler_rb_id, target_ms.clone());
                    let replay = outbound_replay_queues.get_mut(&key).ok_or_else(|| {
                        format!(
                            "bound pull with no replay queue (rb_id={handler_rb_id}, \
                             target={target_ms}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    let idx = replay.iter().position(|id| id == pulled_request_id).ok_or_else(
                        || {
                            format!(
                                "bound pulled request not in replay queue \
                                 (rb_id={handler_rb_id}, target={target_ms}, \
                                 pulled_request_id={pulled_request_id}) (seq={})",
                                recorded.seq
                            )
                        },
                    )?;
                    replay.remove(idx);
                    fulfilled.push((*handler_rb_id, *intent_request_id, *pulled_request_id));
                }
                _ => {}
            }
        }

        for ((handler_rb_id, target_ms), replay) in &outbound_replay_queues {
            if !replay.is_empty() {
                return Err(format!(
                    "non-empty outbound replay queue after bound validation \
                     (rb_id={handler_rb_id}, target={target_ms})"
                ));
            }
        }

        for (sender_rb_id, request_id) in &drained {
            if !fulfilled
                .iter()
                .any(|(rb, intent, _)| rb == sender_rb_id && intent == request_id)
            {
                return Err(format!(
                    "IntentDrained sender_rb_id={sender_rb_id} request_id={request_id} \
                     has no PullFulfilled handler"
                ));
            }
        }

        for (handler_rb_id, intent_request_id, pulled_request_id) in &fulfilled {
            let drain_senders: Vec<_> = drained
                .iter()
                .filter(|(sender, req)| sender == handler_rb_id && req == intent_request_id)
                .collect();
            if drain_senders.is_empty() {
                return Err(format!(
                    "PullFulfilled handler_rb_id={handler_rb_id} \
                     intent_request_id={intent_request_id} has no matching IntentDrained"
                ));
            }
            if drain_senders.len() > 1 {
                return Err(format!(
                    "duplicate PullFulfilled for handler_rb_id={handler_rb_id} \
                     intent_request_id={intent_request_id}"
                ));
            }
            if intent_request_id != pulled_request_id {
                return Err(format!(
                    "bound pull id mismatch for handler_rb_id={handler_rb_id}: \
                     intent={intent_request_id}, pulled={pulled_request_id}"
                ));
            }
        }

        Ok(())
    }

    pub fn validate_no_bind(&self) -> Result<(), String> {
        self.validate_common()?;
        let events = self.events.lock().unwrap();
        let mut outbound_replay_queues: HashMap<OutboundQueueKey, VecDeque<u64>> = HashMap::new();
        let mut saw_mismatch = false;

        for recorded in events.iter() {
            match &recorded.kind {
                ApproxPullEventKind::IntentSent {
                    sender_rb_id,
                    target_ms,
                    request_id,
                    ..
                } => {
                    outbound_replay_queues
                        .entry((*sender_rb_id, target_ms.clone()))
                        .or_default()
                        .push_back(*request_id);
                }
                ApproxPullEventKind::PullFulfilled {
                    handler_rb_id,
                    target_ms,
                    intent_request_id,
                    pulled_request_id,
                    queue_head_request_id,
                    ..
                } => {
                    let head = queue_head_request_id.ok_or_else(|| {
                        format!(
                            "PullFulfilled missing queue_head_request_id \
                             (rb_id={handler_rb_id}, target={target_ms}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if head != *pulled_request_id {
                        return Err(format!(
                            "pulled request is not queue head (rb_id={handler_rb_id}, \
                             target={target_ms}): head={head}, pulled={pulled_request_id} \
                             (seq={})",
                            recorded.seq
                        ));
                    }
                    let key = (*handler_rb_id, target_ms.clone());
                    let replay = outbound_replay_queues.get_mut(&key).ok_or_else(|| {
                        format!(
                            "no-bind pull with no replay queue (rb_id={handler_rb_id}, \
                             target={target_ms}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    let front = replay.pop_front().ok_or_else(|| {
                        format!(
                            "no-bind pull with empty replay queue (rb_id={handler_rb_id}, \
                             target={target_ms}) (seq={})",
                            recorded.seq
                        )
                    })?;
                    if front != *pulled_request_id {
                        return Err(format!(
                            "outbound queue pop was not FCFS (rb_id={handler_rb_id}, \
                             target={target_ms}): expected {front}, got {pulled_request_id} \
                             (seq={})",
                            recorded.seq
                        ));
                    }
                    if intent_request_id != pulled_request_id {
                        saw_mismatch = true;
                    }
                }
                _ => {}
            }
        }

        for ((handler_rb_id, target_ms), replay) in &outbound_replay_queues {
            if !replay.is_empty() {
                return Err(format!(
                    "non-empty outbound replay queue after no-bind validation \
                     (rb_id={handler_rb_id}, target={target_ms})"
                ));
            }
        }

        if !saw_mismatch {
            return Err(
                "no-bind trace never exercised intent_request_id != pulled_request_id; \
                 no-bind semantics may not be active"
                    .into(),
            );
        }

        Ok(())
    }

    /// Check delivery, queue accounting, FIFO pops, and bound pull routing.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_bound()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_bound_accepts_well_formed_sequence() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(3, "frontend", 3, "backend1", 4, 10);
        audit.record_intent_queued("backend1", 4, 3, 10, 0);
        audit.record_intent_drained("backend1", 4, 3, 10, 1, 0, 0, 1);
        audit.record_pull_fulfilled(3, "frontend", 3, "backend1", 4, 10, 10, 1, Some(10));
        audit.validate_bound().expect("valid sequence");
    }

    #[test]
    fn validate_rejects_fifo_violation() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(1, "frontend", 1, "backend1", 0, 1);
        audit.record_intent_sent(2, "frontend", 2, "backend1", 0, 2);
        audit.record_intent_queued("backend1", 0, 1, 1, 0);
        audit.record_intent_queued("backend1", 0, 2, 2, 1);
        audit.record_intent_drained("backend1", 0, 2, 2, 2, 0, 0, 2);
        let err = audit.validate_common().unwrap_err();
        assert!(err.contains("not FIFO"), "unexpected error: {err}");
    }

    #[test]
    fn validate_no_bind_accepts_intent_mismatch() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(0, "frontend", 0, "backend1", 1, 3);
        audit.record_intent_sent(0, "frontend", 0, "backend1", 1, 5);
        audit.record_intent_queued("backend1", 1, 0, 5, 0);
        audit.record_intent_drained("backend1", 1, 0, 5, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, "frontend", 0, "backend1", 1, 5, 3, 2, Some(3));
        audit.record_intent_queued("backend1", 1, 0, 3, 0);
        audit.record_intent_drained("backend1", 1, 0, 3, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, "frontend", 0, "backend1", 1, 3, 5, 1, Some(5));
        audit.validate_no_bind().expect("valid no-bind sequence");
    }

    #[test]
    fn validate_no_bind_rejects_wrong_queue_head() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(0, "frontend", 0, "backend1", 1, 3);
        audit.record_intent_queued("backend1", 1, 0, 3, 0);
        audit.record_intent_drained("backend1", 1, 0, 3, 1, 0, 0, 1);
        audit.record_pull_fulfilled(0, "frontend", 0, "backend1", 1, 3, 99, 1, Some(3));
        let err = audit.validate_no_bind().unwrap_err();
        assert!(
            err.contains("queue head") || err.contains("not FCFS"),
            "unexpected error: {err}"
        );
    }
}
