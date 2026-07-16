use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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
    /// Upstream balancer fulfilled a bound pull and removed the matching queued call.
    PullMatched {
        handler_rb_id: usize,
        handler_ms: String,
        handler_server: usize,
        target_ms: String,
        pull_from_server: usize,
        request_id: u64,
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

    pub fn record_pull_matched(
        &self,
        handler_rb_id: usize,
        handler_ms: &str,
        handler_server: usize,
        target_ms: &str,
        pull_from_server: usize,
        request_id: u64,
    ) {
        self.record(ApproxPullEventKind::PullMatched {
            handler_rb_id,
            handler_ms: handler_ms.to_string(),
            handler_server,
            target_ms: target_ms.to_string(),
            pull_from_server,
            request_id,
        });
    }

    /// Check delivery, queue accounting, FIFO pops, and bound pull routing.
    pub fn validate(&self) -> Result<(), String> {
        let events = self.events.lock().unwrap();
        if events.is_empty() {
            return Err("no approx pull audit events recorded".into());
        }

        let mut intent_queue_depth: HashMap<(String, usize), usize> = HashMap::new();
        let mut fifo_queues: HashMap<(String, usize), VecDeque<(usize, u64)>> = HashMap::new();

        let mut sent: Vec<(String, usize, usize, u64)> = Vec::new();
        let mut queued: Vec<(String, usize, usize, u64)> = Vec::new();
        let mut drained: Vec<(String, usize, usize, u64)> = Vec::new();
        let mut matched: Vec<(usize, u64)> = Vec::new();

        for recorded in events.iter() {
            match &recorded.kind {
                ApproxPullEventKind::IntentSent {
                    sender_rb_id,
                    target_ms,
                    target_server,
                    request_id,
                    ..
                } => {
                    sent.push((
                        target_ms.clone(),
                        *target_server,
                        *sender_rb_id,
                        *request_id,
                    ));
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
                    fifo_queues
                        .entry(key.clone())
                        .or_default()
                        .push_back((*sender_rb_id, *request_id));
                    queued.push((
                        downstream_ms.clone(),
                        *downstream_server,
                        *sender_rb_id,
                        *request_id,
                    ));
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

                    let fifo = fifo_queues.get_mut(&key).expect("fifo queue present");
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

                    drained.push((
                        downstream_ms.clone(),
                        *downstream_server,
                        *sender_rb_id,
                        *request_id,
                    ));
                }
                ApproxPullEventKind::PullMatched {
                    handler_rb_id,
                    request_id,
                    ..
                } => {
                    matched.push((*handler_rb_id, *request_id));
                }
            }
        }

        if sent.is_empty() {
            return Err("no IntentSent events".into());
        }
        if sent.len() != queued.len() {
            return Err(format!(
                "intent delivery mismatch: sent {} intents, queued {}",
                sent.len(),
                queued.len()
            ));
        }
        if sent.len() != drained.len() {
            return Err(format!(
                "intent drain mismatch: sent {} intents, drained {}",
                sent.len(),
                drained.len()
            ));
        }
        if sent.len() != matched.len() {
            return Err(format!(
                "bound pull mismatch: sent {} intents, matched {} pulls",
                sent.len(),
                matched.len()
            ));
        }

        let mut sent_counts: HashMap<(String, usize, usize, u64), u32> = HashMap::new();
        for key in &sent {
            *sent_counts.entry(key.clone()).or_default() += 1;
        }
        let mut queued_counts: HashMap<(String, usize, usize, u64), u32> = HashMap::new();
        for key in &queued {
            *queued_counts.entry(key.clone()).or_default() += 1;
        }
        if sent_counts != queued_counts {
            return Err(format!(
                "IntentSent vs IntentQueued pairing mismatch: sent={sent_counts:?} queued={queued_counts:?}"
            ));
        }

        let mut drained_counts: HashMap<(String, usize, usize, u64), u32> = HashMap::new();
        for key in &drained {
            *drained_counts.entry(key.clone()).or_default() += 1;
        }
        if sent_counts != drained_counts {
            return Err(format!(
                "IntentSent vs IntentDrained pairing mismatch: sent={sent_counts:?} drained={drained_counts:?}"
            ));
        }

        for (target_ms, target_server, sender_rb_id, request_id) in &drained {
            if !matched.contains(&(*sender_rb_id, *request_id)) {
                return Err(format!(
                    "IntentDrained sender_rb_id={sender_rb_id} request_id={request_id} \
                     on {target_ms}/{target_server} has no PullMatched handler"
                ));
            }
        }

        for (handler_rb_id, request_id) in &matched {
            let drain_senders: Vec<_> = drained
                .iter()
                .filter(|(_, _, sender, req)| sender == handler_rb_id && req == request_id)
                .collect();
            if drain_senders.is_empty() {
                return Err(format!(
                    "PullMatched handler_rb_id={handler_rb_id} request_id={request_id} \
                     has no matching IntentDrained"
                ));
            }
            if drain_senders.len() > 1 {
                return Err(format!(
                    "duplicate PullMatched for handler_rb_id={handler_rb_id} request_id={request_id}"
                ));
            }
        }

        for (downstream_ms, downstream_server) in intent_queue_depth.keys() {
            if intent_queue_depth[&(downstream_ms.clone(), *downstream_server)] != 0 {
                return Err(format!(
                    "non-empty intent queue at end of run \
                     ({downstream_ms}/{downstream_server})"
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_well_formed_sequence() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(3, "frontend", 3, "backend1", 4, 10);
        audit.record_intent_queued("backend1", 4, 3, 10, 0);
        audit.record_intent_drained("backend1", 4, 3, 10, 1, 0, 0, 1);
        audit.record_pull_matched(3, "frontend", 3, "backend1", 4, 10);
        audit.validate().expect("valid sequence");
    }

    #[test]
    fn validate_rejects_fifo_violation() {
        let audit = ApproxPullAudit::new();
        audit.record_intent_sent(1, "frontend", 1, "backend1", 0, 1);
        audit.record_intent_sent(2, "frontend", 2, "backend1", 0, 2);
        audit.record_intent_queued("backend1", 0, 1, 1, 0);
        audit.record_intent_queued("backend1", 0, 2, 2, 1);
        audit.record_intent_drained("backend1", 0, 2, 2, 2, 0, 0, 2);
        let err = audit.validate().unwrap_err();
        assert!(err.contains("not FIFO"), "unexpected error: {err}");
    }
}
