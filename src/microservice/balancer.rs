use super::hop::{Hop, OutboundCall, OutboundRelease, ReplicaInput};
use super::occupancy::OccupancyAccumulator;
use super::trace::MsTracer;
use crate::approx::{fatal_pull_abort, PullIntent};
use crate::approx_audit::ApproxPullAudit;
use crate::policy::LoadBalancePolicy;
use crate::policy::LoadBalancePolicyKind;
use crate::policy::PowerOfTwoPolicy;
use crate::rng;
use crate::scheduling::{edf_insert_index, SchedulingPolicyKind};
use hdrhistogram::Histogram;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const CORR_SLACK_DIST_WARMUP: u64 = 200;
const SECS_TO_MS: f64 = 1000.0;

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
}

fn new_corr_histogram() -> Histogram<u64> {
    Histogram::<u64>::new(2).expect("corr histogram precision")
}

fn time_to_ms(time: MonotonicTime) -> f64 {
    time.duration_since(MonotonicTime::EPOCH).as_secs_f64() * SECS_TO_MS
}

fn record_ms(hist: &mut Histogram<u64>, ms: f64) {
    if ms < 0.0 {
        return;
    }
    let value = ms.round().max(0.0) as u64;
    if let Err(e) = hist.record(value) {
        eprintln!("corr histogram record failed for {value} ms: {e}");
    }
}

fn slack_cdf_percentile(hist: &Histogram<u64>, value_ms: f64) -> f64 {
    if hist.is_empty() {
        return 0.0;
    }
    hist.quantile_below(value_ms.floor().max(0.0) as u64)
}

fn corr_rank(slack_hist: &Histogram<u64>, _resp_hist: &Histogram<u64>, sd_ms: f64) -> usize {
    if slack_hist.len() < CORR_SLACK_DIST_WARMUP {
        0
    } else {
        if sd_ms < 0.0 {
            return 10;
        }
        let p = slack_cdf_percentile(slack_hist, sd_ms);

        match p {
            0.0..0.5 => 0,
            0.5..0.8 => 1,
            _ => 2,
        }
    }
}

fn select_corr_replica(server_indices: &[usize], local_inflight: &[u32], rank: usize) -> usize {
    if server_indices.is_empty() {
        return 0;
    }
    let mut distinct_loads: Vec<u32> = server_indices
        .iter()
        .map(|&idx| local_inflight[idx])
        .collect();
    distinct_loads.sort();
    distinct_loads.dedup();
    let target_load = distinct_loads[rank.min(distinct_loads.len() - 1)];
    let tied: Vec<usize> = server_indices
        .iter()
        .filter(|&&idx| local_inflight[idx] == target_load)
        .copied()
        .collect();
    tied[rng::random_usize_range(0..tied.len())]
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicaPull {
    pub target_microservice: String,
    pub server_idx: usize,
    pub request_id: u64,
}

#[derive(Deserialize, Serialize)]
pub struct EdgeBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_policy: LoadBalancePolicyKind,
    api: String,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<ReplicaInput>>,
}

impl EdgeBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        api: String,
        n_servers: usize,
        server_indices: Vec<usize>,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        debug_assert!(
            server_indices.iter().all(|&i| i < n_servers),
            "server_indices must be within n_servers"
        );
        Self {
            policy,
            lb_policy,
            api,
            tracer,
            local_inflight: vec![0; n_servers],
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl EdgeBalancer {
    pub async fn input(&mut self, hop: Hop, cx: &Context<Self>) {
        for (scratch, &server_idx) in self.load_scratch.iter_mut().zip(self.server_indices.iter()) {
            *scratch = self.local_inflight[server_idx];
        }
        let local_idx = self
            .policy
            .select(&self.load_scratch)
            .min(self.load_scratch.len().saturating_sub(1));
        let global_idx = self.server_indices[local_idx];
        self.local_inflight[global_idx] += 1;
        if let Some(tracer) = &self.tracer {
            tracer.log(
                hop.trace,
                cx.time(),
                hop.request_id,
                &format!("EdgeBalancer(api={}) -> server={global_idx}", self.api),
            );
        }
        self.outputs[global_idx]
            .send(ReplicaInput::Upstream(hop))
            .await;
    }

    pub async fn release(&mut self, server_idx: usize, _cx: &Context<Self>) {
        self.local_inflight[server_idx] = self.local_inflight[server_idx].saturating_sub(1);
    }
}

#[derive(Deserialize, Serialize)]
pub struct ReplicaBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_policy: LoadBalancePolicyKind,
    rb_id: usize,
    #[serde(skip)]
    microservice_id: String,
    #[serde(skip)]
    server_idx: usize,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    local_outbound_inflight: HashMap<String, Vec<u32>>,
    #[serde(skip)]
    downstream_indices: HashMap<String, Vec<usize>>,
    #[serde(skip)]
    outbound_scratch: Vec<u32>,
    #[serde(skip)]
    outbound_queues: HashMap<String, VecDeque<OutboundCall>>,
    #[serde(skip)]
    pull_intent_load: HashMap<String, Vec<u32>>,
    pub downstream_outputs: HashMap<String, Vec<Output<ReplicaInput>>>,
    pub pull_intent_outputs: HashMap<String, Vec<Output<PullIntent>>>,
    #[serde(skip)]
    pull_audit: Option<Arc<ApproxPullAudit>>,
    #[serde(skip)]
    no_bind: bool,
    #[serde(skip)]
    approx_sched: SchedulingPolicyKind,
}

impl ReplicaBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        rb_id: usize,
        microservice_id: String,
        server_idx: usize,
        downstream_indices: HashMap<String, Vec<usize>>,
        graph_server_counts: &HashMap<String, u32>,
        tracer: Option<Arc<MsTracer>>,
        pull_audit: Option<Arc<ApproxPullAudit>>,
        no_bind: bool,
        approx_sched: SchedulingPolicyKind,
    ) -> Self {
        let mut local_outbound_inflight = HashMap::new();
        let mut outbound_queues = HashMap::new();
        let mut pull_intent_load = HashMap::new();
        let mut pull_intent_outputs = HashMap::new();
        for (target, indices) in &downstream_indices {
            let n = graph_server_counts.get(target).copied().unwrap_or(0) as usize;
            debug_assert!(
                indices.iter().all(|&i| i < n),
                "downstream indices must be within target servers"
            );
            local_outbound_inflight.insert(target.clone(), vec![0; n]);
            outbound_queues.insert(target.clone(), VecDeque::new());
            pull_intent_load.insert(target.clone(), vec![0; n]);
            pull_intent_outputs.insert(target.clone(), (0..n).map(|_| Output::default()).collect());
        }
        let downstream_outputs = downstream_indices
            .keys()
            .cloned()
            .map(|ms| (ms, Vec::new()))
            .collect();
        Self {
            policy,
            lb_policy,
            rb_id,
            microservice_id,
            server_idx,
            tracer,
            local_outbound_inflight,
            downstream_indices,
            outbound_scratch: Vec::new(),
            outbound_queues,
            pull_intent_load,
            downstream_outputs,
            pull_intent_outputs,
            pull_audit,
            no_bind,
            approx_sched,
        }
    }

    fn enqueue_outbound_call(queue: &mut VecDeque<OutboundCall>, call: OutboundCall, approx_sched: SchedulingPolicyKind) {
        match approx_sched {
            SchedulingPolicyKind::Fifo => queue.push_back(call),
            SchedulingPolicyKind::Edf => {
                let deadline = call.hop.deadline;
                let insert_at = edf_insert_index(
                    queue.iter().map(|c| c.hop.deadline),
                    deadline,
                );
                queue.insert(insert_at, call);
            }
        }
    }

    fn release_pull_intent_load(&mut self, target: &str, server_idx: usize) {
        if let Some(loads) = self.pull_intent_load.get_mut(target) {
            if server_idx < loads.len() {
                loads[server_idx] = loads[server_idx].saturating_sub(1);
            }
        }
    }

    async fn send_pull_intent_for_target(
        &mut self,
        target: &str,
        request_id: u64,
        deadline: MonotonicTime,
    ) {
        let Some(indices) = self.downstream_indices.get(target) else {
            return;
        };
        if indices.is_empty() {
            return;
        }
        let pull_intent_load = self
            .pull_intent_load
            .get(target)
            .expect("missing pull intent load table");
        self.outbound_scratch.clear();
        self.outbound_scratch.extend(
            indices
                .iter()
                .map(|&i| pull_intent_load.get(i).copied().unwrap_or(0)),
        );
        let local_idx = self
            .policy
            .select(&self.outbound_scratch)
            .min(self.outbound_scratch.len().saturating_sub(1));
        let global_idx = indices[local_idx];
        if let Some(loads) = self.pull_intent_load.get_mut(target) {
            loads[global_idx] += 1;
        }
        if let Some(outputs) = self.pull_intent_outputs.get_mut(target) {
            if global_idx < outputs.len() {
                if let Some(audit) = &self.pull_audit {
                    audit.record_intent_sent(
                        self.rb_id,
                        &self.microservice_id,
                        self.server_idx,
                        target,
                        global_idx,
                        request_id,
                        deadline,
                    );
                }
                outputs[global_idx]
                    .send(PullIntent {
                        sender_id: self.rb_id,
                        request_id,
                    })
                    .await;
            }
        }
    }

    fn remove_call_for_pull(
        queue: &mut VecDeque<OutboundCall>,
        request_id: u64,
    ) -> Option<OutboundCall> {
        let idx = queue
            .iter()
            .position(|call| call.hop.request_id == request_id)?;
        queue.remove(idx)
    }

    async fn dispatch_to_server(&mut self, target: &str, server_idx: usize, mut call: OutboundCall) {
        if let Some(inflight) = self.local_outbound_inflight.get_mut(target) {
            inflight[server_idx] += 1;
        }

        if self.lb_policy.is_approx() {
            call.hop.slot_release = Some(OutboundRelease {
                target_microservice: target.to_string(),
                target_server: server_idx,
                response_time_ms: 0,
            });
        } else if let Some(caller) = &mut call.hop.caller {
            caller.outbound_target_microservice = target.to_string();
            caller.outbound_target_server = server_idx;
        }

        let Some(outputs) = self.downstream_outputs.get_mut(target) else {
            return;
        };
        outputs[server_idx]
            .send(ReplicaInput::Upstream(call.hop))
            .await;
    }
}

#[Model]
impl ReplicaBalancer {
    pub async fn outbound(&mut self, call: OutboundCall, cx: &Context<Self>) {
        let target = call.target_microservice.clone();
        let Some(indices) = self.downstream_indices.get(&target) else {
            eprintln!(
                "replica balancer outbound: unknown target microservice {}",
                target
            );
            return;
        };
        let n_outputs = self
            .downstream_outputs
            .get(&target)
            .map(|o| o.len())
            .unwrap_or(0);
        if indices.is_empty() || n_outputs == 0 {
            eprintln!(
                "replica balancer outbound: no servers for microservice {}",
                target
            );
            return;
        }

        if self.lb_policy.is_approx() {
            if let Some(tracer) = &self.tracer {
                tracer.log(
                    call.hop.trace,
                    cx.time(),
                    call.hop.request_id,
                    &format!(
                        "ReplicaBalancer({}/{}) approx enqueue target={target} queue={} endpoint={}",
                        self.microservice_id,
                        self.server_idx,
                        self.outbound_queues
                            .get(&target)
                            .map(|q| q.len() + 1)
                            .unwrap_or(1),
                        call.hop.endpoint
                    ),
                );
            }
            let request_id = call.hop.request_id;
            let deadline = call.hop.deadline;
            let queue = self
                .outbound_queues
                .entry(target.clone())
                .or_default();
            Self::enqueue_outbound_call(queue, call, self.approx_sched);
            self.send_pull_intent_for_target(&target, request_id, deadline).await;
            return;
        }

        let inflight = self
            .local_outbound_inflight
            .get(&target)
            .expect("missing outbound inflight table");
        self.outbound_scratch.clear();
        self.outbound_scratch.extend(
            indices
                .iter()
                .map(|&i| inflight.get(i).copied().unwrap_or(0)),
        );
        let local_idx = self
            .policy
            .select(&self.outbound_scratch)
            .min(self.outbound_scratch.len().saturating_sub(1));
        let global_idx = indices[local_idx];

        if let Some(tracer) = &self.tracer {
            tracer.log(
                call.hop.trace,
                cx.time(),
                call.hop.request_id,
                &format!(
                    "ReplicaBalancer({}/{}) outbound target={target} -> server={global_idx} endpoint={}",
                    self.microservice_id, self.server_idx, call.hop.endpoint
                ),
            );
        }

        let mut call = call;
        if let Some(caller) = &mut call.hop.caller {
            caller.outbound_target_microservice = target.clone();
            caller.outbound_target_server = global_idx;
        }

        if let Some(inflight) = self.local_outbound_inflight.get_mut(&target) {
            inflight[global_idx] += 1;
        }

        let Some(outputs) = self.downstream_outputs.get_mut(&target) else {
            return;
        };
        outputs[global_idx]
            .send(ReplicaInput::Upstream(call.hop))
            .await;
    }

    pub async fn pull(&mut self, pull: ReplicaPull, cx: &Context<Self>) {
        if !self.lb_policy.is_approx() {
            return;
        }
        let target = pull.target_microservice;
        let server_idx = pull.server_idx;

        let Some(queue) = self.outbound_queues.get_mut(&target) else {
            fatal_pull_abort(
                "ms",
                format!(
                    "unknown outbound queue (rb_id={}, microservice_id={}, server_idx={}, target={}, request_id={})",
                    self.rb_id, self.microservice_id, server_idx, target, pull.request_id
                ),
            );
        };

        let intent_request_id = pull.request_id;
        let queue_len_before = queue.len();
        let queue_head_request_id = queue.front().map(|c| c.hop.request_id);

        let call = if self.no_bind {
            if queue.is_empty() {
                fatal_pull_abort(
                    "ms",
                    format!(
                        "no queued outbound call for approx pull (rb_id={}, microservice_id={}, \
                         server_idx={}, target={}, ignored_request_id={}, queue_len=0)",
                        self.rb_id, self.microservice_id, server_idx, target, intent_request_id,
                    ),
                );
            }
            queue.pop_front().expect("queue non-empty")
        } else {
            let Some(call) = Self::remove_call_for_pull(queue, intent_request_id) else {
                let queued_request_ids: Vec<u64> = queue.iter().map(|c| c.hop.request_id).collect();
                fatal_pull_abort(
                    "ms",
                    format!(
                        "bound call not found (rb_id={}, microservice_id={}, server_idx={}, target={}, request_id={}, queue_len={}, queued_request_ids={:?})",
                        self.rb_id,
                        self.microservice_id,
                        server_idx,
                        target,
                        intent_request_id,
                        queue.len(),
                        queued_request_ids,
                    ),
                );
            };
            call
        };
        let pulled_request_id = call.hop.request_id;

        self.release_pull_intent_load(&target, server_idx);
        if let Some(audit) = &self.pull_audit {
            audit.record_pull_fulfilled(
                self.rb_id,
                &self.microservice_id,
                self.server_idx,
                &target,
                server_idx,
                intent_request_id,
                pulled_request_id,
                queue_len_before,
                queue_head_request_id,
            );
        }
        if let Some(tracer) = &self.tracer {
            tracer.log(
                call.hop.trace,
                cx.time(),
                call.hop.request_id,
                &format!(
                    "ReplicaBalancer({}/{}) approx dispatch target={target} -> server={server_idx} endpoint={}",
                    self.microservice_id, self.server_idx, call.hop.endpoint
                ),
            );
        }
        self.dispatch_to_server(&target, server_idx, call).await;
    }

    pub async fn release_outbound(&mut self, release: OutboundRelease, _cx: &Context<Self>) {
        if let Some(inflight) = self
            .local_outbound_inflight
            .get_mut(&release.target_microservice)
        {
            if release.target_server < inflight.len() {
                inflight[release.target_server] = inflight[release.target_server].saturating_sub(1);
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct DownstreamBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_policy: LoadBalancePolicyKind,
    target_microservice: String,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    #[serde(skip, default = "new_corr_histogram")]
    slack_histogram: Histogram<u64>,
    // Observed downstream response times (departure - arrival at replica), recorded on release.
    // Not used for routing yet. Future policy ideas:
    // - Compare incoming slack-d via response_time_histogram.quantile_below(sd_ms) instead of slack history
    // - Use value_at_quantile for latency prediction when picking among inflight ranks
    // - Replace slack_histogram entirely once response-time-based routing is defined
    #[serde(skip, default = "new_corr_histogram")]
    response_time_histogram: Histogram<u64>,
    #[serde(skip)]
    queue: VecDeque<OutboundCall>,
    #[serde(skip)]
    waiting_servers: VecDeque<usize>,
    #[serde(skip)]
    balancer_queue_occupancy: Arc<Mutex<HashMap<String, OccupancyAccumulator>>>,
    pub outputs: Vec<Output<ReplicaInput>>,
}

impl DownstreamBalancer {
    pub fn new(
        target_microservice: String,
        n_servers: usize,
        server_indices: Vec<usize>,
        lb_policy: LoadBalancePolicyKind,
        tracer: Option<Arc<MsTracer>>,
        balancer_queue_occupancy: Arc<Mutex<HashMap<String, OccupancyAccumulator>>>,
    ) -> Self {
        debug_assert!(
            server_indices.iter().all(|&i| i < n_servers),
            "server_indices must be within n_servers"
        );
        Self {
            policy: lb_policy.downstream_push_policy(),
            lb_policy,
            target_microservice,
            tracer,
            local_inflight: vec![0; n_servers],
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            slack_histogram: new_corr_histogram(),
            response_time_histogram: new_corr_histogram(),
            queue: VecDeque::new(),
            waiting_servers: VecDeque::new(),
            balancer_queue_occupancy,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }

    fn sample_queue_occupancy(&self, now: MonotonicTime) {
        if let Ok(mut occ) = self.balancer_queue_occupancy.lock() {
            occ.entry(self.target_microservice.clone())
                .or_default()
                .record(now, self.queue.len() as u32);
        }
    }

    async fn dispatch_to_server(&mut self, server_idx: usize, mut call: OutboundCall) {
        self.local_inflight[server_idx] += 1;

        if self.lb_policy.is_centralized() {
            call.hop.slot_release = Some(OutboundRelease {
                target_microservice: self.target_microservice.clone(),
                target_server: server_idx,
                response_time_ms: 0,
            });
        } else if let Some(caller) = &mut call.hop.caller {
            caller.outbound_target_microservice = self.target_microservice.clone();
            caller.outbound_target_server = server_idx;
        }

        self.outputs[server_idx]
            .send(ReplicaInput::Upstream(call.hop))
            .await;
    }

    async fn dispatch_waiting(&mut self, now: MonotonicTime) {
        while let Some(server_idx) = self.waiting_servers.pop_front() {
            if self.queue.is_empty() {
                self.waiting_servers.push_front(server_idx);
                break;
            }
            self.sample_queue_occupancy(now);
            let call = self.queue.pop_front().unwrap();
            self.sample_queue_occupancy(now);
            self.dispatch_to_server(server_idx, call).await;
        }
    }
}

#[Model]
impl DownstreamBalancer {
    pub async fn outbound(&mut self, call: OutboundCall, cx: &Context<Self>) {
        if call.target_microservice != self.target_microservice {
            eprintln!(
                "downstream balancer outbound: unexpected target {} (expected {})",
                call.target_microservice, self.target_microservice
            );
            return;
        }
        if self.server_indices.is_empty() || self.outputs.is_empty() {
            eprintln!(
                "downstream balancer outbound: no servers for microservice {}",
                self.target_microservice
            );
            return;
        }

        if self.lb_policy.is_centralized() {
            if let Some(tracer) = &self.tracer {
                tracer.log(
                    call.hop.trace,
                    cx.time(),
                    call.hop.request_id,
                    &format!(
                        "DownstreamBalancer(target={}, centralized) enqueue endpoint={} queue={}",
                        self.target_microservice,
                        call.hop.endpoint,
                        self.queue.len() + 1
                    ),
                );
            }
            self.sample_queue_occupancy(cx.time());
            self.queue.push_back(call);
            self.sample_queue_occupancy(cx.time());
            self.dispatch_waiting(cx.time()).await;
            return;
        }

        let global_idx = if self.lb_policy.is_corr() {
            let sd_ms = time_to_ms(call.hop.deadline) - time_to_ms(cx.time());
            let rank = corr_rank(&self.slack_histogram, &self.response_time_histogram, sd_ms);
            let idx = select_corr_replica(&self.server_indices, &self.local_inflight, rank);
            record_ms(&mut self.slack_histogram, sd_ms);
            idx
        } else {
            for (scratch, &server_idx) in
                self.load_scratch.iter_mut().zip(self.server_indices.iter())
            {
                *scratch = self.local_inflight[server_idx];
            }
            let local_idx = self
                .policy
                .select(&self.load_scratch)
                .min(self.load_scratch.len().saturating_sub(1));
            self.server_indices[local_idx]
        };

        if let Some(tracer) = &self.tracer {
            tracer.log(
                call.hop.trace,
                cx.time(),
                call.hop.request_id,
                &format!(
                    "DownstreamBalancer(target={}) -> server={global_idx} endpoint={}",
                    self.target_microservice, call.hop.endpoint
                ),
            );
        }

        self.dispatch_to_server(global_idx, call).await;
    }

    pub async fn pull(&mut self, server_idx: usize, cx: &Context<Self>) {
        if !self.lb_policy.is_centralized() {
            return;
        }
        if let Some(tracer) = &self.tracer {
            tracer.log(
                false,
                cx.time(),
                0,
                &format!(
                    "DownstreamBalancer(target={}, centralized) pull server={server_idx} queue={}",
                    self.target_microservice,
                    self.queue.len()
                ),
            );
        }
        if self.queue.is_empty() {
            self.waiting_servers.push_back(server_idx);
        } else {
            self.sample_queue_occupancy(cx.time());
            let call = self.queue.pop_front().unwrap();
            self.sample_queue_occupancy(cx.time());
            if let Some(tracer) = &self.tracer {
                tracer.log(
                    call.hop.trace,
                    cx.time(),
                    call.hop.request_id,
                    &format!(
                        "DownstreamBalancer(target={}, centralized) dispatch -> server={server_idx} endpoint={}",
                        self.target_microservice, call.hop.endpoint
                    ),
                );
            }
            self.dispatch_to_server(server_idx, call).await;
        }
    }

    pub async fn release(&mut self, release: OutboundRelease, _cx: &Context<Self>) {
        if release.target_microservice != self.target_microservice {
            return;
        }
        if release.target_server < self.local_inflight.len() {
            self.local_inflight[release.target_server] =
                self.local_inflight[release.target_server].saturating_sub(1);
        }
        if self.lb_policy.is_corr() && release.response_time_ms > 0 {
            record_ms(
                &mut self.response_time_histogram,
                release.response_time_ms as f64,
            );
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct OutboundGateway {
    #[serde(skip)]
    downstream_outputs: HashMap<String, Output<OutboundCall>>,
    #[serde(skip)]
    downstream_releases: HashMap<String, Output<OutboundRelease>>,
}

impl OutboundGateway {
    pub fn new(
        downstream_outputs: HashMap<String, Output<OutboundCall>>,
        downstream_releases: HashMap<String, Output<OutboundRelease>>,
    ) -> Self {
        Self {
            downstream_outputs,
            downstream_releases,
        }
    }
}

#[Model]
impl OutboundGateway {
    pub async fn input(&mut self, call: OutboundCall, _cx: &Context<Self>) {
        let target = call.target_microservice.clone();
        let Some(output) = self.downstream_outputs.get_mut(&target) else {
            eprintln!("outbound gateway: unknown target microservice {}", target);
            return;
        };
        output.send(call).await;
    }

    pub async fn release(&mut self, release: OutboundRelease, _cx: &Context<Self>) {
        let target = release.target_microservice.clone();
        let Some(output) = self.downstream_releases.get_mut(&target) else {
            return;
        };
        output.send(release).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::LoadBalancePolicyKind;
    use nexosim::time::MonotonicTime;
    use std::collections::HashMap;
    use std::time::Duration;

    fn test_rb(no_bind: bool) -> ReplicaBalancer {
        let mut downstream_indices = HashMap::new();
        downstream_indices.insert("backend1".to_string(), vec![0]);
        let mut graph_server_counts = HashMap::new();
        graph_server_counts.insert("backend1".to_string(), 1);
        ReplicaBalancer::new(
            Box::new(PowerOfTwoPolicy),
            LoadBalancePolicyKind::Approx,
            0,
            "frontend".to_string(),
            0,
            downstream_indices,
            &graph_server_counts,
            None,
            None,
            no_bind,
            SchedulingPolicyKind::Fifo,
        )
    }

    fn sample_call(request_id: u64) -> OutboundCall {
        OutboundCall {
            target_microservice: "backend1".to_string(),
            hop: Hop {
                request_id,
                trace: false,
                api: "handle".to_string(),
                endpoint: "handle".to_string(),
                sibling_index: 0,
                start: MonotonicTime::EPOCH,
                deadline: MonotonicTime::EPOCH,
                duration: Duration::ZERO,
                processing_time: Duration::ZERO,
                caller: None,
                outbound_release: None,
                slot_release: None,
            },
        }
    }

    #[test]
    fn no_bind_pull_takes_oldest_not_bound_id() {
        let mut rb = test_rb(true);
        rb.outbound_queues
            .get_mut("backend1")
            .unwrap()
            .push_back(sample_call(10));
        rb.outbound_queues
            .get_mut("backend1")
            .unwrap()
            .push_back(sample_call(20));

        let queue = rb.outbound_queues.get_mut("backend1").unwrap();
        let call = queue.pop_front().expect("queue non-empty");
        assert_eq!(call.hop.request_id, 10);
        assert_eq!(queue.front().unwrap().hop.request_id, 20);
        assert!(ReplicaBalancer::remove_call_for_pull(queue, 99).is_none());
    }

    #[test]
    fn no_bind_empty_queue_aborts() {
        let rb = test_rb(true);
        assert!(rb.outbound_queues.get("backend1").unwrap().is_empty());
    }
}
