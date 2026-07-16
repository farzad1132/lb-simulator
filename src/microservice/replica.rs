use super::balancer::ReplicaPull;
use super::callgraph::CallGraph;
use super::hop::{
    CallerRef, CompletedRequest, Hop, OutboundCall, OutboundRelease, ReplicaInput,
    filtered_children, microservice_for_endpoint, sample_duration,
};
use super::microservice_stats::MicroserviceVisitTracker;
use super::occupancy::OccupancyAccumulator;
use super::trace::MsTracer;
use crate::approx::PullIntent;
use crate::approx_audit::ApproxPullAudit;
use crate::scheduling::{SchedulingPolicyKind, edf_insert_index};
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SECS_TO_MS: f64 = 1000.0;

#[derive(Clone, Debug)]
enum ReplicaWork {
    Upstream(Hop),
    DownstreamReturn(Hop),
}

pub struct ReplicaConfig {
    pub graph: Arc<CallGraph>,
    pub microservice_id: String,
    pub server_idx: usize,
    pub max_concurrency: u32,
    pub busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>>,
    pub replica_occupancy: Arc<Mutex<HashMap<String, HashMap<usize, OccupancyAccumulator>>>>,
    pub visit_tracker: Arc<Mutex<MicroserviceVisitTracker>>,
    pub balancer_outbound: Output<OutboundCall>,
    pub outbound_release: Output<OutboundRelease>,
    pub edge_releases: HashMap<String, Output<usize>>,
    pub return_outputs: HashMap<(String, usize), Output<ReplicaInput>>,
    pub completed: Output<CompletedRequest>,
    pub tracer: Option<Arc<MsTracer>>,
    pub pull_output: Option<Output<usize>>,
    pub approx_pull_outputs: HashMap<usize, Output<ReplicaPull>>,
    pub pull_audit: Option<Arc<ApproxPullAudit>>,
    pub scheduling: SchedulingPolicyKind,
}

#[derive(Deserialize, Serialize)]
pub struct Replica {
    pub outbound_release: Output<OutboundRelease>,
    pub edge_releases: HashMap<String, Output<usize>>,
    #[serde(skip)]
    graph: Arc<CallGraph>,
    #[serde(skip)]
    microservice_id: String,
    server_idx: usize,
    max_concurrency: u32,
    in_flight: u32,
    #[serde(skip)]
    pending_pulls: u32,
    #[serde(skip)]
    queue: VecDeque<ReplicaWork>,
    #[serde(skip)]
    busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>>,
    #[serde(skip)]
    replica_occupancy: Arc<Mutex<HashMap<String, HashMap<usize, OccupancyAccumulator>>>>,
    #[serde(skip)]
    visit_tracker: Arc<Mutex<MicroserviceVisitTracker>>,
    #[serde(skip)]
    balancer_outbound: Output<OutboundCall>,
    #[serde(skip)]
    return_outputs: HashMap<(String, usize), Output<ReplicaInput>>,
    #[serde(skip)]
    completed: Output<CompletedRequest>,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    pull_output: Option<Output<usize>>,
    #[serde(skip)]
    approx_pull_outputs: HashMap<usize, Output<ReplicaPull>>,
    #[serde(skip)]
    pull_intent_queue: VecDeque<PullIntent>,
    #[serde(skip)]
    pull_audit: Option<Arc<ApproxPullAudit>>,
    #[serde(skip)]
    scheduling: SchedulingPolicyKind,
}

impl Replica {
    pub fn new(config: ReplicaConfig) -> Self {
        Self {
            outbound_release: config.outbound_release,
            edge_releases: config.edge_releases,
            graph: config.graph,
            microservice_id: config.microservice_id,
            server_idx: config.server_idx,
            max_concurrency: config.max_concurrency.max(1),
            in_flight: 0,
            pending_pulls: 0,
            queue: VecDeque::new(),
            busy_time: config.busy_time,
            replica_occupancy: config.replica_occupancy,
            visit_tracker: config.visit_tracker,
            balancer_outbound: config.balancer_outbound,
            return_outputs: config.return_outputs,
            completed: config.completed,
            tracer: config.tracer,
            pull_output: config.pull_output,
            approx_pull_outputs: config.approx_pull_outputs,
            pull_intent_queue: VecDeque::new(),
            pull_audit: config.pull_audit,
            scheduling: config.scheduling,
        }
    }

    async fn drain_pull_intents_async(&mut self) {
        if self.in_flight + self.pending_pulls >= self.max_concurrency {
            return;
        }
        let queue_len_before = self.pull_intent_queue.len();
        let pending_pulls_before = self.pending_pulls;
        let in_flight_before = self.in_flight;
        let Some(intent) = self.pull_intent_queue.pop_front() else {
            return;
        };
        if let Some(audit) = &self.pull_audit {
            audit.record_intent_drained(
                &self.microservice_id,
                self.server_idx,
                intent.sender_id,
                intent.request_id,
                queue_len_before,
                pending_pulls_before,
                in_flight_before,
                self.max_concurrency,
            );
        }
        self.pending_pulls += 1;
        if let Some(output) = self.approx_pull_outputs.get_mut(&intent.sender_id) {
            output
                .send(ReplicaPull {
                    target_microservice: self.microservice_id.clone(),
                    server_idx: self.server_idx,
                    request_id: intent.request_id,
                })
                .await;
        } else {
            self.pending_pulls = self.pending_pulls.saturating_sub(1);
        }
    }

    fn occupancy_level(&self) -> u32 {
        self.queue.len() as u32 + self.in_flight
    }

    fn sample_occupancy(&self, now: MonotonicTime) {
        if let Ok(mut occ) = self.replica_occupancy.lock() {
            occ.entry(self.microservice_id.clone())
                .or_default()
                .entry(self.server_idx)
                .or_default()
                .record(now, self.occupancy_level());
        }
    }

    fn enqueue_work(&mut self, work: ReplicaWork) {
        match self.scheduling {
            SchedulingPolicyKind::Fifo => {
                self.queue.push_back(work);
            }
            SchedulingPolicyKind::Edf => {
                let deadline = match &work {
                    ReplicaWork::Upstream(h) | ReplicaWork::DownstreamReturn(h) => h.deadline,
                };
                let insert_at = edf_insert_index(
                    self.queue.iter().map(|w| match w {
                        ReplicaWork::Upstream(h) | ReplicaWork::DownstreamReturn(h) => h.deadline,
                    }),
                    deadline,
                );
                self.queue.insert(insert_at, work);
            }
        }
    }

    fn trace(&self, hop: &Hop, cx: &Context<Self>, msg: &str) {
        if let Some(tracer) = &self.tracer {
            tracer.log(hop.trace, cx.time(), hop.request_id, msg);
        }
    }

    fn finalize_visit(
        &self,
        request_id: u64,
        departure: nexosim::time::MonotonicTime,
    ) -> Option<f64> {
        if let Ok(mut tracker) = self.visit_tracker.lock() {
            tracker.finalize_visit(request_id, &self.microservice_id, departure)
        } else {
            None
        }
    }

    async fn release_outbound(&mut self, release: OutboundRelease) {
        self.outbound_release.send(release).await;
    }

    async fn release_edge(&mut self, api: &str) {
        if let Some(output) = self.edge_releases.get_mut(api) {
            output.send(self.server_idx).await;
        }
    }

    fn begin_service(&mut self, mut hop: Hop, cx: &Context<Self>) {
        match sample_duration(&self.graph, &hop.endpoint) {
            Ok(duration) => hop.duration = duration,
            Err(e) => {
                eprintln!("sample_duration: {}", e);
                self.in_flight = self.in_flight.saturating_sub(1);
                self.sample_occupancy(cx.time());
                return;
            }
        }
        self.trace(
            &hop,
            cx,
            &format!(
                "Server({}/{}) serve start endpoint={}",
                self.microservice_id, self.server_idx, hop.endpoint
            ),
        );
        if let Err(h) = cx.schedule_event(hop.duration, schedulable!(Self::complete), hop) {
            eprintln!("could not schedule complete. err: {}", h);
            self.in_flight = self.in_flight.saturating_sub(1);
            self.sample_occupancy(cx.time());
        }
    }

    async fn drain_queue(&mut self, cx: &Context<Self>) {
        while !self.queue.is_empty() && self.in_flight < self.max_concurrency {
            let work = self.queue.pop_front().unwrap();
            self.sample_occupancy(cx.time());
            match work {
                ReplicaWork::Upstream(hop) => {
                    self.in_flight += 1;
                    self.sample_occupancy(cx.time());
                    self.begin_service(hop, cx);
                    break;
                }
                ReplicaWork::DownstreamReturn(hop) => {
                    self.handle_return(hop, cx).await;
                    self.sample_occupancy(cx.time());
                }
            }
        }
    }

    async fn handle_return(&mut self, mut hop: Hop, cx: &Context<Self>) {
        if let Some(release) = hop.outbound_release.take() {
            if release.response_time_ms > 0 {
                if let Ok(mut tracker) = self.visit_tracker.lock() {
                    tracker.add_downstream_response(
                        hop.request_id,
                        &self.microservice_id,
                        release.response_time_ms as f64,
                    );
                }
            }
            if !release.target_microservice.is_empty() {
                self.release_outbound(release).await;
            }
        }
        if let Err(e) = self.advance(hop, cx).await {
            eprintln!("advance on return failed: {}", e);
        }
    }

    async fn advance(&mut self, mut hop: Hop, cx: &Context<Self>) -> Result<(), String> {
        let children = filtered_children(&self.graph, &hop.endpoint, &hop.api);

        if hop.sibling_index < children.len() {
            let child_endpoint = &children[hop.sibling_index];
            let target_microservice = microservice_for_endpoint(&self.graph, child_endpoint)?;
            self.trace(
                &hop,
                cx,
                &format!(
                    "Server({}/{}) dispatch child={child_endpoint} sibling={}",
                    self.microservice_id, self.server_idx, hop.sibling_index
                ),
            );
            let mut child_hop = hop.clone();
            child_hop.endpoint = child_endpoint.clone();
            child_hop.sibling_index = 0;
            child_hop.duration = Duration::ZERO;
            child_hop.outbound_release = None;
            child_hop.slot_release = None;
            child_hop.caller = Some(CallerRef {
                microservice: self.microservice_id.clone(),
                server: self.server_idx,
                resume_endpoint: hop.endpoint.clone(),
                resume_sibling_index: hop.sibling_index + 1,
                resume_caller: hop.caller.clone().map(Box::new),
                outbound_target_microservice: String::new(),
                outbound_target_server: 0,
            });
            self.balancer_outbound
                .send(OutboundCall {
                    target_microservice,
                    hop: child_hop,
                })
                .await;
            return Ok(());
        }

        if let Some(caller) = hop.caller.take() {
            let CallerRef {
                microservice,
                server,
                resume_endpoint,
                resume_sibling_index,
                resume_caller,
                outbound_target_microservice,
                outbound_target_server,
            } = caller;
            let response_ms = self.finalize_visit(hop.request_id, cx.time());
            self.trace(
                &hop,
                cx,
                &format!(
                    "Server({}/{}) return -> {microservice}/{server} resume={resume_endpoint} sibling={resume_sibling_index}",
                    self.microservice_id, self.server_idx
                ),
            );
            hop.endpoint = resume_endpoint;
            hop.sibling_index = resume_sibling_index;
            hop.caller = resume_caller.map(|b| *b);
            let child_response_ms = response_ms
                .map(|ms| ms.round().max(0.0) as u64)
                .unwrap_or(0);
            if child_response_ms > 0 || !outbound_target_microservice.is_empty() {
                hop.outbound_release = Some(OutboundRelease {
                    target_microservice: outbound_target_microservice,
                    target_server: outbound_target_server,
                    response_time_ms: child_response_ms,
                });
            }
            let key = (microservice, server);
            let output = self.return_outputs.get_mut(&key).ok_or_else(|| {
                format!("no return output for {:?} server {}", key.0, key.1)
            })?;
            output.send(ReplicaInput::DownstreamReturn(hop)).await;
            return Ok(());
        }

        let e2e_ms = cx.time().duration_since(hop.start).as_secs_f64() * SECS_TO_MS;
        let proc_ms = hop.processing_time.as_secs_f64() * SECS_TO_MS;
        self.finalize_visit(hop.request_id, cx.time());
        self.trace(
            &hop,
            cx,
            &format!(
                "UserArrival complete api={} e2e_ms={e2e_ms:.2} proc_ms={proc_ms:.2}",
                hop.api
            ),
        );
        self.release_edge(&hop.api).await;
        self.completed
            .send(CompletedRequest {
                request_id: hop.request_id,
                trace: hop.trace,
                api: hop.api,
                start: hop.start,
                finish: cx.time(),
                processing_time: hop.processing_time,
            })
            .await;
        Ok(())
    }
}

#[Model]
impl Replica {
    pub async fn input(&mut self, msg: ReplicaInput, cx: &Context<Self>) {
        match msg {
            ReplicaInput::Upstream(hop) => {
                if let Ok(mut tracker) = self.visit_tracker.lock() {
                    tracker.record_arrival(
                        hop.request_id,
                        &self.microservice_id,
                        cx.time(),
                        hop.deadline,
                    );
                }
                if hop.slot_release.is_some() {
                    self.trace(
                        &hop,
                        cx,
                        &format!(
                            "Server({}/{}) approx upstream endpoint={} inflight={}",
                            self.microservice_id,
                            self.server_idx,
                            hop.endpoint,
                            self.in_flight
                        ),
                    );
                    if !self.approx_pull_outputs.is_empty() {
                        self.pending_pulls = self.pending_pulls.saturating_sub(1);
                    }
                    self.in_flight += 1;
                    self.sample_occupancy(cx.time());
                    self.begin_service(hop, cx);
                    return;
                }
                self.trace(
                    &hop,
                    cx,
                    &format!(
                        "Server({}/{}) enqueue upstream endpoint={} queue={} inflight={}",
                        self.microservice_id,
                        self.server_idx,
                        hop.endpoint,
                        self.queue.len() + 1,
                        self.in_flight
                    ),
                );
                self.enqueue_work(ReplicaWork::Upstream(hop));
                self.sample_occupancy(cx.time());
            }
            ReplicaInput::DownstreamReturn(hop) => {
                self.trace(
                    &hop,
                    cx,
                    &format!(
                        "Server({}/{}) enqueue downstream_return endpoint={} queue={} inflight={}",
                        self.microservice_id,
                        self.server_idx,
                        hop.endpoint,
                        self.queue.len() + 1,
                        self.in_flight
                    ),
                );
                self.enqueue_work(ReplicaWork::DownstreamReturn(hop));
                self.sample_occupancy(cx.time());
            }
        }
        self.drain_queue(cx).await;
        self.sample_occupancy(cx.time());
    }

    pub async fn request_pull(&mut self, _: (), _cx: &Context<Self>) {
        if self.pull_output.is_some() && self.in_flight < self.max_concurrency {
            if let Some(output) = &mut self.pull_output {
                output.send(self.server_idx).await;
            }
        }
    }

    pub async fn receive_pull_intent(&mut self, intent: PullIntent, _cx: &Context<Self>) {
        if self.approx_pull_outputs.is_empty() {
            return;
        }
        let queue_len_before = self.pull_intent_queue.len();
        if let Some(audit) = &self.pull_audit {
            audit.record_intent_queued(
                &self.microservice_id,
                self.server_idx,
                intent.sender_id,
                intent.request_id,
                queue_len_before,
            );
        }
        self.pull_intent_queue.push_back(intent);
        self.drain_pull_intents_async().await;
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut hop: Hop, cx: &Context<Self>) {
        if let Ok(mut busy) = self.busy_time.lock() {
            *busy
                .entry(self.microservice_id.clone())
                .or_default()
                .entry(self.server_idx)
                .or_default() += hop.duration;
        }
        if let Ok(mut tracker) = self.visit_tracker.lock() {
            tracker.add_local_processing(hop.request_id, &self.microservice_id, hop.duration);
        }
        hop.processing_time += hop.duration;
        self.trace(
            &hop,
            cx,
            &format!(
                "Server({}/{}) serve done endpoint={} duration_ms={:.3}",
                self.microservice_id,
                self.server_idx,
                hop.endpoint,
                hop.duration.as_secs_f64() * SECS_TO_MS
            ),
        );
        self.in_flight = self.in_flight.saturating_sub(1);
        self.sample_occupancy(cx.time());

        if let Some(release) = hop.slot_release.take() {
            self.release_outbound(release).await;
            if self.pull_output.is_some() {
                if let Some(output) = &mut self.pull_output {
                    output.send(self.server_idx).await;
                }
            }
            if !self.approx_pull_outputs.is_empty() {
                self.drain_pull_intents_async().await;
            }
        }

        hop.sibling_index = 0;
        if let Err(e) = self.advance(hop, cx).await {
            eprintln!("advance after local complete failed: {}", e);
        }
        self.drain_queue(cx).await;
        self.sample_occupancy(cx.time());
    }
}
