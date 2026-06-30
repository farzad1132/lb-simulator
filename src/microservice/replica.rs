use super::callgraph::CallGraph;
use super::hop::{
    CallerRef, CompletedRequest, Hop, OutboundCall, OutboundRelease, ReplicaInput,
    filtered_children, sample_duration, service_for_endpoint,
};
use super::trace::MsTracer;
use crate::load_registry::LoadRegistry;
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
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
    pub service_id: String,
    pub replica_idx: usize,
    pub max_concurrency: u32,
    pub busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>>,
    pub balancer_outbound: Output<OutboundCall>,
    pub outbound_release: Output<OutboundRelease>,
    pub edge_releases: HashMap<String, Output<usize>>,
    pub return_outputs: HashMap<(String, usize), Output<ReplicaInput>>,
    pub completed: Output<CompletedRequest>,
    pub tracer: Option<Arc<MsTracer>>,
    pub load_registry: LoadRegistry,
}

#[derive(Deserialize, Serialize)]
pub struct Replica {
    pub outbound_release: Output<OutboundRelease>,
    pub edge_releases: HashMap<String, Output<usize>>,
    #[serde(skip)]
    graph: Arc<CallGraph>,
    #[serde(skip)]
    service_id: String,
    replica_idx: usize,
    max_concurrency: u32,
    in_flight: u32,
    #[serde(skip)]
    queue: VecDeque<ReplicaWork>,
    #[serde(skip)]
    busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>>,
    #[serde(skip)]
    balancer_outbound: Output<OutboundCall>,
    #[serde(skip)]
    return_outputs: HashMap<(String, usize), Output<ReplicaInput>>,
    #[serde(skip)]
    completed: Output<CompletedRequest>,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    load_registry: LoadRegistry,
}

impl Replica {
    pub fn new(config: ReplicaConfig) -> Self {
        Self {
            outbound_release: config.outbound_release,
            edge_releases: config.edge_releases,
            graph: config.graph,
            service_id: config.service_id,
            replica_idx: config.replica_idx,
            max_concurrency: config.max_concurrency.max(1),
            in_flight: 0,
            queue: VecDeque::new(),
            busy_time: config.busy_time,
            balancer_outbound: config.balancer_outbound,
            return_outputs: config.return_outputs,
            completed: config.completed,
            tracer: config.tracer,
            load_registry: config.load_registry,
        }
    }

    fn publish_load(&self) {
        self.load_registry
            .set(self.replica_idx, self.in_flight + self.queue.len() as u32);
    }

    fn trace(&self, hop: &Hop, cx: &Context<Self>, msg: &str) {
        if let Some(tracer) = &self.tracer {
            tracer.log(hop.trace, cx.time(), hop.request_id, msg);
        }
    }

    async fn release_outbound(&mut self, release: OutboundRelease) {
        self.outbound_release.send(release).await;
    }

    async fn release_edge(&mut self, api: &str) {
        if let Some(output) = self.edge_releases.get_mut(api) {
            output.send(self.replica_idx).await;
        }
    }

    fn begin_service(&mut self, mut hop: Hop, cx: &Context<Self>) {
        match sample_duration(&self.graph, &hop.endpoint) {
            Ok(duration) => hop.duration = duration,
            Err(e) => {
                eprintln!("sample_duration: {}", e);
                self.in_flight = self.in_flight.saturating_sub(1);
                self.publish_load();
                return;
            }
        }
        self.trace(
            &hop,
            cx,
            &format!(
                "Replica({}/{}) serve start endpoint={}",
                self.service_id, self.replica_idx, hop.endpoint
            ),
        );
        if let Err(h) = cx.schedule_event(hop.duration, schedulable!(Self::complete), hop) {
            eprintln!("could not schedule complete. err: {}", h);
            self.in_flight = self.in_flight.saturating_sub(1);
            self.publish_load();
        }
    }

    async fn drain_queue(&mut self, cx: &Context<Self>) {
        while !self.queue.is_empty() && self.in_flight < self.max_concurrency {
            let work = self.queue.pop_front().unwrap();
            match work {
                ReplicaWork::Upstream(hop) => {
                    self.in_flight += 1;
                    self.publish_load();
                    self.begin_service(hop, cx);
                    break;
                }
                ReplicaWork::DownstreamReturn(hop) => {
                    self.handle_return(hop, cx).await;
                }
            }
        }
    }

    async fn handle_return(&mut self, mut hop: Hop, cx: &Context<Self>) {
        if let Some(release) = hop.outbound_release.take() {
            self.release_outbound(release).await;
        }
        if let Err(e) = self.advance(hop, cx).await {
            eprintln!("advance on return failed: {}", e);
        }
    }

    async fn advance(&mut self, mut hop: Hop, cx: &Context<Self>) -> Result<(), String> {
        let children = filtered_children(&self.graph, &hop.endpoint, &hop.api);

        if hop.sibling_index < children.len() {
            let child_endpoint = &children[hop.sibling_index];
            let target_service = service_for_endpoint(&self.graph, child_endpoint)?;
            self.trace(
                &hop,
                cx,
                &format!(
                    "Replica({}/{}) dispatch child={child_endpoint} sibling={}",
                    self.service_id, self.replica_idx, hop.sibling_index
                ),
            );
            let mut child_hop = hop.clone();
            child_hop.endpoint = child_endpoint.clone();
            child_hop.sibling_index = 0;
            child_hop.duration = Duration::ZERO;
            child_hop.outbound_release = None;
            child_hop.caller = Some(CallerRef {
                service: self.service_id.clone(),
                replica: self.replica_idx,
                resume_endpoint: hop.endpoint.clone(),
                resume_sibling_index: hop.sibling_index + 1,
                resume_caller: hop.caller.clone().map(Box::new),
                outbound_target_service: String::new(),
                outbound_target_replica: 0,
            });
            self.balancer_outbound
                .send(OutboundCall {
                    target_service,
                    hop: child_hop,
                })
                .await;
            return Ok(());
        }

        if let Some(caller) = hop.caller.take() {
            let CallerRef {
                service,
                replica,
                resume_endpoint,
                resume_sibling_index,
                resume_caller,
                outbound_target_service,
                outbound_target_replica,
            } = caller;
            self.trace(
                &hop,
                cx,
                &format!(
                    "Replica({}/{}) return -> {service}/{replica} resume={resume_endpoint} sibling={resume_sibling_index}",
                    self.service_id, self.replica_idx
                ),
            );
            hop.endpoint = resume_endpoint;
            hop.sibling_index = resume_sibling_index;
            hop.caller = resume_caller.map(|b| *b);
            hop.outbound_release = Some(OutboundRelease {
                target_service: outbound_target_service,
                target_replica: outbound_target_replica,
            });
            let key = (service, replica);
            let output = self.return_outputs.get_mut(&key).ok_or_else(|| {
                format!("no return output for {:?} replica {}", key.0, key.1)
            })?;
            output.send(ReplicaInput::DownstreamReturn(hop)).await;
            return Ok(());
        }

        let e2e_ms = cx.time().duration_since(hop.start).as_secs_f64() * SECS_TO_MS;
        let proc_ms = hop.processing_time.as_secs_f64() * SECS_TO_MS;
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
        match &msg {
            ReplicaInput::Upstream(hop) => {
                self.trace(
                    hop,
                    cx,
                    &format!(
                        "Replica({}/{}) enqueue upstream endpoint={} queue={} inflight={}",
                        self.service_id,
                        self.replica_idx,
                        hop.endpoint,
                        self.queue.len() + 1,
                        self.in_flight
                    ),
                );
                self.queue.push_back(ReplicaWork::Upstream(hop.clone()));
            }
            ReplicaInput::DownstreamReturn(hop) => {
                self.trace(
                    hop,
                    cx,
                    &format!(
                        "Replica({}/{}) enqueue downstream_return endpoint={} queue={} inflight={}",
                        self.service_id,
                        self.replica_idx,
                        hop.endpoint,
                        self.queue.len() + 1,
                        self.in_flight
                    ),
                );
                self.queue
                    .push_back(ReplicaWork::DownstreamReturn(hop.clone()));
            }
        }
        self.publish_load();
        self.drain_queue(cx).await;
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut hop: Hop, cx: &Context<Self>) {
        if let Ok(mut busy) = self.busy_time.lock() {
            *busy
                .entry(self.service_id.clone())
                .or_default()
                .entry(self.replica_idx)
                .or_default() += hop.duration;
        }
        hop.processing_time += hop.duration;
        self.trace(
            &hop,
            cx,
            &format!(
                "Replica({}/{}) serve done endpoint={} duration_ms={:.3}",
                self.service_id,
                self.replica_idx,
                hop.endpoint,
                hop.duration.as_secs_f64() * SECS_TO_MS
            ),
        );
        self.in_flight = self.in_flight.saturating_sub(1);
        self.publish_load();

        hop.sibling_index = 0;
        if let Err(e) = self.advance(hop, cx).await {
            eprintln!("advance after local complete failed: {}", e);
        }
        self.drain_queue(cx).await;
    }
}
