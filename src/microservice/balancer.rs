use super::hop::{Hop, OutboundCall, OutboundRelease, ReplicaInput};
use super::trace::MsTracer;
use crate::policy::LoadBalancePolicy;
use crate::policy::LoadBalancePolicyKind;
use crate::policy::PowerOfTwoPolicy;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
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
        for (scratch, &server_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.server_indices.iter())
        {
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
    pub downstream_outputs: HashMap<String, Vec<Output<ReplicaInput>>>,
}

impl ReplicaBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        microservice_id: String,
        server_idx: usize,
        downstream_indices: HashMap<String, Vec<usize>>,
        graph_server_counts: &HashMap<String, u32>,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        let mut local_outbound_inflight = HashMap::new();
        for (target, indices) in &downstream_indices {
            let n = graph_server_counts
                .get(target)
                .copied()
                .unwrap_or(0) as usize;
            debug_assert!(
                indices.iter().all(|&i| i < n),
                "downstream indices must be within target servers"
            );
            local_outbound_inflight.insert(target.clone(), vec![0; n]);
        }
        let downstream_outputs = downstream_indices
            .keys()
            .cloned()
            .map(|ms| (ms, Vec::new()))
            .collect();
        Self {
            policy,
            lb_policy,
            microservice_id,
            server_idx,
            tracer,
            local_outbound_inflight,
            downstream_indices,
            outbound_scratch: Vec::new(),
            downstream_outputs,
        }
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

    pub async fn release_outbound(&mut self, release: OutboundRelease, _cx: &Context<Self>) {
        if let Some(inflight) = self
            .local_outbound_inflight
            .get_mut(&release.target_microservice)
        {
            if release.target_server < inflight.len() {
                inflight[release.target_server] =
                    inflight[release.target_server].saturating_sub(1);
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct DownstreamBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    centralized: bool,
    target_microservice: String,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    #[serde(skip)]
    queue: VecDeque<OutboundCall>,
    #[serde(skip)]
    waiting_servers: VecDeque<usize>,
    pub outputs: Vec<Output<ReplicaInput>>,
}

impl DownstreamBalancer {
    pub fn new(
        target_microservice: String,
        n_servers: usize,
        server_indices: Vec<usize>,
        centralized: bool,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        debug_assert!(
            server_indices.iter().all(|&i| i < n_servers),
            "server_indices must be within n_servers"
        );
        Self {
            policy: Box::new(PowerOfTwoPolicy),
            centralized,
            target_microservice,
            tracer,
            local_inflight: vec![0; n_servers],
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            queue: VecDeque::new(),
            waiting_servers: VecDeque::new(),
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }

    async fn dispatch_to_server(&mut self, server_idx: usize, mut call: OutboundCall) {
        self.local_inflight[server_idx] += 1;

        if self.centralized {
            call.hop.slot_release = Some(OutboundRelease {
                target_microservice: self.target_microservice.clone(),
                target_server: server_idx,
            });
        } else if let Some(caller) = &mut call.hop.caller {
            caller.outbound_target_microservice = self.target_microservice.clone();
            caller.outbound_target_server = server_idx;
        }

        self.outputs[server_idx]
            .send(ReplicaInput::Upstream(call.hop))
            .await;
    }

    async fn dispatch_waiting(&mut self) {
        while let Some(server_idx) = self.waiting_servers.pop_front() {
            if self.queue.is_empty() {
                self.waiting_servers.push_front(server_idx);
                break;
            }
            let call = self.queue.pop_front().unwrap();
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

        if self.centralized {
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
            self.queue.push_back(call);
            self.dispatch_waiting().await;
            return;
        }

        for (scratch, &server_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.server_indices.iter())
        {
            *scratch = self.local_inflight[server_idx];
        }
        let local_idx = self
            .policy
            .select(&self.load_scratch)
            .min(self.load_scratch.len().saturating_sub(1));
        let global_idx = self.server_indices[local_idx];

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
        if !self.centralized {
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
            let call = self.queue.pop_front().unwrap();
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
            eprintln!(
                "outbound gateway: unknown target microservice {}",
                target
            );
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
