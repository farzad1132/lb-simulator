use super::hop::{Hop, OutboundCall, OutboundRelease, ReplicaInput};
use super::trace::MsTracer;
use crate::load_registry::LoadRegistry;
use crate::policy::LoadBalancePolicy;
use crate::policy::LoadBalancePolicyKind;
use crate::policy::PowerOfTwoPolicy;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    load_registry: LoadRegistry,
    #[serde(skip)]
    replica_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<ReplicaInput>>,
}

impl EdgeBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        api: String,
        n_replicas: usize,
        replica_indices: Vec<usize>,
        load_registry: LoadRegistry,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        debug_assert!(
            replica_indices.iter().all(|&i| i < n_replicas),
            "replica_indices must be within n_replicas"
        );
        Self {
            policy,
            lb_policy,
            api,
            tracer,
            local_inflight: vec![0; n_replicas],
            load_registry,
            load_scratch: vec![0; replica_indices.len()],
            replica_indices,
            outputs: (0..n_replicas).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl EdgeBalancer {
    pub async fn input(&mut self, hop: Hop, cx: &Context<Self>) {
        for (scratch, &replica_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.replica_indices.iter())
        {
            *scratch = if self.lb_policy.uses_true_load() {
                self.load_registry.get(replica_idx)
            } else {
                self.local_inflight[replica_idx]
            };
        }
        let local_idx = self
            .policy
            .select(&self.load_scratch)
            .min(self.load_scratch.len().saturating_sub(1));
        let global_idx = self.replica_indices[local_idx];
        self.local_inflight[global_idx] += 1;
        if let Some(tracer) = &self.tracer {
            tracer.log(
                hop.trace,
                cx.time(),
                hop.request_id,
                &format!("EdgeBalancer(api={}) -> replica={global_idx}", self.api),
            );
        }
        self.outputs[global_idx]
            .send(ReplicaInput::Upstream(hop))
            .await;
    }

    pub async fn release(&mut self, replica_idx: usize, _cx: &Context<Self>) {
        self.local_inflight[replica_idx] = self.local_inflight[replica_idx].saturating_sub(1);
    }
}

#[derive(Deserialize, Serialize)]
pub struct ReplicaBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_policy: LoadBalancePolicyKind,
    #[serde(skip)]
    service_id: String,
    #[serde(skip)]
    replica_idx: usize,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    local_outbound_inflight: HashMap<String, Vec<u32>>,
    #[serde(skip)]
    downstream_loads: HashMap<String, LoadRegistry>,
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
        service_id: String,
        replica_idx: usize,
        downstream_indices: HashMap<String, Vec<usize>>,
        downstream_loads: HashMap<String, LoadRegistry>,
        graph_replicas: &HashMap<String, u32>,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        let mut local_outbound_inflight = HashMap::new();
        for (target, indices) in &downstream_indices {
            let n = graph_replicas
                .get(target)
                .copied()
                .unwrap_or(0) as usize;
            debug_assert!(
                indices.iter().all(|&i| i < n),
                "downstream indices must be within target replicas"
            );
            local_outbound_inflight.insert(target.clone(), vec![0; n]);
        }
        let downstream_outputs = downstream_indices
            .keys()
            .cloned()
            .map(|service| (service, Vec::new()))
            .collect();
        Self {
            policy,
            lb_policy,
            service_id,
            replica_idx,
            tracer,
            local_outbound_inflight,
            downstream_loads,
            downstream_indices,
            outbound_scratch: Vec::new(),
            downstream_outputs,
        }
    }
}

#[Model]
impl ReplicaBalancer {
    pub async fn outbound(&mut self, call: OutboundCall, cx: &Context<Self>) {
        let target = call.target_service.clone();
        let Some(indices) = self.downstream_indices.get(&target) else {
            eprintln!(
                "replica balancer outbound: unknown target service {}",
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
                "replica balancer outbound: no replicas for service {}",
                target
            );
            return;
        }

        let inflight = self
            .local_outbound_inflight
            .get(&target)
            .expect("missing outbound inflight table");
        let use_true_load = self.lb_policy.uses_true_load();
        let downstream_load = self.downstream_loads.get(&target);
        self.outbound_scratch.clear();
        self.outbound_scratch.extend(indices.iter().map(|&i| {
            if use_true_load {
                downstream_load.map(|r| r.get(i)).unwrap_or(0)
            } else {
                inflight.get(i).copied().unwrap_or(0)
            }
        }));
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
                    "ReplicaBalancer({}/{}) outbound target={target} -> replica={global_idx} endpoint={}",
                    self.service_id, self.replica_idx, call.hop.endpoint
                ),
            );
        }

        let mut call = call;
        if let Some(caller) = &mut call.hop.caller {
            caller.outbound_target_service = target.clone();
            caller.outbound_target_replica = global_idx;
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
        if let Some(inflight) = self.local_outbound_inflight.get_mut(&release.target_service) {
            if release.target_replica < inflight.len() {
                inflight[release.target_replica] =
                    inflight[release.target_replica].saturating_sub(1);
            }
        }
    }
}
