use super::hop::Hop;
use crate::policy::LoadBalancePolicy;
use crate::policy::LeastRequestPolicy;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(LeastRequestPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct Balancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    replica_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<Hop>>,
}

impl Balancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        n_replicas: usize,
        replica_indices: Vec<usize>,
    ) -> Self {
        Self {
            policy,
            local_inflight: vec![0; n_replicas],
            load_scratch: vec![0; replica_indices.len()],
            replica_indices,
            outputs: (0..n_replicas).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl Balancer {
    pub async fn input(&mut self, hop: Hop, _cx: &Context<Self>) {
        for (scratch, &replica_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.replica_indices.iter())
        {
            *scratch = self.local_inflight[replica_idx];
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.replica_indices[local_idx];
        self.local_inflight[global_idx] += 1;
        self.outputs[global_idx].send(hop).await;
    }

    pub async fn release(&mut self, replica_idx: usize, _cx: &Context<Self>) {
        self.local_inflight[replica_idx] = self.local_inflight[replica_idx].saturating_sub(1);
    }
}
