use super::hop::Hop;
use crate::policy::LoadBalancePolicy;
use crate::policy::PowerOfTwoPolicy;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct Balancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    #[serde(skip)]
    replica_loads: Vec<Arc<AtomicU32>>,
    #[serde(skip)]
    replica_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<Hop>>,
}

impl Balancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        replica_loads: Vec<Arc<AtomicU32>>,
        replica_indices: Vec<usize>,
    ) -> Self {
        let n_replicas = replica_loads.len();
        Self {
            policy,
            replica_loads,
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
            *scratch = self.replica_loads[replica_idx].load(Ordering::Relaxed);
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.replica_indices[local_idx];
        self.outputs[global_idx].send(hop).await;
    }
}
