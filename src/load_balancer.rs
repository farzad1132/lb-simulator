use crate::policy::LoadBalancePolicy;
use crate::policy::PowerOfTwoPolicy;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct LoadBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    #[serde(skip)]
    server_loads: Vec<Arc<AtomicU32>>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<Task>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        server_loads: Vec<Arc<AtomicU32>>,
        server_indices: Vec<usize>,
    ) -> Self {
        let n_servers = server_loads.len();
        Self {
            policy,
            server_loads,
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl LoadBalancer {
    pub async fn input(&mut self, task: Task, _cx: &Context<Self>) {
        for (scratch, &server_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.server_indices.iter())
        {
            *scratch = self.server_loads[server_idx].load(Ordering::Relaxed);
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.server_indices[local_idx];
        self.outputs[global_idx].send(task).await;
    }
}
