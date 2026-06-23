use crate::policy::RandomPolicy;
use crate::policy::LoadBalancePolicy;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(RandomPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct LoadBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    #[serde(skip)]
    server_loads: Vec<Arc<AtomicU32>>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<Task>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        server_loads: Vec<Arc<AtomicU32>>,
        n_servers: usize,
    ) -> Self {
        Self {
            policy,
            server_loads,
            load_scratch: vec![0; n_servers],
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl LoadBalancer {
    pub async fn input(&mut self, task: Task, _cx: &Context<Self>) {
        for (scratch, load) in self.load_scratch.iter_mut().zip(self.server_loads.iter()) {
            *scratch = load.load(Ordering::Relaxed);
        }
        let idx = self.policy.select(&self.load_scratch);
        self.outputs[idx].send(task).await;
    }
}
