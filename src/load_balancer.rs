use crate::policy::RandomPolicy;
use crate::policy::LoadBalancePolicy;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(RandomPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct LoadBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    pub outputs: Vec<Output<Task>>,
}

impl LoadBalancer {
    pub fn new(policy: Box<dyn LoadBalancePolicy>, n_servers: usize) -> Self {
        Self {
            policy,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl LoadBalancer {
    pub async fn input(&mut self, task: Task, _cx: &Context<Self>) {
        let idx = self.policy.select(self.outputs.len());
        self.outputs[idx].send(task).await;
    }
}
