use crate::policy::LoadBalancePolicy;
use crate::policy::PowerOfTwoPolicy;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct LoadBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_id: usize,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    pub outputs: Vec<Output<Task>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        n_servers: usize,
        server_indices: Vec<usize>,
        lb_id: usize,
    ) -> Self {
        Self {
            policy,
            lb_id,
            local_inflight: vec![0; n_servers],
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }
}

#[Model]
impl LoadBalancer {
    pub async fn input(&mut self, mut task: Task, _cx: &Context<Self>) {
        for (scratch, &server_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.server_indices.iter())
        {
            *scratch = self.local_inflight[server_idx];
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.server_indices[local_idx];
        self.local_inflight[global_idx] += 1;
        task.lb_id = self.lb_id;
        self.outputs[global_idx].send(task).await;
    }

    pub async fn release(&mut self, server_idx: usize, _cx: &Context<Self>) {
        self.local_inflight[server_idx] = self.local_inflight[server_idx].saturating_sub(1);
    }
}
