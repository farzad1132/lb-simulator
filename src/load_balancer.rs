use lb::policy::LoadBalancePolicy;
use lb::policy::LoadBalancePolicyKind;
use lb::policy::PowerOfTwoPolicy;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

fn default_policy() -> Box<dyn LoadBalancePolicy> {
    Box::new(PowerOfTwoPolicy)
}

#[derive(Deserialize, Serialize)]
pub struct LoadBalancer {
    #[serde(skip, default = "default_policy")]
    policy: Box<dyn LoadBalancePolicy>,
    lb_policy: LoadBalancePolicyKind,
    lb_id: usize,
    #[serde(skip)]
    local_inflight: Vec<u32>,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    #[serde(skip)]
    queue: Vec<Task>,
    #[serde(skip)]
    waiting_servers: VecDeque<usize>,
    #[serde(skip)]
    pull_intent_load: Vec<u32>,
    preserve_client_metadata: bool,
    pub outputs: Vec<Output<Task>>,
    pub pull_intent_outputs: Vec<Output<usize>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        n_servers: usize,
        server_indices: Vec<usize>,
        lb_id: usize,
        preserve_client_metadata: bool,
    ) -> Self {
        Self {
            policy,
            lb_policy,
            lb_id,
            local_inflight: vec![0; n_servers],
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            queue: Vec::new(),
            waiting_servers: VecDeque::new(),
            pull_intent_load: vec![0; n_servers],
            preserve_client_metadata,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
            pull_intent_outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }

    async fn dispatch_to_server(&mut self, server_idx: usize, mut task: Task) {
        self.local_inflight[server_idx] += 1;
        if !self.preserve_client_metadata {
            task.lb_id = self.lb_id;
            task.origin_server_idx = server_idx;
        }
        self.outputs[server_idx].send(task).await;
    }

    async fn dispatch_waiting(&mut self) {
        while let Some(server_idx) = self.waiting_servers.pop_front() {
            if self.queue.is_empty() {
                self.waiting_servers.push_front(server_idx);
                break;
            }
            let task = self.queue.remove(0);
            self.dispatch_to_server(server_idx, task).await;
        }
    }
}

#[Model]
impl LoadBalancer {
    pub async fn input(&mut self, mut task: Task, _cx: &Context<Self>) {
        if self.lb_policy.is_centralized() {
            self.queue.push(task);
            self.dispatch_waiting().await;
            return;
        }

        if self.lb_policy.is_approx() {
            self.queue.push(task);
            for (scratch, &server_idx) in
                self.load_scratch.iter_mut().zip(self.server_indices.iter())
            {
                *scratch = self.pull_intent_load[server_idx];
            }
            let local_idx = self.policy.select(&self.load_scratch);
            let global_idx = self.server_indices[local_idx];
            self.pull_intent_load[global_idx] += 1;
            self.pull_intent_outputs[global_idx]
                .send(self.lb_id)
                .await;
            return;
        }

        for (scratch, &server_idx) in self.load_scratch.iter_mut().zip(self.server_indices.iter()) {
            *scratch = self.local_inflight[server_idx];
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.server_indices[local_idx];
        self.local_inflight[global_idx] += 1;
        if !self.preserve_client_metadata {
            task.lb_id = self.lb_id;
            task.origin_server_idx = global_idx;
        }
        self.outputs[global_idx].send(task).await;
    }

    pub async fn pull(&mut self, server_idx: usize, _cx: &Context<Self>) {
        if self.lb_policy.is_approx() {
            if self.queue.is_empty() {
                return;
            }
            self.pull_intent_load[server_idx] =
                self.pull_intent_load[server_idx].saturating_sub(1);
            let task = self.queue.remove(0);
            self.dispatch_to_server(server_idx, task).await;
            return;
        }

        if self.queue.is_empty() {
            self.waiting_servers.push_back(server_idx);
        } else {
            let task = self.queue.remove(0);
            self.dispatch_to_server(server_idx, task).await;
        }
    }

    pub async fn release(&mut self, server_idx: usize, _cx: &Context<Self>) {
        self.local_inflight[server_idx] = self.local_inflight[server_idx].saturating_sub(1);
    }
}
