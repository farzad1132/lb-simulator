use lb::load_registry::LoadRegistry;
use crate::policy::LoadBalancePolicy;
use crate::policy::LoadBalancePolicyKind;
use crate::policy::PowerOfTwoPolicy;
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
    load_registry: LoadRegistry,
    #[serde(skip)]
    server_indices: Vec<usize>,
    #[serde(skip)]
    load_scratch: Vec<u32>,
    #[serde(skip)]
    queue: Vec<Task>,
    #[serde(skip)]
    waiting_servers: VecDeque<usize>,
    preserve_client_metadata: bool,
    pub outputs: Vec<Output<Task>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        n_servers: usize,
        server_indices: Vec<usize>,
        lb_id: usize,
        load_registry: LoadRegistry,
        preserve_client_metadata: bool,
    ) -> Self {
        Self {
            policy,
            lb_policy,
            lb_id,
            local_inflight: vec![0; n_servers],
            load_registry,
            load_scratch: vec![0; server_indices.len()],
            server_indices,
            queue: Vec::new(),
            waiting_servers: VecDeque::new(),
            preserve_client_metadata,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
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

        for (scratch, &server_idx) in self
            .load_scratch
            .iter_mut()
            .zip(self.server_indices.iter())
        {
            *scratch = if self.lb_policy.uses_true_load() {
                self.load_registry.get(server_idx)
            } else {
                self.local_inflight[server_idx]
            };
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
