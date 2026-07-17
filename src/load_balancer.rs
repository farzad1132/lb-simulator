use crate::approx::{fatal_pull_abort, PullIntent, PullRequest};
use crate::lb_pull_audit::LbPullAudit;
use crate::policy::ApproxSchedKind;
use crate::policy::LoadBalancePolicy;
use crate::policy::LoadBalancePolicyKind;
use crate::policy::PowerOfTwoPolicy;
use crate::prequal::{
    apply_r_remove, pool_cap, sample_probe_targets, CandidatePool, Probe, ProbeReply, B_REUSE,
    R_PROBE, R_REMOVE,
};
use crate::rng;
use crate::server::Task;
use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

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
    #[serde(skip)]
    next_task_id: u64,
    preserve_client_metadata: bool,
    approx_sched: Option<ApproxSchedKind>,
    #[serde(skip)]
    pull_audit: Option<Arc<LbPullAudit>>,
    #[serde(skip)]
    candidate_pool: CandidatePool,
    #[serde(skip)]
    r_remove_accum: f64,
    n_servers: usize,
    pub outputs: Vec<Output<Task>>,
    pub pull_intent_outputs: Vec<Output<PullIntent>>,
    pub probe_outputs: Vec<Output<Probe>>,
}

impl LoadBalancer {
    pub fn new(
        policy: Box<dyn LoadBalancePolicy>,
        lb_policy: LoadBalancePolicyKind,
        n_servers: usize,
        server_indices: Vec<usize>,
        lb_id: usize,
        preserve_client_metadata: bool,
        approx_sched: Option<ApproxSchedKind>,
        pull_audit: Option<Arc<LbPullAudit>>,
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
            next_task_id: 0,
            preserve_client_metadata,
            approx_sched,
            pull_audit,
            candidate_pool: CandidatePool::new(pool_cap(n_servers)),
            r_remove_accum: 0.0,
            n_servers,
            outputs: (0..n_servers).map(|_| Output::default()).collect(),
            pull_intent_outputs: (0..n_servers).map(|_| Output::default()).collect(),
            probe_outputs: (0..n_servers).map(|_| Output::default()).collect(),
        }
    }

    async fn issue_probes(&mut self) {
        let targets = sample_probe_targets(self.n_servers, &self.candidate_pool, R_PROBE);
        for server_idx in targets {
            self.probe_outputs[server_idx]
                .send(Probe {
                    sender_id: self.lb_id,
                })
                .await;
        }
    }

    async fn dispatch_prequal(&mut self, mut task: Task) {
        apply_r_remove(&mut self.candidate_pool, &mut self.r_remove_accum, R_REMOVE);

        let global_idx = if let Some(server_idx) = self.candidate_pool.select_best() {
            server_idx
        } else if self.n_servers == 0 {
            return;
        } else {
            rng::random_usize_range(0..self.n_servers)
        };

        self.local_inflight[global_idx] += 1;
        if !self.preserve_client_metadata {
            task.lb_id = self.lb_id;
            task.origin_server_idx = global_idx;
        }
        self.outputs[global_idx].send(task).await;

        self.candidate_pool.after_dispatch(global_idx, B_REUSE);
        self.issue_probes().await;
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

    fn remove_task_for_pull(&mut self, request_id: u64) -> Option<Task> {
        let idx = self
            .queue
            .iter()
            .position(|task| task.task_id == request_id)?;
        Some(self.queue.remove(idx))
    }

    async fn send_pull_intent(&mut self, request_id: u64) {
        for (scratch, &server_idx) in self.load_scratch.iter_mut().zip(self.server_indices.iter()) {
            *scratch = self.pull_intent_load[server_idx];
        }
        let local_idx = self.policy.select(&self.load_scratch);
        let global_idx = self.server_indices[local_idx];
        self.pull_intent_load[global_idx] += 1;
        if let Some(audit) = &self.pull_audit {
            audit.record_intent_sent(self.lb_id, global_idx, request_id);
        }
        self.pull_intent_outputs[global_idx]
            .send(PullIntent {
                sender_id: self.lb_id,
                request_id,
            })
            .await;
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
            task.task_id = self.next_task_id;
            self.next_task_id += 1;
            let request_id = task.task_id;
            let queue_len_before = self.queue.len();
            if let Some(audit) = &self.pull_audit {
                audit.record_task_enqueued(self.lb_id, request_id, queue_len_before);
            }
            self.queue.push(task);
            self.send_pull_intent(request_id).await;
            return;
        }

        if self.lb_policy.is_prequal() {
            self.dispatch_prequal(task).await;
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

    pub async fn pull(&mut self, pull: PullRequest, _cx: &Context<Self>) {
        if self.lb_policy.is_approx() {
            let server_idx = pull.server_idx;
            if self.approx_sched.is_some() {
                if self.queue.is_empty() {
                    fatal_pull_abort(
                        "lb",
                        format!(
                            "no queued task for approx pull (lb_id={}, server_idx={}, \
                             ignored_request_id={:?}, queue_len=0, pull_intent_load={}, \
                             queued_task_ids=[])",
                            self.lb_id,
                            server_idx,
                            pull.request_id,
                            self.pull_intent_load[server_idx],
                        ),
                    );
                }
                let queue_len_before = self.queue.len();
                let queue_head_task_id = self.queue.first().map(|t| t.task_id);
                let task = self.queue.remove(0);
                let pulled_task_id = task.task_id;
                if let Some(audit) = &self.pull_audit {
                    audit.record_pull_fulfilled(
                        self.lb_id,
                        server_idx,
                        pull.request_id,
                        pulled_task_id,
                        queue_len_before,
                        queue_head_task_id,
                    );
                }
                self.pull_intent_load[server_idx] =
                    self.pull_intent_load[server_idx].saturating_sub(1);
                self.dispatch_to_server(server_idx, task).await;
                return;
            }

            let Some(request_id) = pull.request_id else {
                fatal_pull_abort(
                    "lb",
                    format!(
                        "missing request_id on approx pull (lb_id={}, server_idx={})",
                        self.lb_id, server_idx
                    ),
                );
            };
            let queue_len_before = self.queue.len();
            let queue_head_task_id = self.queue.first().map(|t| t.task_id);
            let Some(task) = self.remove_task_for_pull(request_id) else {
                let queued_task_ids: Vec<u64> = self.queue.iter().map(|t| t.task_id).collect();
                fatal_pull_abort(
                    "lb",
                    format!(
                        "bound task not found (lb_id={}, server_idx={}, request_id={}, queue_len={}, queued_task_ids={:?})",
                        self.lb_id,
                        server_idx,
                        request_id,
                        self.queue.len(),
                        queued_task_ids,
                    ),
                );
            };
            let pulled_task_id = task.task_id;
            if let Some(audit) = &self.pull_audit {
                audit.record_pull_fulfilled(
                    self.lb_id,
                    server_idx,
                    Some(request_id),
                    pulled_task_id,
                    queue_len_before,
                    queue_head_task_id,
                );
            }
            self.pull_intent_load[server_idx] =
                self.pull_intent_load[server_idx].saturating_sub(1);
            self.dispatch_to_server(server_idx, task).await;
            return;
        }

        let server_idx = pull.server_idx;
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

    pub async fn probe_reply(&mut self, reply: ProbeReply, _cx: &Context<Self>) {
        if !self.lb_policy.is_prequal() {
            return;
        }
        self.candidate_pool
            .ingest_reply(reply.server_idx, reply.rif);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexosim::time::MonotonicTime;

    fn test_lb(approx_sched: Option<ApproxSchedKind>) -> LoadBalancer {
        LoadBalancer::new(
            LoadBalancePolicyKind::Approx.build(),
            LoadBalancePolicyKind::Approx,
            2,
            vec![0, 1],
            0,
            false,
            approx_sched,
            None,
        )
    }

    fn task_with_id(task_id: u64) -> Task {
        let mut task = Task::new(MonotonicTime::EPOCH, std::time::Duration::from_secs(1));
        task.task_id = task_id;
        task
    }

    #[test]
    fn bound_pull_removes_matching_task_not_front() {
        let mut lb = test_lb(None);
        lb.queue.push(task_with_id(1));
        lb.queue.push(task_with_id(2));
        lb.queue.push(task_with_id(3));
        let task = lb.remove_task_for_pull(2).expect("task 2 present");
        assert_eq!(task.task_id, 2);
        assert_eq!(lb.queue.len(), 2);
        assert_eq!(lb.queue[0].task_id, 1);
        assert_eq!(lb.queue[1].task_id, 3);
    }

    #[test]
    fn no_bind_pull_takes_oldest_not_bound_id() {
        let mut lb = test_lb(Some(ApproxSchedKind::Fcfs));
        lb.queue.push(task_with_id(1));
        lb.queue.push(task_with_id(2));
        lb.queue.push(task_with_id(3));
        let task = lb.queue.remove(0);
        assert_eq!(task.task_id, 1);
        assert_eq!(lb.queue.len(), 2);
        assert_eq!(lb.queue[0].task_id, 2);
        assert_eq!(lb.queue[1].task_id, 3);
    }

    #[test]
    #[should_panic(expected = "no queued task")]
    fn no_bind_empty_queue_panics() {
        crate::approx::fatal_pull_abort(
            "lb",
            "no queued task for approx pull (lb_id=0, server_idx=0, \
             ignored_request_id=Some(99), queue_len=0, pull_intent_load=0, \
             queued_task_ids=[])",
        );
    }
}
