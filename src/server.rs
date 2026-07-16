use lb::approx::{PullIntent, PullRequest};
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::simulation::EventKey;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct Task {
    pub duration: Duration,
    pub finish: MonotonicTime,
    pub start: MonotonicTime,
    pub task_id: u64,
    pub lb_id: usize,
    pub origin_server_idx: usize,
    pub served_by_express: bool,
    pub evicted_at: Option<MonotonicTime>,
    pub shed_at: Option<MonotonicTime>,
    pub service_started_at: Option<MonotonicTime>,
}

impl Task {
    pub fn new(start: MonotonicTime, duration: Duration) -> Self {
        Self {
            duration,
            finish: MonotonicTime::EPOCH,
            start,
            task_id: 0,
            lb_id: 0,
            origin_server_idx: 0,
            served_by_express: false,
            evicted_at: None,
            shed_at: None,
            service_started_at: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum QueueDelayEvictionMode {
    Monitored,
    ImmediateIdeal,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum ExpressEvictionPolicy {
    QueueDepth(u32),
    QueueDelay {
        threshold: Duration,
        mode: QueueDelayEvictionMode,
    },
    Combined {
        depth_threshold: u32,
        delay_threshold: Duration,
    },
}

#[derive(Clone, Copy, Debug)]
struct InFlightService {
    started_at: MonotonicTime,
    duration: Duration,
}

struct PendingEviction {
    event_key: EventKey,
}

struct PendingShed {
    event_key: EventKey,
}

fn remaining_service_time(started_at: MonotonicTime, duration: Duration, now: MonotonicTime) -> Duration {
    let elapsed = now.duration_since(started_at);
    duration.saturating_sub(elapsed)
}

fn head_of_line_wait(in_flight: &[InFlightService], now: MonotonicTime) -> Duration {
    in_flight
        .iter()
        .map(|svc| now.duration_since(svc.started_at))
        .max()
        .unwrap_or(Duration::ZERO)
}

fn head_of_line_exceeds_threshold(
    in_flight: &[InFlightService],
    now: MonotonicTime,
    threshold: Duration,
) -> bool {
    !in_flight.is_empty() && head_of_line_wait(in_flight, now) > threshold
}

fn ideal_queue_delay_estimate(
    queue: &[Task],
    in_flight: &[InFlightService],
    now: MonotonicTime,
) -> Duration {
    let queue_work: Duration = queue.iter().map(|t| t.duration).fold(Duration::ZERO, |a, b| a + b);
    let min_remaining = in_flight
        .iter()
        .map(|svc| remaining_service_time(svc.started_at, svc.duration, now))
        .min()
        .unwrap_or(Duration::ZERO);
    queue_work + min_remaining
}

fn remove_task_from_queue(queue: &mut Vec<Task>, task_start: MonotonicTime) -> Option<Task> {
    let idx = queue.iter().position(|t| t.start == task_start)?;
    Some(queue.remove(idx))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum DispatchMode {
    Push,
    Centralized,
    Approx,
}

#[derive(Deserialize, Serialize)]
pub struct Server {
    pub output: Output<Task>,
    pub express_output: Output<Task>,
    pub pull_output: Output<PullRequest>,
    server_idx: usize,
    release_outputs: Vec<Output<usize>>,
    pull_outputs: Vec<Output<PullRequest>>,
    max_concurrency: u32,
    in_flight: u32,
    pending_pulls: u32,
    dispatch_mode: DispatchMode,
    #[serde(skip)]
    in_flight_services: Vec<InFlightService>,
    queue: Vec<Task>,
    #[serde(skip)]
    pull_intent_queue: VecDeque<PullIntent>,
    express_eviction: Option<ExpressEvictionPolicy>,
    work_shedding: Option<Duration>,
    is_express: bool,
    express_lb_id: Option<usize>,
    #[serde(skip)]
    pending_evictions: Vec<(MonotonicTime, PendingEviction)>,
    #[serde(skip)]
    shed_outputs: Vec<Output<Task>>,
    #[serde(skip)]
    pending_sheds: Vec<(MonotonicTime, PendingShed)>,
}

impl Server {
    pub fn new(
        max_concurrency: u32,
        server_idx: usize,
        release_outputs: Vec<Output<usize>>,
        express_eviction: Option<ExpressEvictionPolicy>,
        work_shedding: Option<Duration>,
        is_express: bool,
        express_lb_id: Option<usize>,
        dispatch_mode: DispatchMode,
    ) -> Self {
        Self {
            output: Output::default(),
            express_output: Output::default(),
            pull_output: Output::default(),
            server_idx,
            release_outputs,
            pull_outputs: Vec::new(),
            max_concurrency: max_concurrency.max(1),
            in_flight: 0,
            pending_pulls: 0,
            dispatch_mode,
            in_flight_services: Vec::new(),
            queue: Vec::new(),
            pull_intent_queue: VecDeque::new(),
            express_eviction,
            work_shedding,
            is_express,
            express_lb_id,
            pending_evictions: Vec::new(),
            shed_outputs: Vec::new(),
            pending_sheds: Vec::new(),
        }
    }

    pub fn set_pull_outputs(&mut self, pull_outputs: Vec<Output<PullRequest>>) {
        self.pull_outputs = pull_outputs;
    }

    pub fn set_shed_outputs(&mut self, shed_outputs: Vec<Output<Task>>) {
        self.shed_outputs = shed_outputs;
    }

    fn depth_exceeds(&self, th: u32) -> bool {
        self.queue.len() as u32 > th
    }

    fn delay_should_evict_immediate(
        &self,
        threshold: Duration,
        mode: QueueDelayEvictionMode,
        now: MonotonicTime,
    ) -> bool {
        head_of_line_exceeds_threshold(&self.in_flight_services, now, threshold)
            || (mode == QueueDelayEvictionMode::ImmediateIdeal
                && ideal_queue_delay_estimate(&self.queue, &self.in_flight_services, now)
                    > threshold)
    }

    fn delay_should_schedule_timer(
        &self,
        threshold: Duration,
        mode: QueueDelayEvictionMode,
        now: MonotonicTime,
    ) -> bool {
        mode == QueueDelayEvictionMode::Monitored
            && ideal_queue_delay_estimate(&self.queue, &self.in_flight_services, now) > threshold
    }

    async fn evict_newest(&mut self, cx: &Context<Self>) {
        let evicted = self.queue.pop().expect("queue non-empty");
        self.forward_evicted(evicted, cx).await;
    }

    async fn apply_enqueue_eviction(&mut self, task_start: MonotonicTime, cx: &Context<Self>) {
        let now = cx.time();
        match self.express_eviction {
            None => {}
            Some(ExpressEvictionPolicy::QueueDepth(th)) => {
                if self.depth_exceeds(th) {
                    self.evict_newest(cx).await;
                }
            }
            Some(ExpressEvictionPolicy::QueueDelay { threshold, mode }) => {
                if self.delay_should_evict_immediate(threshold, mode, now) {
                    self.evict_newest(cx).await;
                } else if self.delay_should_schedule_timer(threshold, mode, now) {
                    self.schedule_monitored_eviction(task_start, threshold, cx);
                }
            }
            Some(ExpressEvictionPolicy::Combined {
                depth_threshold,
                delay_threshold,
            }) => {
                if self.depth_exceeds(depth_threshold) {
                    self.evict_newest(cx).await;
                } else if head_of_line_exceeds_threshold(
                    &self.in_flight_services,
                    now,
                    delay_threshold,
                ) {
                    self.evict_newest(cx).await;
                } else if ideal_queue_delay_estimate(
                    &self.queue,
                    &self.in_flight_services,
                    now,
                ) > delay_threshold
                {
                    self.schedule_monitored_eviction(task_start, delay_threshold, cx);
                }
            }
        }
    }

    fn cancel_pending_eviction(&mut self, task_start: MonotonicTime) {
        if let Some(idx) = self
            .pending_evictions
            .iter()
            .position(|(start, _)| *start == task_start)
        {
            let (_, pending) = self.pending_evictions.remove(idx);
            pending.event_key.cancel();
        }
    }

    fn schedule_monitored_eviction(
        &mut self,
        task_start: MonotonicTime,
        threshold: Duration,
        cx: &Context<Self>,
    ) {
        if let Ok(event_key) =
            cx.schedule_keyed_event(threshold, schedulable!(Self::evict_task), task_start)
        {
            self.pending_evictions
                .push((task_start, PendingEviction { event_key }));
        }
    }

    async fn forward_evicted(&mut self, mut task: Task, cx: &Context<Self>) {
        task.evicted_at = Some(cx.time());
        self.express_output.send(task).await;
    }

    async fn shed_newest(&mut self, cx: &Context<Self>) {
        let shed = self.queue.pop().expect("queue non-empty");
        self.forward_shed(shed, cx).await;
    }

    async fn apply_work_shedding_on_enqueue(&mut self, task_start: MonotonicTime, cx: &Context<Self>) {
        let Some(threshold) = self.work_shedding else {
            return;
        };
        let now = cx.time();
        if head_of_line_exceeds_threshold(&self.in_flight_services, now, threshold) {
            self.shed_newest(cx).await;
        } else if ideal_queue_delay_estimate(&self.queue, &self.in_flight_services, now)
            > threshold
        {
            self.schedule_monitored_shed(task_start, threshold, cx);
        }
    }

    fn cancel_pending_shed(&mut self, task_start: MonotonicTime) {
        if let Some(idx) = self
            .pending_sheds
            .iter()
            .position(|(start, _)| *start == task_start)
        {
            let (_, pending) = self.pending_sheds.remove(idx);
            pending.event_key.cancel();
        }
    }

    fn schedule_monitored_shed(
        &mut self,
        task_start: MonotonicTime,
        threshold: Duration,
        cx: &Context<Self>,
    ) {
        if let Ok(event_key) =
            cx.schedule_keyed_event(threshold, schedulable!(Self::shed_task), task_start)
        {
            self.pending_sheds
                .push((task_start, PendingShed { event_key }));
        }
    }

    async fn forward_shed(&mut self, mut task: Task, cx: &Context<Self>) {
        task.shed_at = Some(cx.time());
        let lb_id = task.lb_id;
        self.release_outputs[lb_id]
            .send(self.server_idx)
            .await;
        self.shed_outputs[lb_id].send(task).await;
    }

    fn begin_service(&mut self, mut task: Task, cx: &Context<Self>) {
        self.cancel_pending_eviction(task.start);
        self.cancel_pending_shed(task.start);
        let started_at = cx.time();
        task.service_started_at = Some(started_at);
        self.in_flight_services.push(InFlightService {
            started_at,
            duration: task.duration,
        });
        self.in_flight += 1;
        if let Err(t) = cx.schedule_event(task.duration, schedulable!(Self::complete), task) {
            eprintln!("could not schedule complete. err: {}", t);
            self.in_flight_services.pop();
            self.in_flight -= 1;
        }
    }

    fn drain_queue(&mut self, cx: &Context<Self>) {
        while self.in_flight < self.max_concurrency && !self.queue.is_empty() {
            let next = self.queue.remove(0);
            self.begin_service(next, cx);
        }
    }

    fn remove_in_flight(&mut self, service_started_at: MonotonicTime) {
        if let Some(idx) = self
            .in_flight_services
            .iter()
            .position(|svc| svc.started_at == service_started_at)
        {
            self.in_flight_services.remove(idx);
        }
    }
}

#[Model]
impl Server {
    pub async fn input(&mut self, task: Task, cx: &Context<Self>) {
        if matches!(
            self.dispatch_mode,
            DispatchMode::Centralized | DispatchMode::Approx
        ) {
            if self.dispatch_mode == DispatchMode::Approx {
                self.pending_pulls = self.pending_pulls.saturating_sub(1);
            }
            self.begin_service(task, cx);
            return;
        }

        if self.in_flight < self.max_concurrency {
            self.begin_service(task, cx);
        } else {
            let task_start = task.start;
            self.queue.push(task);
            self.apply_enqueue_eviction(task_start, cx).await;
            self.apply_work_shedding_on_enqueue(task_start, cx).await;
        }
    }

    pub async fn receive_pull_intent(&mut self, intent: PullIntent, _cx: &Context<Self>) {
        if self.dispatch_mode != DispatchMode::Approx {
            return;
        }
        self.pull_intent_queue.push_back(intent);
        self.drain_pull_intents_async().await;
    }

    pub async fn request_pull(&mut self, _: (), _cx: &Context<Self>) {
        if self.dispatch_mode == DispatchMode::Centralized && self.in_flight < self.max_concurrency {
            self.pull_output
                .send(PullRequest {
                    server_idx: self.server_idx,
                    request_id: None,
                })
                .await;
        }
    }

    async fn drain_pull_intents_async(&mut self) {
        if self.in_flight + self.pending_pulls >= self.max_concurrency {
            return;
        }
        let Some(intent) = self.pull_intent_queue.pop_front() else {
            return;
        };
        self.pending_pulls += 1;
        if let Some(output) = self.pull_outputs.get_mut(intent.sender_id) {
            output
                .send(PullRequest {
                    server_idx: self.server_idx,
                    request_id: Some(intent.request_id),
                })
                .await;
        } else {
            self.pending_pulls = self.pending_pulls.saturating_sub(1);
        }
    }

    #[nexosim(schedulable)]
    async fn evict_task(&mut self, task_start: MonotonicTime, cx: &Context<Self>) {
        self.pending_evictions.retain(|(start, _)| *start != task_start);
        if let Some(task) = remove_task_from_queue(&mut self.queue, task_start) {
            self.forward_evicted(task, cx).await;
        }
    }

    #[nexosim(schedulable)]
    async fn shed_task(&mut self, task_start: MonotonicTime, cx: &Context<Self>) {
        self.pending_sheds.retain(|(start, _)| *start != task_start);
        if let Some(task) = remove_task_from_queue(&mut self.queue, task_start) {
            self.forward_shed(task, cx).await;
        }
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut task: Task, cx: &Context<Self>) {
        task.finish = cx.time();
        task.served_by_express = self.is_express;
        let lb_id = task.lb_id;
        let origin_server_idx = task.origin_server_idx;
        if let Some(started_at) = task.service_started_at {
            self.remove_in_flight(started_at);
        }
        self.output.send(task).await;
        if self.is_express {
            let express_lb_id = self
                .express_lb_id
                .expect("express server must have express_lb_id");
            self.release_outputs[express_lb_id]
                .send(self.server_idx)
                .await;
            self.release_outputs[lb_id]
                .send(origin_server_idx)
                .await;
        } else {
            self.release_outputs[lb_id]
                .send(self.server_idx)
                .await;
        }
        self.in_flight -= 1;
        match self.dispatch_mode {
            DispatchMode::Centralized => {
                self.pull_output
                    .send(PullRequest {
                        server_idx: self.server_idx,
                        request_id: None,
                    })
                    .await;
            }
            DispatchMode::Approx => {
                self.drain_pull_intents_async().await;
            }
            DispatchMode::Push => {
                self.drain_queue(cx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_with_duration(start_offset: f64, duration: f64) -> Task {
        Task::new(
            MonotonicTime::EPOCH + Duration::from_secs_f64(start_offset),
            Duration::from_secs_f64(duration),
        )
    }

    fn in_flight(started_offset: f64, duration: f64) -> InFlightService {
        InFlightService {
            started_at: MonotonicTime::EPOCH + Duration::from_secs_f64(started_offset),
            duration: Duration::from_secs_f64(duration),
        }
    }

    #[test]
    fn head_of_line_wait_uses_in_flight_service_start() {
        let in_flight = vec![in_flight(1.0, 2.0)];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(3.5);
        let delay = head_of_line_wait(&in_flight, now);
        assert_eq!(delay, Duration::from_secs_f64(2.5));
    }

    #[test]
    fn head_of_line_wait_uses_max_elapsed_among_in_flight() {
        let in_flight = vec![in_flight(0.0, 4.0), in_flight(2.0, 1.0)];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(3.0);
        let delay = head_of_line_wait(&in_flight, now);
        assert_eq!(delay, Duration::from_secs_f64(3.0));
    }

    #[test]
    fn head_of_line_exceeds_threshold_at_boundary() {
        let in_flight = vec![in_flight(1.0, 2.0)];
        let threshold = Duration::from_secs_f64(2.5);
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(3.5);
        assert!(!head_of_line_exceeds_threshold(&in_flight, now, threshold));
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(3.51);
        assert!(head_of_line_exceeds_threshold(&in_flight, now, threshold));
    }

    #[test]
    fn head_of_line_exceeds_threshold_false_when_no_in_flight() {
        let threshold = Duration::from_secs_f64(1.0);
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(10.0);
        assert!(!head_of_line_exceeds_threshold(&[], now, threshold));
    }

    #[test]
    fn ideal_queue_delay_estimate_sums_queue_and_min_remaining() {
        let queue = vec![
            task_with_duration(0.0, 1.0),
            task_with_duration(0.0, 2.0),
        ];
        let in_flight = vec![
            in_flight(0.0, 4.0),
            in_flight(0.0, 1.5),
        ];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(1.0);
        let delay = ideal_queue_delay_estimate(&queue, &in_flight, now);
        assert_eq!(delay, Duration::from_secs_f64(3.5));
    }

    #[test]
    fn ideal_queue_delay_estimate_with_no_in_flight_uses_queue_only() {
        let queue = vec![task_with_duration(0.0, 0.75)];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(1.0);
        let delay = ideal_queue_delay_estimate(&queue, &[], now);
        assert_eq!(delay, Duration::from_secs_f64(0.75));
    }

    #[test]
    fn remove_task_from_queue_finds_task_not_at_back() {
        let t0 = task_with_duration(0.0, 1.0);
        let t1 = task_with_duration(1.0, 1.0);
        let t2 = task_with_duration(2.0, 1.0);
        let mut queue = vec![t0.clone(), t1.clone(), t2.clone()];
        let removed = remove_task_from_queue(&mut queue, t1.start).unwrap();
        assert_eq!(removed.start, t1.start);
        assert_eq!(queue.len(), 2);
        assert_eq!(queue[0].start, t0.start);
        assert_eq!(queue[1].start, t2.start);
    }

    #[test]
    fn remove_task_from_queue_returns_none_when_missing() {
        let mut queue = vec![task_with_duration(0.0, 1.0)];
        let missing = MonotonicTime::EPOCH + Duration::from_secs_f64(99.0);
        assert!(remove_task_from_queue(&mut queue, missing).is_none());
    }

    #[test]
    fn queue_delay_eviction_mode_from_ideal_flag() {
        assert_eq!(
            if false {
                QueueDelayEvictionMode::ImmediateIdeal
            } else {
                QueueDelayEvictionMode::Monitored
            },
            QueueDelayEvictionMode::Monitored
        );
        assert_eq!(
            if true {
                QueueDelayEvictionMode::ImmediateIdeal
            } else {
                QueueDelayEvictionMode::Monitored
            },
            QueueDelayEvictionMode::ImmediateIdeal
        );
    }
}
