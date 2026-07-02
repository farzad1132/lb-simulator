use lb::load_registry::LoadRegistry;
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct Task {
    pub duration: Duration,
    pub finish: MonotonicTime,
    pub start: MonotonicTime,
    pub lb_id: usize,
    pub origin_server_idx: usize,
    pub served_by_express: bool,
    pub evicted_at: Option<MonotonicTime>,
    pub service_started_at: Option<MonotonicTime>,
}

impl Task {
    pub fn new(start: MonotonicTime, duration: Duration) -> Self {
        Self {
            duration,
            finish: MonotonicTime::EPOCH,
            start,
            lb_id: 0,
            origin_server_idx: 0,
            served_by_express: false,
            evicted_at: None,
            service_started_at: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum ExpressEvictionPolicy {
    QueueDepth(u32),
    QueueDelay {
        threshold: Duration,
        ideal: bool,
    },
}

#[derive(Clone, Copy, Debug)]
struct InFlightService {
    started_at: MonotonicTime,
    duration: Duration,
}

fn remaining_service_time(started_at: MonotonicTime, duration: Duration, now: MonotonicTime) -> Duration {
    let elapsed = now.duration_since(started_at);
    duration.saturating_sub(elapsed)
}

fn queue_delay_estimate(
    queue: &[Task],
    in_flight: &[InFlightService],
    now: MonotonicTime,
    ideal: bool,
) -> Duration {
    if ideal {
        let queue_work: Duration = queue.iter().map(|t| t.duration).fold(Duration::ZERO, |a, b| a + b);
        let min_remaining = in_flight
            .iter()
            .map(|svc| remaining_service_time(svc.started_at, svc.duration, now))
            .min()
            .unwrap_or(Duration::ZERO);
        queue_work + min_remaining
    } else {
        now.duration_since(queue[0].start)
    }
}

#[derive(Deserialize, Serialize)]
pub struct Server {
    pub output: Output<Task>,
    pub express_output: Output<Task>,
    server_idx: usize,
    release_outputs: Vec<Output<usize>>,
    max_concurrency: u32,
    in_flight: u32,
    #[serde(skip)]
    in_flight_services: Vec<InFlightService>,
    queue: Vec<Task>,
    express_eviction: Option<ExpressEvictionPolicy>,
    is_express: bool,
    express_lb_id: Option<usize>,
    #[serde(skip)]
    load_registry: LoadRegistry,
}

impl Server {
    pub fn new(
        max_concurrency: u32,
        server_idx: usize,
        release_outputs: Vec<Output<usize>>,
        load_registry: LoadRegistry,
        express_eviction: Option<ExpressEvictionPolicy>,
        is_express: bool,
        express_lb_id: Option<usize>,
    ) -> Self {
        Self {
            output: Output::default(),
            express_output: Output::default(),
            server_idx,
            release_outputs,
            max_concurrency: max_concurrency.max(1),
            in_flight: 0,
            in_flight_services: Vec::new(),
            queue: Vec::new(),
            express_eviction,
            is_express,
            express_lb_id,
            load_registry,
        }
    }

    fn should_evict(&self, now: MonotonicTime) -> bool {
        match self.express_eviction {
            None => false,
            Some(ExpressEvictionPolicy::QueueDepth(th)) => self.queue.len() as u32 > th,
            Some(ExpressEvictionPolicy::QueueDelay { threshold, ideal }) => {
                queue_delay_estimate(&self.queue, &self.in_flight_services, now, ideal) > threshold
            }
        }
    }

    fn publish_load(&self) {
        self.load_registry
            .set(self.server_idx, self.in_flight + self.queue.len() as u32);
    }

    fn begin_service(&mut self, mut task: Task, cx: &Context<Self>) {
        let started_at = cx.time();
        task.service_started_at = Some(started_at);
        self.in_flight_services.push(InFlightService {
            started_at,
            duration: task.duration,
        });
        self.in_flight += 1;
        self.publish_load();
        if let Err(t) = cx.schedule_event(task.duration, schedulable!(Self::complete), task) {
            eprintln!("could not schedule complete. err: {}", t);
            self.in_flight_services.pop();
            self.in_flight -= 1;
            self.publish_load();
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
        if self.in_flight < self.max_concurrency {
            self.begin_service(task, cx);
        } else {
            self.queue.push(task);
            if self.express_eviction.is_some() && self.should_evict(cx.time()) {
                let mut evicted = self.queue.pop().expect("queue non-empty");
                evicted.evicted_at = Some(cx.time());
                self.express_output.send(evicted).await;
            }
            self.publish_load();
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
        self.publish_load();
        self.drain_queue(cx);
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
    fn queue_delay_estimate_default_uses_head_of_line_wait() {
        let queue = vec![task_with_duration(1.0, 2.0)];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(3.5);
        let delay = queue_delay_estimate(&queue, &[], now, false);
        assert_eq!(delay, Duration::from_secs_f64(2.5));
    }

    #[test]
    fn queue_delay_estimate_ideal_sums_queue_and_min_remaining() {
        let queue = vec![
            task_with_duration(0.0, 1.0),
            task_with_duration(0.0, 2.0),
        ];
        let in_flight = vec![
            in_flight(0.0, 4.0),
            in_flight(0.0, 1.5),
        ];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(1.0);
        let delay = queue_delay_estimate(&queue, &in_flight, now, true);
        assert_eq!(delay, Duration::from_secs_f64(3.5));
    }

    #[test]
    fn queue_delay_estimate_ideal_with_no_in_flight_uses_queue_only() {
        let queue = vec![task_with_duration(0.0, 0.75)];
        let now = MonotonicTime::EPOCH + Duration::from_secs_f64(1.0);
        let delay = queue_delay_estimate(&queue, &[], now, true);
        assert_eq!(delay, Duration::from_secs_f64(0.75));
    }
}
