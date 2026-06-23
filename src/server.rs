use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct Task {
    pub duration: Duration,
    pub finish: MonotonicTime,
    pub start: MonotonicTime,
}

impl Task {
    pub fn new(start: MonotonicTime, duration: Duration) -> Self {
        Self {
            duration,
            finish: MonotonicTime::EPOCH,
            start,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Server {
    pub output: Output<Task>,
    max_concurrency: u32,
    in_flight: u32,
    queue: Vec<Task>,
    #[serde(skip)]
    load: Arc<AtomicU32>,
}

impl Server {
    pub fn new(max_concurrency: u32, load: Arc<AtomicU32>) -> Self {
        Self {
            output: Output::default(),
            max_concurrency: max_concurrency.max(1),
            in_flight: 0,
            queue: Vec::new(),
            load,
        }
    }

    fn sync_load(&self) {
        self.load
            .store(self.in_flight + self.queue.len() as u32, Ordering::Relaxed);
    }

    fn begin_service(&mut self, task: Task, cx: &Context<Self>) {
        self.in_flight += 1;
        if let Err(t) = cx.schedule_event(task.duration, schedulable!(Self::complete), task) {
            eprintln!("could not schedule complete. err: {}", t);
            self.in_flight -= 1;
        }
        self.sync_load();
    }

    fn drain_queue(&mut self, cx: &Context<Self>) {
        while self.in_flight < self.max_concurrency && !self.queue.is_empty() {
            let next = self.queue.remove(0);
            self.begin_service(next, cx);
        }
        self.sync_load();
    }
}

#[Model]
impl Server {
    pub async fn input(&mut self, task: Task, cx: &Context<Self>) {
        if self.in_flight < self.max_concurrency {
            self.begin_service(task, cx);
        } else {
            self.queue.push(task);
            self.sync_load();
        }
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut task: Task, cx: &Context<Self>) {
        task.finish = cx.time();
        self.output.send(task).await;
        self.in_flight -= 1;
        self.drain_queue(cx);
    }
}
