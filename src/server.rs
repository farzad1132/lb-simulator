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
}

impl Task {
    pub fn new(start: MonotonicTime, duration: Duration) -> Self {
        Self {
            duration,
            finish: MonotonicTime::EPOCH,
            start,
            lb_id: 0,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Server {
    pub output: Output<Task>,
    server_idx: usize,
    release_outputs: Vec<Output<usize>>,
    max_concurrency: u32,
    in_flight: u32,
    queue: Vec<Task>,
}

impl Server {
    pub fn new(
        max_concurrency: u32,
        server_idx: usize,
        release_outputs: Vec<Output<usize>>,
    ) -> Self {
        Self {
            output: Output::default(),
            server_idx,
            release_outputs,
            max_concurrency: max_concurrency.max(1),
            in_flight: 0,
            queue: Vec::new(),
        }
    }

    fn begin_service(&mut self, task: Task, cx: &Context<Self>) {
        self.in_flight += 1;
        if let Err(t) = cx.schedule_event(task.duration, schedulable!(Self::complete), task) {
            eprintln!("could not schedule complete. err: {}", t);
            self.in_flight -= 1;
        }
    }

    fn drain_queue(&mut self, cx: &Context<Self>) {
        while self.in_flight < self.max_concurrency && !self.queue.is_empty() {
            let next = self.queue.remove(0);
            self.begin_service(next, cx);
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
        }
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut task: Task, cx: &Context<Self>) {
        task.finish = cx.time();
        let lb_id = task.lb_id;
        self.output.send(task).await;
        self.release_outputs[lb_id]
            .send(self.server_idx)
            .await;
        self.in_flight -= 1;
        self.drain_queue(cx);
    }
}
