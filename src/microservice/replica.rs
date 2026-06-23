use super::hop::Hop;
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::Output;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Deserialize, Serialize)]
pub struct Replica {
    pub output: Output<Hop>,
    max_concurrency: u32,
    in_flight: u32,
    queue: Vec<Hop>,
    #[serde(skip)]
    load: Arc<AtomicU32>,
}

impl Replica {
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

    fn begin_service(&mut self, hop: Hop, cx: &Context<Self>) {
        self.in_flight += 1;
        if let Err(h) = cx.schedule_event(hop.duration, schedulable!(Self::complete), hop) {
            eprintln!("could not schedule complete. err: {}", h);
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
impl Replica {
    pub async fn input(&mut self, hop: Hop, cx: &Context<Self>) {
        if self.in_flight < self.max_concurrency {
            self.begin_service(hop, cx);
        } else {
            self.queue.push(hop);
            self.sync_load();
        }
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, hop: Hop, cx: &Context<Self>) {
        self.output.send(hop).await;
        self.in_flight -= 1;
        self.drain_queue(cx);
    }
}
