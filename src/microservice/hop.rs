use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::callgraph::CallGraph;

const MIN_DURATION_SECS: f32 = 1e-9;

pub fn sample_exp(rng: &mut impl Rng, mean: f32) -> f32 {
    let u = loop {
        let u = 1.0 - rng.random::<f32>();
        if u > 0.0 && u.is_finite() {
            break u;
        }
    };
    (-mean * u.ln()).max(MIN_DURATION_SECS)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Hop {
    pub api: String,
    pub hop_index: usize,
    pub start: MonotonicTime,
    pub duration: Duration,
    pub processing_time: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompletedRequest {
    pub api: String,
    pub start: MonotonicTime,
    pub finish: MonotonicTime,
    pub processing_time: Duration,
}

#[derive(Deserialize, Serialize)]
pub struct HopForward {
    pub output: Output<HopDispatcherMsg>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HopDispatcherMsg {
    Arrival(String),
    ReplicaDone(Hop),
}

#[derive(Deserialize, Serialize)]
pub struct HopDispatcher {
    pub completed: Output<CompletedRequest>,
    #[serde(skip)]
    graph: Arc<CallGraph>,
    #[serde(skip)]
    service_outputs: HashMap<String, Output<Hop>>,
    #[serde(skip)]
    busy_time: Arc<Mutex<HashMap<String, Duration>>>,
}

impl HopDispatcher {
    pub fn new(
        graph: Arc<CallGraph>,
        service_outputs: HashMap<String, Output<Hop>>,
        busy_time: Arc<Mutex<HashMap<String, Duration>>>,
    ) -> Self {
        Self {
            completed: Output::default(),
            graph,
            service_outputs,
            busy_time,
        }
    }

    fn endpoint_for(&self, hop: &Hop) -> Result<&str, String> {
        self.graph
            .paths
            .get(&hop.api)
            .and_then(|p| p.get(hop.hop_index))
            .map(String::as_str)
            .ok_or_else(|| format!("invalid hop_index {} for API {}", hop.hop_index, hop.api))
    }

    fn service_for_endpoint(&self, endpoint: &str) -> Result<&str, String> {
        self.graph
            .endpoint_service
            .get(endpoint)
            .map(String::as_str)
            .ok_or_else(|| format!("unknown endpoint {}", endpoint))
    }

    async fn dispatch(&mut self, mut hop: Hop) -> Result<(), String> {
        let endpoint = self.endpoint_for(&hop)?.to_string();
        let mean = *self
            .graph
            .interface_means
            .get(&endpoint)
            .ok_or_else(|| format!("no mean for endpoint {}", endpoint))?;
        hop.duration = Duration::from_secs_f32(sample_exp(&mut rand::rng(), mean));

        let service = self.service_for_endpoint(&endpoint)?.to_string();
        let output = self
            .service_outputs
            .get_mut(&service)
            .ok_or_else(|| format!("no balancer for service {}", service))?;
        output.send(hop).await;
        Ok(())
    }

    async fn on_arrival(&mut self, api: String, cx: &Context<Self>) {
        let hop = Hop {
            api,
            hop_index: 0,
            start: cx.time(),
            duration: Duration::ZERO,
            processing_time: Duration::ZERO,
        };
        if let Err(e) = self.dispatch(hop).await {
            eprintln!("dispatch failed on arrival: {}", e);
        }
    }

    async fn on_replica_done(&mut self, mut hop: Hop, cx: &Context<Self>) {
        let finish_time = cx.time();
        let endpoint = match self.endpoint_for(&hop) {
            Ok(e) => e.to_string(),
            Err(e) => {
                eprintln!("replica_complete: {}", e);
                return;
            }
        };
        let service = match self.service_for_endpoint(&endpoint) {
            Ok(s) => s.to_string(),
            Err(e) => {
                eprintln!("replica_complete: {}", e);
                return;
            }
        };

        if let Ok(mut busy) = self.busy_time.lock() {
            *busy.entry(service).or_default() += hop.duration;
        }
        hop.processing_time += hop.duration;
        hop.hop_index += 1;

        let path_len = self
            .graph
            .paths
            .get(&hop.api)
            .map(|p| p.len())
            .unwrap_or(0);

        if hop.hop_index >= path_len {
            self.completed
                .send(CompletedRequest {
                    api: hop.api,
                    start: hop.start,
                    finish: finish_time,
                    processing_time: hop.processing_time,
                })
                .await;
        } else if let Err(e) = self.dispatch(hop).await {
            eprintln!("dispatch failed after replica: {}", e);
        }
    }
}

#[Model]
impl HopForward {
    pub async fn input(&mut self, hop: Hop, _cx: &Context<Self>) {
        self.output
            .send(HopDispatcherMsg::ReplicaDone(hop))
            .await;
    }
}

#[Model]
impl HopDispatcher {
    pub async fn input(&mut self, msg: HopDispatcherMsg, cx: &Context<Self>) {
        match msg {
            HopDispatcherMsg::Arrival(api) => self.on_arrival(api, cx).await,
            HopDispatcherMsg::ReplicaDone(hop) => self.on_replica_done(hop, cx).await,
        }
    }
}
