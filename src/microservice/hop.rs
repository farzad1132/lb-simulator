use nexosim::time::MonotonicTime;
use rand::Rng;
use serde::{Deserialize, Serialize};
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
pub struct CallerRef {
    pub microservice: String,
    pub server: usize,
    pub resume_endpoint: String,
    pub resume_sibling_index: usize,
    pub resume_caller: Option<Box<CallerRef>>,
    #[serde(default)]
    pub outbound_target_microservice: String,
    #[serde(default)]
    pub outbound_target_server: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutboundRelease {
    pub target_microservice: String,
    pub target_server: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Hop {
    pub request_id: u64,
    #[serde(default)]
    pub trace: bool,
    pub api: String,
    pub endpoint: String,
    pub sibling_index: usize,
    pub start: MonotonicTime,
    pub deadline: MonotonicTime,
    pub duration: Duration,
    pub processing_time: Duration,
    pub caller: Option<CallerRef>,
    #[serde(default)]
    pub outbound_release: Option<OutboundRelease>,
    #[serde(default)]
    pub slot_release: Option<OutboundRelease>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompletedRequest {
    pub request_id: u64,
    #[serde(default)]
    pub trace: bool,
    pub api: String,
    pub start: MonotonicTime,
    pub finish: MonotonicTime,
    pub processing_time: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutboundCall {
    pub target_microservice: String,
    pub hop: Hop,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ReplicaInput {
    Upstream(Hop),
    DownstreamReturn(Hop),
}

pub fn filtered_children(graph: &CallGraph, endpoint: &str, api: &str) -> Vec<String> {
    graph
        .children
        .get(endpoint)
        .map(|edges| {
            edges
                .iter()
                .filter(|(_, edge_api)| edge_api.as_deref() == Some(api))
                .map(|(target, _)| target.clone())
                .collect()
        })
        .unwrap_or_default()
}

pub fn sample_duration(graph: &CallGraph, endpoint: &str) -> Result<Duration, String> {
    let mean = *graph
        .interface_means
        .get(endpoint)
        .ok_or_else(|| format!("no mean for endpoint {}", endpoint))?;
    let secs = if graph.force_fixed_svc {
        mean.max(MIN_DURATION_SECS)
    } else {
        crate::rng::with_rng(|rng| sample_exp(rng, mean))
    };
    Ok(Duration::from_secs_f32(secs))
}

pub fn microservice_for_endpoint(graph: &CallGraph, endpoint: &str) -> Result<String, String> {
    graph
        .endpoint_microservice
        .get(endpoint)
        .cloned()
        .ok_or_else(|| format!("unknown endpoint {}", endpoint))
}
