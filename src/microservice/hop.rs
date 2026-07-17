use nexosim::time::MonotonicTime;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::callgraph::{CallGraph, MsServiceDistribution};

const MIN_DURATION_SECS: f32 = 1e-9;
/// Unit-mean bimodal shape: E[S] = 0.9·0.5 + 0.1·5.5 = 1. Scaled by endpoint mean.
const BIMODAL_MODES: [f32; 2] = [0.5, 5.5];
const BIMODAL_PROBS: [f32; 2] = [0.9, 0.1];

pub fn sample_exp(rng: &mut impl Rng, mean: f32) -> f32 {
    let u = loop {
        let u = 1.0 - rng.random::<f32>();
        if u > 0.0 && u.is_finite() {
            break u;
        }
    };
    (-mean * u.ln()).max(MIN_DURATION_SECS)
}

fn select_bimodal_mode(rng: &mut impl Rng, mean: f32) -> f32 {
    if rng.random::<f32>() < BIMODAL_PROBS[0] {
        BIMODAL_MODES[0] * mean
    } else {
        BIMODAL_MODES[1] * mean
    }
}

fn sample_bimodal(rng: &mut impl Rng, mean: f32) -> f32 {
    let mode_mean = select_bimodal_mode(rng, mean);
    sample_exp(rng, mode_mean)
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
    #[serde(default)]
    pub response_time_ms: u64,
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
    let secs = match graph.service_dist {
        MsServiceDistribution::Exp => crate::rng::with_rng(|rng| sample_exp(rng, mean)),
        MsServiceDistribution::Fixed => mean.max(MIN_DURATION_SECS),
        MsServiceDistribution::Bimodal => crate::rng::with_rng(|rng| sample_bimodal(rng, mean)),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bimodal_unit_modes_have_mean_one() {
        let mean = BIMODAL_MODES[0] * BIMODAL_PROBS[0] + BIMODAL_MODES[1] * BIMODAL_PROBS[1];
        assert!((mean - 1.0).abs() < 1e-6);
    }

    #[test]
    fn bimodal_scaled_modes_preserve_endpoint_mean() {
        let endpoint_mean = 0.003_f32;
        let scaled_mean = BIMODAL_MODES[0] * endpoint_mean * BIMODAL_PROBS[0]
            + BIMODAL_MODES[1] * endpoint_mean * BIMODAL_PROBS[1];
        assert!((scaled_mean - endpoint_mean).abs() < 1e-9);
    }
}
