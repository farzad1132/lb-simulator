use nexosim::time::MonotonicTime;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

const SECS_TO_MS: f64 = 1000.0;

#[derive(Default)]
struct ActiveVisit {
    arrival: MonotonicTime,
    slack_d_ms: f64,
    queueing_delay: Option<Duration>,
    cumulative_queueing: Option<Duration>,
    local_processing: Duration,
}

#[derive(Default)]
struct MicroserviceSamples {
    arrival_times_ms: Vec<f64>,
    departure_times_ms: Vec<f64>,
    response_time_ms: Vec<f64>,
    queueing_delay_ms: Vec<f64>,
    cumulative_queueing_delay_ms: Vec<f64>,
    processing_time_ms: Vec<f64>,
    slack_d_ms: Vec<f64>,
}

#[derive(Serialize)]
pub struct MicroserviceStats {
    pub inter_arrival_ms: Vec<f64>,
    pub inter_departure_ms: Vec<f64>,
    pub response_time_ms: Vec<f64>,
    pub queueing_delay_ms: Vec<f64>,
    pub cumulative_queueing_delay_ms: Vec<f64>,
    pub processing_time_ms: Vec<f64>,
    pub slack_d_ms: Vec<f64>,
}

pub struct MicroserviceVisitTracker {
    active: HashMap<(u64, String), ActiveVisit>,
    request_cumulative_queueing: HashMap<u64, Duration>,
    samples: HashMap<String, MicroserviceSamples>,
    microservice_index: HashMap<String, usize>,
    num_microservices: usize,
    per_request_cumulative: HashMap<u64, Vec<f64>>,
}

impl Default for MicroserviceVisitTracker {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl MicroserviceVisitTracker {
    pub fn new(microservice_order: &[String]) -> Self {
        let microservice_index = microservice_order
            .iter()
            .enumerate()
            .map(|(idx, id)| (id.clone(), idx))
            .collect();
        Self {
            active: HashMap::new(),
            request_cumulative_queueing: HashMap::new(),
            samples: HashMap::new(),
            num_microservices: microservice_order.len(),
            microservice_index,
            per_request_cumulative: HashMap::new(),
        }
    }

    pub fn record_arrival(
        &mut self,
        request_id: u64,
        microservice_id: &str,
        arrival: MonotonicTime,
        deadline: MonotonicTime,
    ) {
        let arrival_ms = time_to_ms(arrival);
        let deadline_ms = time_to_ms(deadline);
        self.active.insert(
            (request_id, microservice_id.to_string()),
            ActiveVisit {
                arrival,
                slack_d_ms: deadline_ms - arrival_ms,
                ..Default::default()
            },
        );
    }

    pub fn record_server_start(
        &mut self,
        request_id: u64,
        microservice_id: &str,
        start: MonotonicTime,
    ) {
        if let Some(visit) = self.active.get_mut(&(request_id, microservice_id.to_string())) {
            if visit.queueing_delay.is_none() {
                let queueing = start.duration_since(visit.arrival);
                visit.queueing_delay = Some(queueing);
                let cumulative = {
                    let total = self
                        .request_cumulative_queueing
                        .entry(request_id)
                        .or_insert(Duration::ZERO);
                    *total += queueing;
                    *total
                };
                visit.cumulative_queueing = Some(cumulative);
            }
        }
    }

    pub fn add_local_processing(
        &mut self,
        request_id: u64,
        microservice_id: &str,
        duration: Duration,
    ) {
        if let Some(visit) = self.active.get_mut(&(request_id, microservice_id.to_string())) {
            visit.local_processing += duration;
        }
    }

    pub fn finalize_visit(
        &mut self,
        request_id: u64,
        microservice_id: &str,
        departure: MonotonicTime,
    ) {
        let key = (request_id, microservice_id.to_string());
        let Some(visit) = self.active.remove(&key) else {
            return;
        };

        let response = departure.duration_since(visit.arrival);
        let queueing = visit.queueing_delay.unwrap_or(Duration::ZERO);
        let cumulative = visit.cumulative_queueing.unwrap_or(queueing);
        let cumulative_ms = cumulative.as_secs_f64() * SECS_TO_MS;

        if let Some(ms_index) = self.microservice_index.get(microservice_id) {
            let row = self
                .per_request_cumulative
                .entry(request_id)
                .or_insert_with(|| vec![f64::NAN; self.num_microservices]);
            row[*ms_index] = cumulative_ms;
        }

        let samples = self.samples.entry(microservice_id.to_string()).or_default();
        samples.arrival_times_ms.push(
            visit
                .arrival
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64()
                * SECS_TO_MS,
        );
        samples.departure_times_ms.push(
            departure
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64()
                * SECS_TO_MS,
        );
        samples
            .response_time_ms
            .push(response.as_secs_f64() * SECS_TO_MS);
        samples
            .queueing_delay_ms
            .push(queueing.as_secs_f64() * SECS_TO_MS);
        samples
            .cumulative_queueing_delay_ms
            .push(cumulative_ms);
        samples
            .processing_time_ms
            .push(visit.local_processing.as_secs_f64() * SECS_TO_MS);
        samples.slack_d_ms.push(visit.slack_d_ms);
    }

    pub fn per_request_cumulative_queueing_ms(&self) -> Vec<Vec<f64>> {
        let mut request_ids: Vec<_> = self.per_request_cumulative.keys().copied().collect();
        request_ids.sort_unstable();
        request_ids
            .into_iter()
            .map(|id| self.per_request_cumulative[&id].clone())
            .collect()
    }

    pub fn into_stats(&self, microservice_order: &[String]) -> HashMap<String, MicroserviceStats> {
        let mut out = HashMap::new();
        for microservice_id in microservice_order {
            let Some(samples) = self.samples.get(microservice_id) else {
                continue;
            };
            out.insert(
                microservice_id.clone(),
                MicroserviceStats {
                    inter_arrival_ms: consecutive_diffs(&samples.arrival_times_ms),
                    inter_departure_ms: consecutive_diffs(&samples.departure_times_ms),
                    response_time_ms: samples.response_time_ms.clone(),
                    queueing_delay_ms: samples.queueing_delay_ms.clone(),
                    cumulative_queueing_delay_ms: samples.cumulative_queueing_delay_ms.clone(),
                    processing_time_ms: samples.processing_time_ms.clone(),
                    slack_d_ms: samples.slack_d_ms.clone(),
                },
            );
        }
        out
    }
}

fn time_to_ms(time: MonotonicTime) -> f64 {
    time.duration_since(MonotonicTime::EPOCH)
        .as_secs_f64()
        * SECS_TO_MS
}

fn consecutive_diffs(times_ms: &[f64]) -> Vec<f64> {
    if times_ms.len() < 2 {
        return Vec::new();
    }
    let mut sorted = times_ms.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.windows(2).map(|w| w[1] - w[0]).collect()
}
