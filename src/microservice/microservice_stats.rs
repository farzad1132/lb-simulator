use nexosim::time::MonotonicTime;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

const SECS_TO_MS: f64 = 1000.0;

#[derive(Default)]
struct ActiveVisit {
    arrival: MonotonicTime,
    slack_d_ms: f64,
    downstream_response: Duration,
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
    request_ids: Vec<u64>,
    slo_violations: usize,
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
    pub prob_latency_gt_slo: f64,
}

pub struct MicroserviceVisitTracker {
    active: HashMap<(u64, String), ActiveVisit>,
    samples: HashMap<String, MicroserviceSamples>,
    microservice_index: HashMap<String, usize>,
    num_microservices: usize,
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
            samples: HashMap::new(),
            num_microservices: microservice_order.len(),
            microservice_index,
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

    pub fn add_downstream_response(
        &mut self,
        request_id: u64,
        microservice_id: &str,
        response_ms: f64,
    ) {
        if response_ms <= 0.0 {
            return;
        }
        if let Some(visit) = self.active.get_mut(&(request_id, microservice_id.to_string())) {
            visit.downstream_response += Duration::from_secs_f64(response_ms / SECS_TO_MS);
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
    ) -> Option<f64> {
        let key = (request_id, microservice_id.to_string());
        let Some(visit) = self.active.remove(&key) else {
            return None;
        };

        let response = departure.duration_since(visit.arrival);
        let own_response = response.saturating_sub(visit.downstream_response);
        let queueing = own_response.saturating_sub(visit.local_processing);
        let queueing_ms = queueing.as_secs_f64() * SECS_TO_MS;

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
        let response_ms = response.as_secs_f64() * SECS_TO_MS;
        samples.response_time_ms.push(response_ms);
        samples
            .queueing_delay_ms
            .push(queueing_ms);
        samples.cumulative_queueing_delay_ms.push(f64::NAN);
        samples
            .processing_time_ms
            .push(visit.local_processing.as_secs_f64() * SECS_TO_MS);
        samples.slack_d_ms.push(visit.slack_d_ms);
        samples.request_ids.push(request_id);
        if response_ms > visit.slack_d_ms {
            samples.slo_violations += 1;
        }
        Some(response_ms)
    }

    fn hop_queueing_by_request(&self) -> HashMap<u64, Vec<f64>> {
        let mut hop_queueing_by_request: HashMap<u64, Vec<f64>> = HashMap::new();
        for (microservice_id, samples) in &self.samples {
            let Some(ms_index) = self.microservice_index.get(microservice_id) else {
                continue;
            };
            for (idx, &request_id) in samples.request_ids.iter().enumerate() {
                let row = hop_queueing_by_request
                    .entry(request_id)
                    .or_insert_with(|| vec![f64::NAN; self.num_microservices]);
                row[*ms_index] = samples.queueing_delay_ms[idx];
            }
        }
        hop_queueing_by_request
    }

    fn cumulative_prefix(hop_queueing: &[f64]) -> Vec<f64> {
        let mut prefix = 0.0;
        hop_queueing
            .iter()
            .map(|q| {
                if q.is_nan() {
                    f64::NAN
                } else {
                    prefix += *q;
                    prefix
                }
            })
            .collect()
    }

    pub fn finalize_cumulative_metrics(&mut self) {
        let hop_queueing_by_request = self.hop_queueing_by_request();
        let mut prefix_by_request: HashMap<u64, Vec<f64>> = HashMap::new();
        for (request_id, hop_queueing) in &hop_queueing_by_request {
            prefix_by_request.insert(*request_id, Self::cumulative_prefix(hop_queueing));
        }

        for (microservice_id, samples) in &mut self.samples {
            let Some(ms_index) = self.microservice_index.get(microservice_id) else {
                continue;
            };
            samples.cumulative_queueing_delay_ms.clear();
            for &request_id in &samples.request_ids {
                let cumulative = prefix_by_request
                    .get(&request_id)
                    .and_then(|row| row.get(*ms_index).copied())
                    .unwrap_or(f64::NAN);
                samples.cumulative_queueing_delay_ms.push(cumulative);
            }
        }
    }

    pub fn per_request_cumulative_queueing_ms(&self) -> Vec<Vec<f64>> {
        let hop_queueing_by_request = self.hop_queueing_by_request();
        let mut request_ids: Vec<_> = hop_queueing_by_request.keys().copied().collect();
        request_ids.sort_unstable();
        request_ids
            .into_iter()
            .map(|id| {
                Self::cumulative_prefix(
                    hop_queueing_by_request
                        .get(&id)
                        .expect("request id present"),
                )
            })
            .collect()
    }

    pub fn into_stats(&self, microservice_order: &[String]) -> HashMap<String, MicroserviceStats> {
        let mut out = HashMap::new();
        for microservice_id in microservice_order {
            let Some(samples) = self.samples.get(microservice_id) else {
                continue;
            };
            let n = samples.response_time_ms.len();
            let prob_latency_gt_slo = if n == 0 {
                0.0
            } else {
                samples.slo_violations as f64 / n as f64
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
                    prob_latency_gt_slo,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queueing_is_own_response_minus_processing() {
        let mut tracker = MicroserviceVisitTracker::new(&["svc".to_string()]);
        let arrival = MonotonicTime::EPOCH;
        let deadline = arrival + Duration::from_secs(10);
        tracker.record_arrival(1, "svc", arrival, deadline);
        tracker.add_downstream_response(1, "svc", 300.0);
        tracker.add_local_processing(1, "svc", Duration::from_millis(50));
        let departure = arrival + Duration::from_millis(500);
        tracker.finalize_visit(1, "svc", departure);

        let mut tracker = tracker;
        tracker.finalize_cumulative_metrics();
        let stats = tracker.into_stats(&["svc".to_string()]);
        let svc = &stats["svc"];
        assert_eq!(svc.response_time_ms, vec![500.0]);
        assert_eq!(svc.processing_time_ms, vec![50.0]);
        assert_eq!(svc.queueing_delay_ms, vec![150.0]);
        assert_eq!(svc.cumulative_queueing_delay_ms, vec![150.0]);
    }

    #[test]
    fn cumulative_is_prefix_sum_of_queueing_delays_along_chain() {
        assert_eq!(
            MicroserviceVisitTracker::cumulative_prefix(&[10.0, 20.0, 30.0]),
            vec![10.0, 30.0, 60.0]
        );
        let with_nan = MicroserviceVisitTracker::cumulative_prefix(&[f64::NAN, 20.0, 30.0]);
        assert!(with_nan[0].is_nan());
        assert_eq!(with_nan[1], 20.0);
        assert_eq!(with_nan[2], 50.0);

        let chain = ["frontend", "backend1", "backend2"];
        let mut tracker = MicroserviceVisitTracker::new(&chain.map(str::to_string).to_vec());
        let deadline = MonotonicTime::EPOCH + Duration::from_secs(60);

        // frontend: queueing = 10 ms
        tracker.record_arrival(1, "frontend", MonotonicTime::EPOCH, deadline);
        tracker.add_local_processing(1, "frontend", Duration::from_millis(5));
        tracker.finalize_visit(1, "frontend", MonotonicTime::EPOCH + Duration::from_millis(15));

        // backend1: queueing = 20 ms (50 ms downstream + 5 ms proc + 20 ms own queueing)
        let be1_arrival = MonotonicTime::EPOCH + Duration::from_millis(100);
        tracker.record_arrival(1, "backend1", be1_arrival, deadline);
        tracker.add_downstream_response(1, "backend1", 50.0);
        tracker.add_local_processing(1, "backend1", Duration::from_millis(5));
        tracker.finalize_visit(1, "backend1", be1_arrival + Duration::from_millis(75));

        // backend2: queueing = 30 ms
        let be2_arrival = MonotonicTime::EPOCH + Duration::from_millis(200);
        tracker.record_arrival(1, "backend2", be2_arrival, deadline);
        tracker.add_local_processing(1, "backend2", Duration::from_millis(5));
        tracker.finalize_visit(1, "backend2", be2_arrival + Duration::from_millis(35));

        tracker.finalize_cumulative_metrics();
        let per_request = tracker.per_request_cumulative_queueing_ms();
        let stats = tracker.into_stats(&chain.map(str::to_string).to_vec());

        assert_eq!(stats["frontend"].queueing_delay_ms, vec![10.0]);
        assert_eq!(stats["backend1"].queueing_delay_ms, vec![20.0]);
        assert_eq!(stats["backend2"].queueing_delay_ms, vec![30.0]);
        assert_eq!(stats["frontend"].cumulative_queueing_delay_ms, vec![10.0]);
        assert_eq!(stats["backend1"].cumulative_queueing_delay_ms, vec![30.0]);
        assert_eq!(stats["backend2"].cumulative_queueing_delay_ms, vec![60.0]);
        assert_eq!(per_request, vec![vec![10.0, 30.0, 60.0]]);
    }
}
