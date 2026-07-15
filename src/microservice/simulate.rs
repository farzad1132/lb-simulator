use clap::ValueEnum;
use nexosim::model::{Context, Model};
use nexosim::ports::{
    EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue,
};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::balancer::{DownstreamBalancer, EdgeBalancer, OutboundGateway, ReplicaBalancer};
#[cfg(test)]
use super::callgraph::ApiLoad;
use super::callgraph::{CallGraph, LoadSpec, load_spec_from_file};
use super::hop::{
    CompletedRequest, Hop, OutboundCall, ReplicaInput, microservice_for_endpoint, sample_exp,
};
use super::microservice_stats::{MicroserviceStats, MicroserviceVisitTracker};
use super::occupancy::OccupancyAccumulator;
use super::replica::{Replica, ReplicaConfig};
use super::trace::MsTracer;
use crate::policy::LoadBalancePolicyKind;
use crate::rng;
use crate::scheduling::SchedulingPolicyKind;
use crate::sim_util;
use crate::subset::{self, SubsetPolicyKind};

const HUMAN_PERCENTILES: [f64; 12] = [
    1.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 99.0, 100.0,
];
const SECS_TO_MS: f64 = 1000.0;

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Human,
    Json,
}

pub struct MsArgs {
    pub callgraph: PathBuf,
    pub load_file: PathBuf,
    pub n: u32,
    pub lb_policy: LoadBalancePolicyKind,
    pub lb_subset_size: u32,
    pub lb_subset_policy: SubsetPolicyKind,
    pub seed: Option<u64>,
    pub rps: Option<f64>,
    pub slo_ms: Option<f64>,
    pub format: OutputFormat,
    pub trace: bool,
    pub trace_limit: u32,
    pub scale: u32,
    pub verbose: u8,
    pub scheduling: SchedulingPolicyKind,
    pub force_fixed_svc: bool,
}

#[derive(Serialize)]
pub struct ApiStats {
    pub e2e_ms: Vec<f64>,
    pub processing_time_ms: Vec<f64>,
    pub unloaded_latency_p99_ms: f64,
    pub slo_latency_ms: f64,
    pub prob_latency_gt_slo: f64,
}

#[derive(Serialize)]
pub struct MsStats {
    pub microservice_utilization_pct: HashMap<String, f64>,
    pub server_utilization_pct: HashMap<String, HashMap<usize, f64>>,
    pub server_avg_queue_inflight: HashMap<String, HashMap<usize, f64>>,
    pub by_api: HashMap<String, ApiStats>,
    pub by_microservice: HashMap<String, MicroserviceStats>,
    pub microservice_order: Vec<String>,
    pub total_processing_p99_ms: f64,
    pub per_request_cumulative_queueing_ms: Vec<Vec<f64>>,
}

#[derive(Deserialize, Serialize)]
struct UserArrival {
    #[serde(skip)]
    graph: Arc<CallGraph>,
    #[serde(skip)]
    load: Arc<LoadSpec>,
    #[serde(skip)]
    edge_balancers: HashMap<String, Output<Hop>>,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
    #[serde(skip)]
    next_request_id: Arc<AtomicU64>,
}

impl UserArrival {
    fn new(
        graph: Arc<CallGraph>,
        load: Arc<LoadSpec>,
        edge_balancers: HashMap<String, Output<Hop>>,
        tracer: Option<Arc<MsTracer>>,
        next_request_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            graph,
            load,
            edge_balancers,
            tracer,
            next_request_id,
        }
    }
}

#[Model]
impl UserArrival {
    async fn inject(&mut self, api: String, cx: &Context<Self>) {
        let endpoint = match self.graph.entrypoints.get(&api) {
            Some(e) => e.clone(),
            None => {
                eprintln!("arrival: unknown api {}", api);
                return;
            }
        };
        let output = match self.edge_balancers.get_mut(&api) {
            Some(o) => o,
            None => {
                eprintln!("arrival: no edge balancer for api {}", api);
                return;
            }
        };
        let (request_id, trace) = {
            let id = self.next_request_id.fetch_add(1, Ordering::Relaxed) + 1;
            let trace = self
                .tracer
                .as_ref()
                .map(|t| t.should_trace(id))
                .unwrap_or(false);
            (id, trace)
        };
        let slo_ms = match self.load.get(&api) {
            Some(spec) => spec.slo_ms,
            None => {
                eprintln!("arrival: no load spec for api {}", api);
                return;
            }
        };
        let now = cx.time();
        let hop = Hop {
            request_id,
            trace,
            api,
            endpoint: endpoint.clone(),
            sibling_index: 0,
            start: now,
            deadline: now + Duration::from_secs_f64(slo_ms / SECS_TO_MS),
            duration: Duration::ZERO,
            processing_time: Duration::ZERO,
            caller: None,
            outbound_release: None,
            slot_release: None,
        };
        if let Some(tracer) = &self.tracer {
            tracer.log(
                trace,
                cx.time(),
                request_id,
                &format!("UserArrival api={} entry={endpoint}", hop.api),
            );
        }
        output.send(hop).await;
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
    sorted[idx]
}

fn split_by_rps(n: u32, load: &LoadSpec) -> HashMap<String, u32> {
    let total_rps: f64 = load.values().map(|spec| spec.rps).sum();
    if total_rps <= 0.0 || n == 0 {
        return HashMap::new();
    }
    let mut counts = HashMap::new();
    let mut assigned = 0u32;
    let mut apis: Vec<_> = load.keys().cloned().collect();
    apis.sort();
    for (i, api) in apis.iter().enumerate() {
        let rps = load[api].rps;
        let count = if i + 1 == apis.len() {
            n - assigned
        } else {
            ((f64::from(n) * rps / total_rps).round() as u32).min(n - assigned)
        };
        counts.insert(api.clone(), count);
        assigned += count;
    }
    counts
}

fn apply_load_overrides(load: &mut LoadSpec, rps: Option<f64>, slo_ms: Option<f64>) {
    for spec in load.values_mut() {
        if let Some(rps) = rps {
            spec.rps = rps;
        }
        if let Some(slo_ms) = slo_ms {
            spec.slo_ms = slo_ms;
        }
    }
}

fn new_bench() -> SimInit {
    SimInit::with_num_threads(1)
}

fn downstream_targets(graph: &CallGraph, microservice_id: &str) -> HashSet<String> {
    let mut targets = HashSet::new();
    for (endpoint, owner) in &graph.endpoint_microservice {
        if owner != microservice_id {
            continue;
        }
        if let Some(edges) = graph.children.get(endpoint) {
            for (target, _) in edges {
                if let Some(downstream) = graph.endpoint_microservice.get(target) {
                    targets.insert(downstream.clone());
                }
            }
        }
    }
    targets
}

fn all_downstream_targets(graph: &CallGraph) -> HashSet<String> {
    let mut targets = HashSet::new();
    for microservice_id in &graph.microservice_order {
        targets.extend(downstream_targets(graph, microservice_id));
    }
    targets
}

fn entry_microservices_by_api(graph: &CallGraph) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (api, endpoint) in &graph.entrypoints {
        if let Ok(ms) = microservice_for_endpoint(graph, endpoint) {
            map.insert(api.clone(), ms);
        }
    }
    map
}

fn poisson_arrivals(
    sim: &Simulation,
    input: &EventId<String>,
    api: String,
    rps: f64,
    n: u32,
) -> Result<(), SchedulingError> {
    if n == 0 || rps <= 0.0 {
        return Ok(());
    }
    let arrival_mean = 1.0 / rps as f32;
    let scheduler = sim.scheduler();
    let mut offset = Duration::ZERO;

    rng::with_rng(|rng| {
        for _ in 0..n {
            offset += Duration::from_secs_f32(sample_exp(rng, arrival_mean));
            scheduler.schedule_event(offset, input, api.clone())?;
        }
        Ok::<(), SchedulingError>(())
    })?;
    Ok(())
}

fn new_api_stats() -> ApiStats {
    ApiStats {
        e2e_ms: Vec::new(),
        processing_time_ms: Vec::new(),
        unloaded_latency_p99_ms: 0.0,
        slo_latency_ms: 0.0,
        prob_latency_gt_slo: 0.0,
    }
}

fn finalize_api_stats(stats: &mut ApiStats) {
    let mut processing = stats.processing_time_ms.clone();
    processing.sort_by(f64::total_cmp);
    stats.unloaded_latency_p99_ms = percentile(&processing, 99.0);
    stats.prob_latency_gt_slo = if stats.e2e_ms.is_empty() || stats.slo_latency_ms <= 0.0 {
        0.0
    } else {
        let violations = stats
            .e2e_ms
            .iter()
            .filter(|&&e2e| e2e > stats.slo_latency_ms)
            .count();
        violations as f64 / stats.e2e_ms.len() as f64
    };
}

fn calculate_stats(
    completed: &mut EventQueueReader<CompletedRequest>,
    busy_time: &HashMap<String, HashMap<usize, Duration>>,
    replica_occupancy: &mut HashMap<String, HashMap<usize, OccupancyAccumulator>>,
    balancer_queue_occupancy: &mut HashMap<String, OccupancyAccumulator>,
    graph: &CallGraph,
    load: &LoadSpec,
    sim_start: MonotonicTime,
    sim_end: MonotonicTime,
    visit_tracker: &MicroserviceVisitTracker,
    lb_policy: LoadBalancePolicyKind,
    downstream_targets: &HashSet<String>,
) -> Option<MsStats> {
    let mut by_api: HashMap<String, ApiStats> = HashMap::new();

    while let Some(req) = completed.try_read() {
        let e2e_ms = req.finish.duration_since(req.start).as_secs_f64() * SECS_TO_MS;
        let proc_ms = req.processing_time.as_secs_f64() * SECS_TO_MS;
        let entry = by_api.entry(req.api.clone()).or_insert_with(new_api_stats);
        entry.e2e_ms.push(e2e_ms);
        entry.processing_time_ms.push(proc_ms);
    }

    if by_api.is_empty() {
        return None;
    }

    for (api, stats) in by_api.iter_mut() {
        if stats.processing_time_ms.is_empty() {
            continue;
        }
        if let Some(spec) = load.get(api) {
            stats.slo_latency_ms = spec.slo_ms;
        }
        finalize_api_stats(stats);
    }

    let mut microservice_utilization_pct = HashMap::new();
    let mut server_utilization_pct = HashMap::new();
    let obs_secs = sim_end.duration_since(sim_start).as_secs_f64();
    if obs_secs > 0.0 {
        for microservice_id in &graph.microservice_order {
            if let Some(spec) = graph.microservices.get(microservice_id) {
                let server_busy = busy_time.get(microservice_id);
                let total_busy: Duration = server_busy
                    .map(|m| m.values().copied().sum())
                    .unwrap_or(Duration::ZERO);
                let pct = total_busy.as_secs_f64() / (obs_secs * f64::from(spec.cpu)) * 100.0;
                microservice_utilization_pct.insert(microservice_id.clone(), pct);

                let concurrency = (spec.cpu / spec.replicas).max(1);
                let mut by_server = HashMap::new();
                let n_servers = spec.replicas as usize;
                for i in 0..n_servers {
                    let busy = server_busy
                        .and_then(|m| m.get(&i).copied())
                        .unwrap_or(Duration::ZERO);
                    let server_pct =
                        busy.as_secs_f64() / (obs_secs * f64::from(concurrency)) * 100.0;
                    by_server.insert(i, server_pct);
                }
                server_utilization_pct.insert(microservice_id.clone(), by_server);
            }
        }
    }

    let mut server_avg_queue_inflight = HashMap::new();
    for microservice_id in &graph.microservice_order {
        let n_servers = graph.microservices[microservice_id].replicas as usize;
        let mut by_server = HashMap::new();
        for i in 0..n_servers {
            let avg = replica_occupancy
                .get_mut(microservice_id)
                .and_then(|m| m.get_mut(&i))
                .map(|acc| acc.finalize(sim_end, sim_start))
                .unwrap_or(0.0);
            by_server.insert(i, avg);
        }
        server_avg_queue_inflight.insert(microservice_id.clone(), by_server);
    }

    if lb_policy.is_centralized() {
        let mut sorted_targets: Vec<_> = downstream_targets.iter().cloned().collect();
        sorted_targets.sort();
        for target in sorted_targets {
            let n = graph.microservices[&target].replicas as f64;
            if n <= 0.0 {
                continue;
            }
            let q_avg = balancer_queue_occupancy
                .get_mut(&target)
                .map(|acc| acc.finalize(sim_end, sim_start))
                .unwrap_or(0.0);
            let fair_share = q_avg / n;
            if let Some(servers) = server_avg_queue_inflight.get_mut(&target) {
                for avg in servers.values_mut() {
                    *avg += fair_share;
                }
            }
        }
    }

    let by_microservice = visit_tracker.into_stats(&graph.microservice_order);
    let per_request_cumulative_queueing_ms = visit_tracker.per_request_cumulative_queueing_ms();

    let mut all_processing: Vec<f64> = Vec::new();
    for stats in by_api.values() {
        all_processing.extend(&stats.processing_time_ms);
    }
    all_processing.sort_by(f64::total_cmp);
    let total_processing_p99_ms = percentile(&all_processing, 99.0);

    Some(MsStats {
        microservice_utilization_pct,
        server_utilization_pct,
        server_avg_queue_inflight,
        by_api,
        by_microservice,
        microservice_order: graph.microservice_order.clone(),
        total_processing_p99_ms,
        per_request_cumulative_queueing_ms,
    })
}

pub fn run(args: &MsArgs) -> Result<Option<MsStats>, Box<dyn std::error::Error>> {
    if args.lb_policy.uses_shared_downstream() && args.lb_subset_size > 0 {
        return Err(
            "--lb-subset-size is not supported with --lb-policy cl, cl-lr, centralized, or corr".into(),
        );
    }
    rng::enter_run(args.seed);
    let result = run_inner(args);
    rng::exit_run();
    result
}

fn run_inner(args: &MsArgs) -> Result<Option<MsStats>, Box<dyn std::error::Error>> {
    let mut graph = CallGraph::from_file(&args.callgraph)?;
    graph.apply_scale(args.scale)?;
    graph.force_fixed_svc = args.force_fixed_svc;
    let graph = Arc::new(graph);
    let mut load = load_spec_from_file(&args.load_file)?;
    apply_load_overrides(&mut load, args.rps, args.slo_ms);
    graph.validate_load(&load)?;
    let load = Arc::new(load);

    let next_request_id = Arc::new(AtomicU64::new(0));

    let visit_tracker = Arc::new(Mutex::new(MicroserviceVisitTracker::new(
        &graph.microservice_order,
    )));

    let busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    for microservice_id in &graph.microservice_order {
        let n_servers = graph.microservices[microservice_id].replicas as usize;
        let mut by_server = HashMap::new();
        for i in 0..n_servers {
            by_server.insert(i, Duration::ZERO);
        }
        busy_time
            .lock()
            .unwrap()
            .insert(microservice_id.clone(), by_server);
    }

    let replica_occupancy: Arc<Mutex<HashMap<String, HashMap<usize, OccupancyAccumulator>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    for microservice_id in &graph.microservice_order {
        let n_servers = graph.microservices[microservice_id].replicas as usize;
        let mut by_server = HashMap::new();
        for i in 0..n_servers {
            by_server.insert(i, OccupancyAccumulator::default());
        }
        replica_occupancy
            .lock()
            .unwrap()
            .insert(microservice_id.clone(), by_server);
    }

    let balancer_queue_occupancy: Arc<Mutex<HashMap<String, OccupancyAccumulator>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let tracer = if args.trace {
        Some(MsTracer::new(args.trace_limit))
    } else {
        None
    };

    let entry_microservices = entry_microservices_by_api(graph.as_ref());
    let microservice_server_counts: HashMap<String, u32> = graph
        .microservices
        .iter()
        .map(|(id, spec)| (id.clone(), spec.replicas))
        .collect();

    let mut bench = new_bench();
    let (sink, mut completed) = event_queue(SinkState::Enabled);

    let mut server_mailboxes: HashMap<(String, usize), Mailbox<Replica>> = HashMap::new();
    for microservice_id in &graph.microservice_order {
        let n_servers = graph.microservices[microservice_id].replicas as usize;
        for i in 0..n_servers {
            server_mailboxes.insert((microservice_id.clone(), i), Mailbox::new());
        }
    }

    struct PendingEdgeBalancer {
        balancer: EdgeBalancer,
        mailbox: Mailbox<EdgeBalancer>,
        api: String,
        entry_microservice: String,
        server_indices: Vec<usize>,
    }

    struct PendingReplicaBalancer {
        balancer: ReplicaBalancer,
        mailbox: Mailbox<ReplicaBalancer>,
        microservice_id: String,
        server_idx: usize,
        downstream_indices: HashMap<String, Vec<usize>>,
    }

    struct PendingDownstreamBalancer {
        balancer: DownstreamBalancer,
        mailbox: Mailbox<DownstreamBalancer>,
        target_microservice: String,
        server_indices: Vec<usize>,
    }

    struct PendingOutboundGateway {
        gateway: OutboundGateway,
        mailbox: Mailbox<OutboundGateway>,
        microservice_id: String,
        server_idx: usize,
    }

    let use_shared_downstream = args.lb_policy.uses_shared_downstream();

    let mut pending_edge_balancers: Vec<PendingEdgeBalancer> = Vec::new();
    let mut edge_balancer_inputs: HashMap<String, Output<Hop>> = HashMap::new();
    let mut edge_balancer_addresses: HashMap<String, nexosim::simulation::Address<EdgeBalancer>> =
        HashMap::new();

    let mut apis: Vec<_> = graph.entrypoints.keys().cloned().collect();
    apis.sort();
    for (api_index, api) in apis.iter().enumerate() {
        let entry_endpoint = &graph.entrypoints[api];
        let entry_microservice = microservice_for_endpoint(graph.as_ref(), entry_endpoint)?;
        let n_servers = graph.microservices[&entry_microservice].replicas as usize;
        let server_indices = if use_shared_downstream {
            (0..n_servers).collect()
        } else {
            subset::assign_subset(
                args.lb_subset_policy,
                n_servers,
                api_index,
                args.lb_subset_size,
            )
        };
        if args.verbose >= 1 {
            eprintln!("api {api} subset: {server_indices:?}");
        }

        let balancer = EdgeBalancer::new(
            args.lb_policy.ingress_policy(),
            args.lb_policy,
            api.clone(),
            n_servers,
            server_indices.clone(),
            tracer.clone(),
        );
        let mailbox = Mailbox::new();
        let address = mailbox.address();

        let mut input = Output::default();
        input.connect(EdgeBalancer::input, &mailbox);
        edge_balancer_inputs.insert(api.clone(), input);
        edge_balancer_addresses.insert(api.clone(), address.clone());

        pending_edge_balancers.push(PendingEdgeBalancer {
            balancer,
            mailbox,
            api: api.clone(),
            entry_microservice,
            server_indices,
        });
    }

    for pending in &mut pending_edge_balancers {
        for &server_idx in &pending.server_indices {
            if let Some(mb) =
                server_mailboxes.get(&(pending.entry_microservice.clone(), server_idx))
            {
                pending.balancer.outputs[server_idx].connect(Replica::input, mb);
            }
        }
    }

    let mut pending_replica_balancers: Vec<PendingReplicaBalancer> = Vec::new();
    let mut replica_balancer_outbound: HashMap<(String, usize), Output<OutboundCall>> =
        HashMap::new();
    let mut replica_balancer_addresses: HashMap<
        (String, usize),
        nexosim::simulation::Address<ReplicaBalancer>,
    > = HashMap::new();

    let mut pending_downstream_balancers: Vec<PendingDownstreamBalancer> = Vec::new();
    let mut pending_outbound_gateways: Vec<PendingOutboundGateway> = Vec::new();
    let mut outbound_gateway_outbound: HashMap<(String, usize), Output<OutboundCall>> =
        HashMap::new();
    let mut outbound_gateway_addresses: HashMap<
        (String, usize),
        nexosim::simulation::Address<OutboundGateway>,
    > = HashMap::new();
    let mut downstream_balancer_addresses: HashMap<
        String,
        nexosim::simulation::Address<DownstreamBalancer>,
    > = HashMap::new();

    if use_shared_downstream {
        let mut sorted_targets: Vec<_> = all_downstream_targets(&graph).into_iter().collect();
        sorted_targets.sort();

        for target in &sorted_targets {
            let n_servers = graph.microservices[target].replicas as usize;
            let server_indices: Vec<usize> = (0..n_servers).collect();
            if args.verbose >= 1 {
                eprintln!("downstream balancer {target} subset: {server_indices:?}");
            }

            let balancer = DownstreamBalancer::new(
                target.clone(),
                n_servers,
                server_indices.clone(),
                args.lb_policy,
                tracer.clone(),
                balancer_queue_occupancy.clone(),
            );
            let mailbox = Mailbox::new();
            let address = mailbox.address();
            downstream_balancer_addresses.insert(target.clone(), address);

            pending_downstream_balancers.push(PendingDownstreamBalancer {
                balancer,
                mailbox,
                target_microservice: target.clone(),
                server_indices,
            });
        }

        for pending in &mut pending_downstream_balancers {
            for &server_idx in &pending.server_indices {
                if let Some(mb) =
                    server_mailboxes.get(&(pending.target_microservice.clone(), server_idx))
                {
                    pending.balancer.outputs[server_idx].connect(Replica::input, mb);
                }
            }
        }

        for microservice_id in &graph.microservice_order {
            let n_servers = graph.microservices[microservice_id].replicas as usize;

            for server_idx in 0..n_servers {
                let targets = downstream_targets(&graph, microservice_id);
                let mut downstream_outputs = HashMap::new();
                let mut downstream_releases = HashMap::new();
                for target in &targets {
                    let db_address = downstream_balancer_addresses
                        .get(target)
                        .expect("downstream balancer address");
                    let mut out = Output::default();
                    out.connect(DownstreamBalancer::outbound, db_address);
                    downstream_outputs.insert(target.clone(), out);
                    let mut release = Output::default();
                    release.connect(DownstreamBalancer::release, db_address);
                    downstream_releases.insert(target.clone(), release);
                }

                let gateway = OutboundGateway::new(downstream_outputs, downstream_releases);
                let mailbox = Mailbox::new();
                let address = mailbox.address();

                let mut outbound = Output::default();
                outbound.connect(OutboundGateway::input, &mailbox);
                outbound_gateway_outbound.insert((microservice_id.clone(), server_idx), outbound);
                outbound_gateway_addresses.insert((microservice_id.clone(), server_idx), address);

                pending_outbound_gateways.push(PendingOutboundGateway {
                    gateway,
                    mailbox,
                    microservice_id: microservice_id.clone(),
                    server_idx,
                });
            }
        }
    } else {
        for microservice_id in &graph.microservice_order {
            let n_servers = graph.microservices[microservice_id].replicas as usize;

            for server_idx in 0..n_servers {
                let mut downstream_indices = HashMap::new();
                for target in downstream_targets(&graph, microservice_id) {
                    let target_servers = graph.microservices[&target].replicas as usize;
                    let indices = subset::assign_subset(
                        args.lb_subset_policy,
                        target_servers,
                        server_idx,
                        args.lb_subset_size,
                    );
                    if args.verbose >= 1 {
                        eprintln!(
                            "server {microservice_id}/{server_idx} -> {target} subset: {indices:?}"
                        );
                    }
                    downstream_indices.insert(target.clone(), indices);
                }

                let balancer = ReplicaBalancer::new(
                    args.lb_policy.build(),
                    args.lb_policy,
                    microservice_id.clone(),
                    server_idx,
                    downstream_indices.clone(),
                    &microservice_server_counts,
                    tracer.clone(),
                );
                let mailbox = Mailbox::new();
                let address = mailbox.address();

                let mut outbound = Output::default();
                outbound.connect(ReplicaBalancer::outbound, &mailbox);
                replica_balancer_outbound.insert((microservice_id.clone(), server_idx), outbound);
                replica_balancer_addresses
                    .insert((microservice_id.clone(), server_idx), address.clone());

                pending_replica_balancers.push(PendingReplicaBalancer {
                    balancer,
                    mailbox,
                    microservice_id: microservice_id.clone(),
                    server_idx,
                    downstream_indices: downstream_indices.clone(),
                });
            }
        }

        for pending in &mut pending_replica_balancers {
            for (target_microservice, indices) in &pending.downstream_indices {
                let n_target = graph.microservices[target_microservice].replicas as usize;
                let outputs = pending
                    .balancer
                    .downstream_outputs
                    .get_mut(target_microservice)
                    .unwrap_or_else(|| {
                        panic!("missing downstream outputs for {target_microservice}")
                    });
                if outputs.len() != n_target {
                    *outputs = (0..n_target).map(|_| Output::default()).collect();
                }
                for &server_idx in indices {
                    if let Some(mb) =
                        server_mailboxes.get(&(target_microservice.clone(), server_idx))
                    {
                        outputs[server_idx].connect(Replica::input, mb);
                    }
                }
            }
        }
    }

    let mut return_outputs: HashMap<(String, usize), Output<ReplicaInput>> = HashMap::new();
    for (key, mb) in &server_mailboxes {
        let mut output = Output::default();
        output.connect(Replica::input, mb);
        return_outputs.insert(key.clone(), output);
    }

    let mut completed_output = Output::default();
    completed_output.connect_sink(sink);

    let apis_by_entry_microservice: HashMap<String, Vec<String>> = {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (api, microservice) in &entry_microservices {
            map.entry(microservice.clone())
                .or_default()
                .push(api.clone());
        }
        for apis in map.values_mut() {
            apis.sort();
        }
        map
    };

    for pending in pending_edge_balancers {
        bench = bench.add_model(
            pending.balancer,
            pending.mailbox,
            &format!("edge-balancer-{}", pending.api),
        );
    }

    for pending in pending_replica_balancers {
        bench = bench.add_model(
            pending.balancer,
            pending.mailbox,
            &format!(
                "replica-balancer-{}-{}",
                pending.microservice_id, pending.server_idx
            ),
        );
    }

    for pending in pending_downstream_balancers {
        bench = bench.add_model(
            pending.balancer,
            pending.mailbox,
            &format!("downstream-balancer-{}", pending.target_microservice),
        );
    }

    for pending in pending_outbound_gateways {
        bench = bench.add_model(
            pending.gateway,
            pending.mailbox,
            &format!(
                "outbound-gateway-{}-{}",
                pending.microservice_id, pending.server_idx
            ),
        );
    }

    let downstream_target_set: HashSet<String> = if use_shared_downstream {
        all_downstream_targets(&graph)
    } else {
        HashSet::new()
    };

    let mut pull_registrations: Vec<(EventId<()>, u32)> = Vec::new();

    for microservice_id in &graph.microservice_order {
        let spec = &graph.microservices[microservice_id];
        let n_servers = spec.replicas as usize;
        let concurrency = (spec.cpu / spec.replicas).max(1);

        for i in 0..n_servers {
            let mb = server_mailboxes
                .remove(&(microservice_id.clone(), i))
                .expect("server mailbox");
            let outbound = if use_shared_downstream {
                outbound_gateway_outbound
                    .get(&(microservice_id.clone(), i))
                    .cloned()
                    .expect("outbound gateway outbound")
            } else {
                replica_balancer_outbound
                    .get(&(microservice_id.clone(), i))
                    .cloned()
                    .expect("replica balancer outbound")
            };

            let mut edge_releases = HashMap::new();
            if let Some(apis) = apis_by_entry_microservice.get(microservice_id) {
                for api in apis {
                    let edge_address = edge_balancer_addresses
                        .get(api)
                        .expect("edge balancer address");
                    let mut release = Output::default();
                    release.connect(EdgeBalancer::release, edge_address);
                    edge_releases.insert(api.clone(), release);
                }
            }

            let mut outbound_release = Output::default();
            if use_shared_downstream {
                let gw_address = outbound_gateway_addresses
                    .get(&(microservice_id.clone(), i))
                    .expect("outbound gateway address");
                outbound_release.connect(OutboundGateway::release, gw_address);
            } else {
                let rb_address = replica_balancer_addresses
                    .get(&(microservice_id.clone(), i))
                    .expect("replica balancer address");
                outbound_release.connect(ReplicaBalancer::release_outbound, rb_address);
            }

            let mut pull_output = None;
            if args.lb_policy.is_centralized() && downstream_target_set.contains(microservice_id) {
                let db_address = downstream_balancer_addresses
                    .get(microservice_id)
                    .expect("downstream balancer address");
                let mut output = Output::default();
                output.connect(DownstreamBalancer::pull, db_address);
                pull_output = Some(output);
            }

            let pull_input = if pull_output.is_some() {
                Some(
                    EventSource::new()
                        .connect(Replica::request_pull, &mb)
                        .register(&mut bench),
                )
            } else {
                None
            };

            let replica = Replica::new(ReplicaConfig {
                graph: graph.clone(),
                microservice_id: microservice_id.clone(),
                server_idx: i,
                max_concurrency: concurrency,
                busy_time: busy_time.clone(),
                replica_occupancy: replica_occupancy.clone(),
                visit_tracker: visit_tracker.clone(),
                balancer_outbound: outbound,
                outbound_release,
                edge_releases,
                return_outputs: return_outputs.clone(),
                completed: completed_output.clone(),
                tracer: tracer.clone(),
                pull_output,
                scheduling: args.scheduling,
            });
            bench = bench.add_model(replica, mb, &format!("{microservice_id}-server-{i}"));
            if let Some(pull_input) = pull_input {
                pull_registrations.push((pull_input, concurrency));
            }
        }
    }

    let arrival = UserArrival::new(
        graph.clone(),
        load.clone(),
        edge_balancer_inputs,
        tracer,
        next_request_id,
    );
    let arrival_mb = Mailbox::new();
    let arrival_input = EventSource::new()
        .connect(UserArrival::inject, &arrival_mb)
        .register(&mut bench);
    bench = bench.add_model(arrival, arrival_mb, "user-arrival");

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    for (pull_input, concurrency) in pull_registrations {
        sim_util::schedule_initial_pulls(&simu, &[pull_input], concurrency)?;
    }

    let counts = split_by_rps(args.n, &load);
    let mut apis: Vec<_> = counts.keys().cloned().collect();
    apis.sort();
    for api in apis {
        let count = counts[&api];
        let rps = load[&api].rps;
        poisson_arrivals(&simu, &arrival_input, api, rps, count)?;
    }

    simu.run()?;

    let sim_end = simu.time();
    let busy = busy_time.lock().unwrap().clone();
    let mut replica_occ = replica_occupancy.lock().unwrap().clone();
    let mut balancer_occ = balancer_queue_occupancy.lock().unwrap().clone();
    let tracker = visit_tracker.lock().unwrap();
    Ok(calculate_stats(
        &mut completed,
        &busy,
        &mut replica_occ,
        &mut balancer_occ,
        graph.as_ref(),
        &load,
        t0,
        sim_end,
        &tracker,
        args.lb_policy,
        &downstream_target_set,
    ))
}

pub fn print_human_stats(stats: &MsStats) {
    println!("microservice utilization (%):");
    let mut microservice_ids: Vec<_> = stats.microservice_utilization_pct.keys().cloned().collect();
    microservice_ids.sort();
    for microservice_id in microservice_ids {
        println!(
            "  {}: {:.2}",
            microservice_id, stats.microservice_utilization_pct[&microservice_id]
        );
        if let Some(servers) = stats.server_utilization_pct.get(&microservice_id) {
            let mut indices: Vec<_> = servers.keys().copied().collect();
            indices.sort_unstable();
            for idx in indices {
                println!("    server {}: {:.2}", idx, servers[&idx]);
            }
        }
    }
    println!(
        "total processing p99: {:.4} ms",
        stats.total_processing_p99_ms
    );
    for (api, api_stats) in &stats.by_api {
        println!("API {api}:");
        println!(
            "  unloaded latency (p99): {:.4} ms",
            api_stats.unloaded_latency_p99_ms
        );
        println!("  SLO latency: {:.4} ms", api_stats.slo_latency_ms);
        println!("  P(latency > SLO): {:.6}", api_stats.prob_latency_gt_slo);
        print_percentile_table("  e2e latency (ms):", &mut api_stats.e2e_ms.clone());
        print_percentile_table(
            "  processing time (ms):",
            &mut api_stats.processing_time_ms.clone(),
        );
    }
}

fn print_percentile_table(label: &str, values: &mut [f64]) {
    values.sort_by(f64::total_cmp);
    println!("{label}");
    print!("  ");
    for pct in HUMAN_PERCENTILES {
        print!("p{pct:.0}: {:>8.4}  ", percentile(values, pct));
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn client_server_args(seed: u64) -> MsArgs {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        MsArgs {
            callgraph: root.join("tests/client_server/single_replica/callgraph.json"),
            load_file: root.join("tests/client_server/single_replica/load.json"),
            n: 5_000,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(seed),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        }
    }

    #[test]
    fn fanin_multi_replica_seed_1() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let stats = run(&MsArgs {
            callgraph: root.join("tests/fanin/multi/callgraph.json"),
            load_file: root.join("tests/fanin/multi/load.json"),
            n: 1000,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(1),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats");
        assert!(!stats.by_api.is_empty());

        let graph = CallGraph::from_file(&root.join("tests/fanin/multi/callgraph.json")).unwrap();
        for microservice_id in &graph.microservice_order {
            let spec = &graph.microservices[microservice_id];
            let servers = stats
                .server_utilization_pct
                .get(microservice_id)
                .expect("server utilization for microservice");
            assert_eq!(servers.len(), spec.replicas as usize);
            for i in 0..spec.replicas as usize {
                let pct = servers[&i];
                assert!(
                    (0.0..=100.0).contains(&pct),
                    "server {i} of {microservice_id}: {pct}% out of range"
                );
            }
        }
    }

    #[test]
    fn single_server_utilization_matches_overall() {
        let args = client_server_args(7);
        let stats = run(&args).unwrap().expect("stats");
        let overall = stats.microservice_utilization_pct["server"];
        let server = stats.server_utilization_pct["server"][&0];
        assert!(
            (overall - server).abs() < 1e-9,
            "single server: overall={overall}, server={server}"
        );
    }

    #[test]
    fn server_utilization_is_reproducible() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let args = MsArgs {
            callgraph: root.join("tests/fanin/multi/callgraph.json"),
            load_file: root.join("tests/fanin/multi/load.json"),
            n: 1000,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(99),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        };
        let first = run(&args).unwrap().expect("stats");
        let second = run(&args).unwrap().expect("stats");
        assert_eq!(
            first.microservice_utilization_pct,
            second.microservice_utilization_pct
        );
        for microservice_id in first.server_utilization_pct.keys() {
            let mut first_vals: Vec<_> = first.server_utilization_pct[microservice_id]
                .values()
                .copied()
                .collect();
            let mut second_vals: Vec<_> = second.server_utilization_pct[microservice_id]
                .values()
                .copied()
                .collect();
            first_vals.sort_by(f64::total_cmp);
            second_vals.sort_by(f64::total_cmp);
            assert_eq!(first_vals, second_vals, "microservice {microservice_id}");
        }
    }

    #[test]
    fn server_avg_queue_inflight_present_and_non_negative() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let stats = run(&MsArgs {
            callgraph: root.join("tests/fanin/multi/callgraph.json"),
            load_file: root.join("tests/fanin/multi/load.json"),
            n: 1000,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(1),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats");

        let graph = CallGraph::from_file(&root.join("tests/fanin/multi/callgraph.json")).unwrap();
        for microservice_id in &graph.microservice_order {
            let spec = &graph.microservices[microservice_id];
            let servers = stats
                .server_avg_queue_inflight
                .get(microservice_id)
                .expect("avg queue+inflight for microservice");
            assert_eq!(servers.len(), spec.replicas as usize);
            for i in 0..spec.replicas as usize {
                let avg = servers[&i];
                assert!(avg.is_finite() && avg >= 0.0, "server {i} of {microservice_id}: {avg}");
            }
        }
    }

    #[test]
    fn server_avg_queue_inflight_is_reproducible() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let args = MsArgs {
            callgraph: root.join("tests/fanin/multi/callgraph.json"),
            load_file: root.join("tests/fanin/multi/load.json"),
            n: 1000,
            lb_policy: LoadBalancePolicyKind::Cl,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(99),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        };
        let first = run(&args).unwrap().expect("stats");
        let second = run(&args).unwrap().expect("stats");
        assert_eq!(
            first.server_avg_queue_inflight,
            second.server_avg_queue_inflight
        );
    }

    #[test]
    fn centralized_fair_share_adds_balancer_queue_to_downstream_replicas() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let stats = run(&MsArgs {
            callgraph: root.join("tests/chain/3/callgraph.json"),
            load_file: root.join("tests/chain/3/load.json"),
            n: 5000,
            lb_policy: LoadBalancePolicyKind::Centralized,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(42),
            rps: Some(500.0),
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats");

        let cl_stats = run(&MsArgs {
            callgraph: root.join("tests/chain/3/callgraph.json"),
            load_file: root.join("tests/chain/3/load.json"),
            n: 5000,
            lb_policy: LoadBalancePolicyKind::Cl,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(42),
            rps: Some(500.0),
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats");

        let downstream = "backend2";
        let centralized_vals: Vec<f64> = stats.server_avg_queue_inflight[downstream]
            .values()
            .copied()
            .collect();
        let cl_vals: Vec<f64> = cl_stats.server_avg_queue_inflight[downstream]
            .values()
            .copied()
            .collect();
        let centralized_mean =
            centralized_vals.iter().sum::<f64>() / centralized_vals.len() as f64;
        let cl_mean = cl_vals.iter().sum::<f64>() / cl_vals.len() as f64;
        assert!(
            centralized_mean > 0.0,
            "centralized downstream avg occupancy should be positive under load"
        );
        let centralized_sum: f64 = centralized_vals.iter().sum();
        let cl_sum: f64 = cl_vals.iter().sum();
        assert!(
            centralized_sum > 0.0 && cl_sum > 0.0,
            "centralized sum={centralized_sum}, cl sum={cl_sum}"
        );
        assert!(
            (centralized_mean - cl_mean).abs() < 50.0,
            "centralized mean={centralized_mean}, cl mean={cl_mean}"
        );
    }

    #[test]
    fn fair_share_finalize_adds_queue_average_over_replicas() {
        use super::super::occupancy::OccupancyAccumulator;

        let sim_start = MonotonicTime::EPOCH;
        let sim_end = sim_start + Duration::from_secs(10);

        let mut replica = OccupancyAccumulator::default();
        replica.record(sim_start, 1);
        replica.record(sim_end, 1);
        let replica_avg = replica.finalize(sim_end, sim_start);

        let mut balancer = OccupancyAccumulator::default();
        balancer.record(sim_start, 6);
        balancer.record(sim_end, 6);
        let q_avg = balancer.finalize(sim_end, sim_start);

        let n = 3.0;
        let combined = replica_avg + q_avg / n;
        assert!((replica_avg - 1.0).abs() < 1e-9);
        assert!((q_avg - 6.0).abs() < 1e-9);
        assert!((combined - 3.0).abs() < 1e-9);
    }

    #[test]
    fn prob_latency_gt_slo_computed_from_e2e() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let stats = run(&MsArgs {
            callgraph: root.join("tests/fanin/multi/callgraph.json"),
            load_file: root.join("tests/fanin/multi/load.json"),
            n: 1000,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(1),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats");

        for (api, api_stats) in &stats.by_api {
            let expected = api_stats
                .e2e_ms
                .iter()
                .filter(|&&e2e| e2e > api_stats.slo_latency_ms)
                .count() as f64
                / api_stats.e2e_ms.len() as f64;
            assert!(
                (api_stats.prob_latency_gt_slo - expected).abs() < 1e-12,
                "api {api}"
            );
            assert!((0.0..=1.0).contains(&api_stats.prob_latency_gt_slo));
        }
    }

    #[test]
    fn same_seed_is_reproducible() {
        let args = client_server_args(42);
        let first = run(&args).unwrap().expect("stats");
        let second = run(&args).unwrap().expect("stats");
        assert_eq!(
            first.by_api["handle"].e2e_ms,
            second.by_api["handle"].e2e_ms
        );
    }

    #[test]
    fn load_overrides_apply_to_all_apis() {
        let mut load = LoadSpec::from([
            (
                "a".to_string(),
                ApiLoad {
                    rps: 10.0,
                    slo_ms: 20.0,
                },
            ),
            (
                "b".to_string(),
                ApiLoad {
                    rps: 30.0,
                    slo_ms: 40.0,
                },
            ),
        ]);

        apply_load_overrides(&mut load, Some(1234.0), Some(56.0));

        for spec in load.values() {
            assert_eq!(spec.rps, 1234.0);
            assert_eq!(spec.slo_ms, 56.0);
        }
    }

    #[test]
    fn rps_override_changes_arrival_rate_enough_to_affect_utilization() {
        let mut low = client_server_args(11);
        low.rps = Some(1.0);
        let mut high = client_server_args(11);
        high.rps = Some(100.0);

        let low_stats = run(&low).unwrap().expect("low stats");
        let high_stats = run(&high).unwrap().expect("high stats");

        assert!(
            high_stats.microservice_utilization_pct["server"]
                > low_stats.microservice_utilization_pct["server"],
            "expected higher rps override to increase utilization"
        );
    }

    #[test]
    fn slo_override_changes_reported_slo_and_violation_probability() {
        let mut args = client_server_args(13);
        args.slo_ms = Some(1.0);

        let stats = run(&args).unwrap().expect("stats");
        let api = &stats.by_api["handle"];

        assert_eq!(api.slo_latency_ms, 1.0);
        assert!(api.prob_latency_gt_slo > 0.0);
    }

    fn assert_chain3_visit_metrics(stats: &MsStats, slo_ms: f64) {
        let chain = ["frontend", "backend1", "backend2"];
        assert_eq!(
            stats.microservice_order,
            vec!["frontend", "backend1", "backend2"]
        );
        let mut mean_slack = [0.0; 3];
        let mut mean_cumulative = [0.0; 3];
        for (idx, ms) in chain.iter().enumerate() {
            let ms_stats = &stats.by_microservice[*ms];
            assert_eq!(ms_stats.response_time_ms.len(), 500, "{ms}");
            assert_eq!(ms_stats.inter_arrival_ms.len(), 499, "{ms}");
            assert_eq!(ms_stats.slack_d_ms.len(), 500, "{ms}");
            assert_eq!(ms_stats.cumulative_queueing_delay_ms.len(), 500, "{ms}");
            mean_slack[idx] = ms_stats.slack_d_ms.iter().sum::<f64>() / 500.0;
            mean_cumulative[idx] =
                ms_stats.cumulative_queueing_delay_ms.iter().sum::<f64>() / 500.0;
            assert!(
                (0.0..=1.0).contains(&ms_stats.prob_latency_gt_slo),
                "{ms}: prob_latency_gt_slo out of range"
            );
            let expected = ms_stats
                .response_time_ms
                .iter()
                .zip(ms_stats.slack_d_ms.iter())
                .filter(|(rt, sd)| **rt > **sd)
                .count() as f64
                / 500.0;
            assert!(
                (ms_stats.prob_latency_gt_slo - expected).abs() < 1e-12,
                "{ms}: prob_latency_gt_slo {} vs expected {expected}",
                ms_stats.prob_latency_gt_slo
            );
            for i in 0..500 {
                assert!(
                    ms_stats.queueing_delay_ms[i] + ms_stats.processing_time_ms[i]
                        <= ms_stats.response_time_ms[i] + 1e-6,
                    "{ms} visit {i}"
                );
                assert!(
                    ms_stats.cumulative_queueing_delay_ms[i]
                        >= ms_stats.queueing_delay_ms[i] - 1e-6,
                    "{ms} visit {i}: cumulative should be >= local queueing"
                );
            }
        }
        assert!(
            mean_slack[0] > slo_ms - 1.0 && mean_slack[0] <= slo_ms,
            "frontend mean slack {mean_slack:?} vs slo_ms={slo_ms}"
        );
        assert!(
            mean_slack[0] > mean_slack[1] && mean_slack[1] > mean_slack[2],
            "mean slack should decrease down chain: {mean_slack:?}"
        );
        assert!(
            mean_cumulative[0] < mean_cumulative[1] && mean_cumulative[1] < mean_cumulative[2],
            "mean cumulative queueing should increase down chain: {mean_cumulative:?}"
        );
        assert!(stats.total_processing_p99_ms > 0.0);

        assert_eq!(stats.per_request_cumulative_queueing_ms.len(), 500);
        for (row_idx, row) in stats.per_request_cumulative_queueing_ms.iter().enumerate() {
            assert_eq!(row.len(), 3, "row {row_idx}");
            assert!(
                row[0] <= row[1] + 1e-6 && row[1] <= row[2] + 1e-6,
                "row {row_idx}: cumulative should be non-decreasing: {row:?}"
            );
        }
        for (idx, ms) in chain.iter().enumerate() {
            let ms_stats = &stats.by_microservice[*ms];
            let per_request_mean = stats
                .per_request_cumulative_queueing_ms
                .iter()
                .map(|row| row[idx])
                .sum::<f64>()
                / 500.0;
            let visit_mean = ms_stats.cumulative_queueing_delay_ms.iter().sum::<f64>() / 500.0;
            assert!(
                (per_request_mean - visit_mean).abs() < 1e-6,
                "{ms}: per-request mean {per_request_mean} vs visit mean {visit_mean}"
            );
        }
    }

    fn chain3_visit_stats(scheduling: SchedulingPolicyKind) -> MsStats {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        run(&MsArgs {
            callgraph: root.join("tests/chain/3/callgraph.json"),
            load_file: root.join("tests/chain/3/load.json"),
            n: 500,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(42),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling,
            force_fixed_svc: false,
        })
        .unwrap()
        .expect("stats")
    }

    #[test]
    fn chain3_by_microservice_visit_metrics() {
        assert_chain3_visit_metrics(&chain3_visit_stats(SchedulingPolicyKind::Fifo), 16.0);
        assert_chain3_visit_metrics(&chain3_visit_stats(SchedulingPolicyKind::Edf), 16.0);
    }

    #[test]
    fn force_fixed_svc_uses_constant_processing_times() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let stats = run(&MsArgs {
            callgraph: root.join("tests/chain/3/callgraph.json"),
            load_file: root.join("tests/chain/3/load.json"),
            n: 500,
            lb_policy: LoadBalancePolicyKind::LeastRequest,
            lb_subset_size: 0,
            lb_subset_policy: SubsetPolicyKind::Deterministic,
            seed: Some(42),
            rps: None,
            slo_ms: None,
            format: OutputFormat::Json,
            trace: false,
            trace_limit: 5,
            scale: 0,
            verbose: 0,
            scheduling: SchedulingPolicyKind::Fifo,
            force_fixed_svc: true,
        })
        .unwrap()
        .expect("stats");

        let expected_ms = [("frontend", 1.0), ("backend1", 1.0), ("backend2", 1.0)];
        for (ms, expected) in expected_ms {
            let samples = &stats.by_microservice[ms].processing_time_ms;
            assert!(!samples.is_empty(), "{ms}");
            for &sample in samples {
                assert!(
                    (sample - expected).abs() < 1e-9,
                    "{ms}: expected constant {expected} ms, got {sample}"
                );
            }
        }
    }

    #[test]
    fn non_positive_overrides_are_rejected_by_load_validation() {
        let mut bad_rps = client_server_args(17);
        bad_rps.rps = Some(0.0);
        assert!(run(&bad_rps).is_err());

        let mut bad_slo = client_server_args(17);
        bad_slo.slo_ms = Some(0.0);
        assert!(run(&bad_slo).is_err());
    }
}
