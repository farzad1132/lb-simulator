use clap::ValueEnum;
use nexosim::model::{Context, Model};
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::load_registry::LoadRegistry;
use crate::policy::LoadBalancePolicyKind;
use crate::rng;
use crate::subset::{self, SubsetPolicyKind};
use super::balancer::{EdgeBalancer, ReplicaBalancer};
use super::callgraph::{CallGraph, LoadSpec, load_spec_from_file};
#[cfg(test)]
use super::callgraph::ApiLoad;
use super::hop::{
    CompletedRequest, Hop, OutboundCall, ReplicaInput, sample_exp,
    service_for_endpoint,
};
use super::replica::{Replica, ReplicaConfig};
use super::trace::MsTracer;

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
    pub utilization_pct: HashMap<String, f64>,
    pub replica_utilization_pct: HashMap<String, HashMap<usize, f64>>,
    pub by_api: HashMap<String, ApiStats>,
}

#[derive(Deserialize, Serialize)]
struct UserArrival {
    #[serde(skip)]
    graph: Arc<CallGraph>,
    #[serde(skip)]
    edge_balancers: HashMap<String, Output<Hop>>,
    #[serde(skip)]
    tracer: Option<Arc<MsTracer>>,
}

impl UserArrival {
    fn new(
        graph: Arc<CallGraph>,
        edge_balancers: HashMap<String, Output<Hop>>,
        tracer: Option<Arc<MsTracer>>,
    ) -> Self {
        Self {
            graph,
            edge_balancers,
            tracer,
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
        let (request_id, trace) = match &self.tracer {
            Some(tracer) => tracer.next_request_id(),
            None => (0, false),
        };
        let hop = Hop {
            request_id,
            trace,
            api,
            endpoint: endpoint.clone(),
            sibling_index: 0,
            start: cx.time(),
            duration: Duration::ZERO,
            processing_time: Duration::ZERO,
            caller: None,
            outbound_release: None,
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

fn new_bench(seed: Option<u64>) -> SimInit {
    if seed.is_some() {
        SimInit::with_num_threads(1)
    } else {
        SimInit::new()
    }
}

fn downstream_targets(graph: &CallGraph, service_id: &str) -> HashSet<String> {
    let mut targets = HashSet::new();
    for (endpoint, owner) in &graph.endpoint_service {
        if owner != service_id {
            continue;
        }
        if let Some(edges) = graph.children.get(endpoint) {
            for (target, _) in edges {
                if let Some(downstream) = graph.endpoint_service.get(target) {
                    targets.insert(downstream.clone());
                }
            }
        }
    }
    targets
}

fn entry_services_by_api(graph: &CallGraph) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (api, endpoint) in &graph.entrypoints {
        if let Ok(service) = service_for_endpoint(graph, endpoint) {
            map.insert(api.clone(), service);
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
    graph: &CallGraph,
    load: &LoadSpec,
    observation: Duration,
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

    let mut utilization_pct = HashMap::new();
    let mut replica_utilization_pct = HashMap::new();
    let obs_secs = observation.as_secs_f64();
    if obs_secs > 0.0 {
        for service_id in &graph.service_order {
            if let Some(spec) = graph.services.get(service_id) {
                let replica_busy = busy_time.get(service_id);
                let total_busy: Duration = replica_busy
                    .map(|m| m.values().copied().sum())
                    .unwrap_or(Duration::ZERO);
                let pct =
                    total_busy.as_secs_f64() / (obs_secs * f64::from(spec.cpu)) * 100.0;
                utilization_pct.insert(service_id.clone(), pct);

                let concurrency = (spec.cpu / spec.replicas).max(1);
                let mut by_replica = HashMap::new();
                let n_replicas = spec.replicas as usize;
                for i in 0..n_replicas {
                    let busy = replica_busy
                        .and_then(|m| m.get(&i).copied())
                        .unwrap_or(Duration::ZERO);
                    let replica_pct =
                        busy.as_secs_f64() / (obs_secs * f64::from(concurrency)) * 100.0;
                    by_replica.insert(i, replica_pct);
                }
                replica_utilization_pct.insert(service_id.clone(), by_replica);
            }
        }
    }

    Some(MsStats {
        utilization_pct,
        replica_utilization_pct,
        by_api,
    })
}

pub fn run(args: &MsArgs) -> Result<Option<MsStats>, Box<dyn std::error::Error>> {
    rng::enter_run(args.seed);
    let result = run_inner(args);
    rng::exit_run();
    result
}

fn run_inner(args: &MsArgs) -> Result<Option<MsStats>, Box<dyn std::error::Error>> {
    let mut graph = CallGraph::from_file(&args.callgraph)?;
    graph.apply_scale(args.scale)?;
    let graph = Arc::new(graph);
    let mut load = load_spec_from_file(&args.load_file)?;
    apply_load_overrides(&mut load, args.rps, args.slo_ms);
    graph.validate_load(&load)?;

    let busy_time: Arc<Mutex<HashMap<String, HashMap<usize, Duration>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    for service_id in &graph.service_order {
        let n_replicas = graph.services[service_id].replicas as usize;
        let mut by_replica = HashMap::new();
        for i in 0..n_replicas {
            by_replica.insert(i, Duration::ZERO);
        }
        busy_time
            .lock()
            .unwrap()
            .insert(service_id.clone(), by_replica);
    }

    let tracer = if args.trace {
        Some(MsTracer::new(args.trace_limit))
    } else {
        None
    };

    let entry_services = entry_services_by_api(graph.as_ref());
    let service_replica_counts: HashMap<String, u32> = graph
        .services
        .iter()
        .map(|(id, spec)| (id.clone(), spec.replicas))
        .collect();

    let replica_loads: HashMap<String, LoadRegistry> = graph
        .service_order
        .iter()
        .map(|service_id| {
            let n = graph.services[service_id].replicas as usize;
            (service_id.clone(), LoadRegistry::new(n))
        })
        .collect();

    let mut bench = new_bench(args.seed);
    let (sink, mut completed) = event_queue(SinkState::Enabled);

    let mut replica_mailboxes: HashMap<(String, usize), Mailbox<Replica>> = HashMap::new();
    for service_id in &graph.service_order {
        let n_replicas = graph.services[service_id].replicas as usize;
        for i in 0..n_replicas {
            replica_mailboxes.insert((service_id.clone(), i), Mailbox::new());
        }
    }

    struct PendingEdgeBalancer {
        balancer: EdgeBalancer,
        mailbox: Mailbox<EdgeBalancer>,
        api: String,
        entry_service: String,
        replica_indices: Vec<usize>,
    }

    struct PendingReplicaBalancer {
        balancer: ReplicaBalancer,
        mailbox: Mailbox<ReplicaBalancer>,
        service_id: String,
        replica_idx: usize,
        downstream_indices: HashMap<String, Vec<usize>>,
    }

    let mut pending_edge_balancers: Vec<PendingEdgeBalancer> = Vec::new();
    let mut edge_balancer_inputs: HashMap<String, Output<Hop>> = HashMap::new();
    let mut edge_balancer_addresses: HashMap<String, nexosim::simulation::Address<EdgeBalancer>> =
        HashMap::new();

    let mut apis: Vec<_> = graph.entrypoints.keys().cloned().collect();
    apis.sort();
    for (api_index, api) in apis.iter().enumerate() {
        let entry_endpoint = &graph.entrypoints[api];
        let entry_service = service_for_endpoint(graph.as_ref(), entry_endpoint)?;
        let n_replicas = graph.services[&entry_service].replicas as usize;
        let replica_indices = subset::assign_subset(
            args.lb_subset_policy,
            n_replicas,
            api_index,
            args.lb_subset_size,
        );

        let balancer = EdgeBalancer::new(
            args.lb_policy.build(),
            args.lb_policy,
            api.clone(),
            n_replicas,
            replica_indices.clone(),
            replica_loads
                .get(&entry_service)
                .expect("missing load registry for entry service")
                .clone(),
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
            entry_service,
            replica_indices,
        });
    }

    for pending in &mut pending_edge_balancers {
        for &replica_idx in &pending.replica_indices {
            if let Some(mb) = replica_mailboxes.get(&(pending.entry_service.clone(), replica_idx)) {
                pending.balancer.outputs[replica_idx]
                    .connect(Replica::input, mb);
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

    for service_id in &graph.service_order {
        let n_replicas = graph.services[service_id].replicas as usize;

        for replica_idx in 0..n_replicas {
            let mut downstream_indices = HashMap::new();
            for target in downstream_targets(&graph, service_id) {
                let target_replicas = graph.services[&target].replicas as usize;
                downstream_indices.insert(
                    target.clone(),
                    subset::assign_subset(
                        args.lb_subset_policy,
                        target_replicas,
                        replica_idx,
                        args.lb_subset_size,
                    ),
                );
            }

            let downstream_loads: HashMap<String, LoadRegistry> = downstream_indices
                .keys()
                .map(|target| {
                    (
                        target.clone(),
                        replica_loads
                            .get(target)
                            .expect("missing load registry for downstream service")
                            .clone(),
                    )
                })
                .collect();

            let balancer = ReplicaBalancer::new(
                args.lb_policy.build(),
                args.lb_policy,
                service_id.clone(),
                replica_idx,
                downstream_indices.clone(),
                downstream_loads,
                &service_replica_counts,
                tracer.clone(),
            );
            let mailbox = Mailbox::new();
            let address = mailbox.address();

            let mut outbound = Output::default();
            outbound.connect(ReplicaBalancer::outbound, &mailbox);
            replica_balancer_outbound.insert((service_id.clone(), replica_idx), outbound);
            replica_balancer_addresses.insert((service_id.clone(), replica_idx), address.clone());

            pending_replica_balancers.push(PendingReplicaBalancer {
                balancer,
                mailbox,
                service_id: service_id.clone(),
                replica_idx,
                downstream_indices: downstream_indices.clone(),
            });
        }
    }

    for pending in &mut pending_replica_balancers {
        for (target_service, indices) in &pending.downstream_indices {
            let n_target = graph.services[target_service].replicas as usize;
            let outputs = pending
                .balancer
                .downstream_outputs
                .get_mut(target_service)
                .unwrap_or_else(|| panic!("missing downstream outputs for {target_service}"));
            if outputs.len() != n_target {
                *outputs = (0..n_target).map(|_| Output::default()).collect();
            }
            for &replica_idx in indices {
                if let Some(mb) = replica_mailboxes.get(&(target_service.clone(), replica_idx)) {
                    outputs[replica_idx].connect(Replica::input, mb);
                }
            }
        }
    }

    let mut return_outputs: HashMap<(String, usize), Output<ReplicaInput>> = HashMap::new();
    for (key, mb) in &replica_mailboxes {
        let mut output = Output::default();
        output.connect(Replica::input, mb);
        return_outputs.insert(key.clone(), output);
    }

    let mut completed_output = Output::default();
    completed_output.connect_sink(sink);

    let apis_by_entry_service: HashMap<String, Vec<String>> = {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        for (api, service) in &entry_services {
            map.entry(service.clone()).or_default().push(api.clone());
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
                pending.service_id, pending.replica_idx
            ),
        );
    }

    for service_id in &graph.service_order {
        let spec = &graph.services[service_id];
        let n_replicas = spec.replicas as usize;
        let concurrency = (spec.cpu / spec.replicas).max(1);

        for i in 0..n_replicas {
            let mb = replica_mailboxes
                .remove(&(service_id.clone(), i))
                .expect("replica mailbox");
            let outbound = replica_balancer_outbound
                .get(&(service_id.clone(), i))
                .cloned()
                .expect("replica balancer outbound");
            let rb_address = replica_balancer_addresses
                .get(&(service_id.clone(), i))
                .expect("replica balancer address");

            let mut edge_releases = HashMap::new();
            if let Some(apis) = apis_by_entry_service.get(service_id) {
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
            outbound_release.connect(ReplicaBalancer::release_outbound, rb_address);

            let replica = Replica::new(ReplicaConfig {
                graph: graph.clone(),
                service_id: service_id.clone(),
                replica_idx: i,
                max_concurrency: concurrency,
                busy_time: busy_time.clone(),
                balancer_outbound: outbound,
                outbound_release,
                edge_releases,
                return_outputs: return_outputs.clone(),
                completed: completed_output.clone(),
                tracer: tracer.clone(),
                load_registry: replica_loads
                    .get(service_id)
                    .expect("missing load registry for service")
                    .clone(),
            });
            bench = bench.add_model(replica, mb, &format!("{service_id}-replica-{i}"));
        }
    }

    let arrival = UserArrival::new(graph.clone(), edge_balancer_inputs, tracer);
    let arrival_mb = Mailbox::new();
    let arrival_input = EventSource::new()
        .connect(UserArrival::inject, &arrival_mb)
        .register(&mut bench);
    bench = bench.add_model(arrival, arrival_mb, "user-arrival");

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    let counts = split_by_rps(args.n, &load);
    let mut apis: Vec<_> = counts.keys().cloned().collect();
    apis.sort();
    for api in apis {
        let count = counts[&api];
        let rps = load[&api].rps;
        poisson_arrivals(&simu, &arrival_input, api, rps, count)?;
    }

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    let busy = busy_time.lock().unwrap().clone();
    Ok(calculate_stats(
        &mut completed,
        &busy,
        graph.as_ref(),
        &load,
        observation,
    ))
}

pub fn print_human_stats(stats: &MsStats) {
    println!("utilization (%):");
    let mut service_ids: Vec<_> = stats.utilization_pct.keys().cloned().collect();
    service_ids.sort();
    for service_id in service_ids {
        println!(
            "  {}: {:.2}",
            service_id,
            stats.utilization_pct[&service_id]
        );
        if let Some(replicas) = stats.replica_utilization_pct.get(&service_id) {
            let mut indices: Vec<_> = replicas.keys().copied().collect();
            indices.sort_unstable();
            for idx in indices {
                println!(
                    "    replica {}: {:.2}",
                    idx,
                    replicas[&idx]
                );
            }
        }
    }
    for (api, api_stats) in &stats.by_api {
        println!("API {api}:");
        println!(
            "  unloaded latency (p99): {:.4} ms",
            api_stats.unloaded_latency_p99_ms
        );
        println!("  SLO latency: {:.4} ms", api_stats.slo_latency_ms);
        println!(
            "  P(latency > SLO): {:.6}",
            api_stats.prob_latency_gt_slo
        );
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
        })
        .unwrap()
        .expect("stats");
        assert!(!stats.by_api.is_empty());

        let graph = CallGraph::from_file(&root.join("tests/fanin/multi/callgraph.json")).unwrap();
        for service_id in &graph.service_order {
            let spec = &graph.services[service_id];
            let replicas = stats
                .replica_utilization_pct
                .get(service_id)
                .expect("replica utilization for service");
            assert_eq!(replicas.len(), spec.replicas as usize);
            for i in 0..spec.replicas as usize {
                let pct = replicas[&i];
                assert!(
                    (0.0..=100.0).contains(&pct),
                    "replica {i} of {service_id}: {pct}% out of range"
                );
            }
        }
    }

    #[test]
    fn single_replica_utilization_matches_overall() {
        let args = client_server_args(7);
        let stats = run(&args).unwrap().expect("stats");
        let overall = stats.utilization_pct["server"];
        let replica = stats.replica_utilization_pct["server"][&0];
        assert!(
            (overall - replica).abs() < 1e-9,
            "single replica: overall={overall}, replica={replica}"
        );
    }

    #[test]
    fn replica_utilization_is_reproducible() {
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
        };
        let first = run(&args).unwrap().expect("stats");
        let second = run(&args).unwrap().expect("stats");
        assert_eq!(first.utilization_pct, second.utilization_pct);
        for service_id in first.replica_utilization_pct.keys() {
            let mut first_vals: Vec<_> = first.replica_utilization_pct[service_id]
                .values()
                .copied()
                .collect();
            let mut second_vals: Vec<_> = second.replica_utilization_pct[service_id]
                .values()
                .copied()
                .collect();
            first_vals.sort_by(f64::total_cmp);
            second_vals.sort_by(f64::total_cmp);
            assert_eq!(first_vals, second_vals, "service {service_id}");
        }
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
            high_stats.utilization_pct["server"] > low_stats.utilization_pct["server"],
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
