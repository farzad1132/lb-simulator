use clap::ValueEnum;
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use rand::seq::SliceRandom;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::policy::LoadBalancePolicyKind;
use super::balancer::Balancer;
use super::callgraph::{CallGraph, LoadSpec, load_spec_from_file};
use super::hop::{
    CompletedRequest, Hop, HopDispatcher, HopDispatcherMsg, HopForward, sample_exp,
};
use super::replica::Replica;

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
    pub format: OutputFormat,
}

#[derive(Serialize)]
pub struct ApiStats {
    pub e2e_ms: Vec<f64>,
    pub processing_time_ms: Vec<f64>,
    pub unloaded_latency_p99_ms: f64,
    pub slo_latency_ms: f64,
}

#[derive(Serialize)]
pub struct MsStats {
    pub utilization_pct: HashMap<String, f64>,
    pub by_api: HashMap<String, ApiStats>,
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
    let apis: Vec<_> = load.keys().cloned().collect();
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

fn random_replica_subset(n_replicas: usize, subset_size: u32) -> Vec<usize> {
    let k = if subset_size == 0 {
        n_replicas
    } else {
        (subset_size as usize).min(n_replicas).max(1)
    };
    let mut indices: Vec<usize> = (0..n_replicas).collect();
    indices.shuffle(&mut rand::rng());
    indices.truncate(k);
    indices
}

fn poisson_arrivals(
    sim: &Simulation,
    input: &EventId<HopDispatcherMsg>,
    api: String,
    rps: f64,
    n: u32,
) -> Result<(), SchedulingError> {
    if n == 0 || rps <= 0.0 {
        return Ok(());
    }
    let arrival_mean = 1.0 / rps as f32;
    let scheduler = sim.scheduler();
    let mut rng = rand::rng();
    let mut offset = Duration::ZERO;

    for _ in 0..n {
        offset += Duration::from_secs_f32(sample_exp(&mut rng, arrival_mean));
        scheduler.schedule_event(
            offset,
            input,
            HopDispatcherMsg::Arrival(api.clone()),
        )?;
    }
    Ok(())
}

fn new_api_stats() -> ApiStats {
    ApiStats {
        e2e_ms: Vec::new(),
        processing_time_ms: Vec::new(),
        unloaded_latency_p99_ms: 0.0,
        slo_latency_ms: 0.0,
    }
}

fn finalize_api_stats(stats: &mut ApiStats) {
    let mut processing = stats.processing_time_ms.clone();
    processing.sort_by(f64::total_cmp);
    stats.unloaded_latency_p99_ms = percentile(&processing, 99.0);
}

fn calculate_stats(
    completed: &mut EventQueueReader<CompletedRequest>,
    busy_time: &HashMap<String, Duration>,
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
        finalize_api_stats(stats);
        if let Some(spec) = load.get(api) {
            stats.slo_latency_ms = spec.slo_ms;
        }
    }

    let mut utilization_pct = HashMap::new();
    let obs_secs = observation.as_secs_f64();
    if obs_secs > 0.0 {
        for service_id in &graph.service_order {
            if let Some(spec) = graph.services.get(service_id) {
                let busy = busy_time.get(service_id).copied().unwrap_or(Duration::ZERO);
                let pct = busy.as_secs_f64() / (obs_secs * f64::from(spec.cpu)) * 100.0;
                utilization_pct.insert(service_id.clone(), pct);
            }
        }
    }

    Some(MsStats {
        utilization_pct,
        by_api,
    })
}

pub fn run(args: &MsArgs) -> Result<Option<MsStats>, Box<dyn std::error::Error>> {
    let graph = Arc::new(CallGraph::from_file(&args.callgraph)?);
    let load = load_spec_from_file(&args.load_file)?;
    graph.validate_load(&load)?;

    let busy_time: Arc<Mutex<HashMap<String, Duration>>> = Arc::new(Mutex::new(HashMap::new()));
    for service_id in &graph.service_order {
        busy_time
            .lock()
            .unwrap()
            .insert(service_id.clone(), Duration::ZERO);
    }

    let mut bench = SimInit::new();
    let (sink, mut completed) = event_queue(SinkState::Enabled);

    let mut service_outputs: HashMap<String, Output<Hop>> = HashMap::new();
    let dispatcher_mb = Mailbox::<HopDispatcher>::new();
    let forward_mb = Mailbox::<HopForward>::new();

    for service_id in &graph.service_order {
        let spec = &graph.services[service_id];
        let n_replicas = spec.replicas as usize;
        let concurrency = (spec.cpu / spec.replicas).max(1);

        let replica_mailboxes: Vec<_> = (0..n_replicas).map(|_| Mailbox::new()).collect();

        let replica_indices = random_replica_subset(n_replicas, args.lb_subset_size);
        let mut balancer = Balancer::new(
            args.lb_policy.build(),
            n_replicas,
            replica_indices,
        );

        let balancer_mb = Mailbox::new();
        let balancer_address = balancer_mb.address();
        let mut to_balancer = Output::default();
        to_balancer.connect(Balancer::input, &balancer_mb);
        service_outputs.insert(service_id.clone(), to_balancer);

        for (i, mb) in replica_mailboxes.iter().enumerate() {
            balancer.outputs[i].connect(Replica::input, mb);
        }
        bench = bench.add_model(balancer, balancer_mb, service_id);

        for (i, mb) in replica_mailboxes.into_iter().enumerate() {
            let mut replica = Replica::new(concurrency, i);
            replica.output.connect(HopForward::input, &forward_mb);
            replica.release.connect(Balancer::release, &balancer_address);
            bench = bench.add_model(replica, mb, &format!("{service_id}-replica-{i}"));
        }
    }

    let mut forwarder = HopForward {
        output: Output::default(),
    };
    forwarder
        .output
        .connect(HopDispatcher::input, &dispatcher_mb);
    bench = bench.add_model(forwarder, forward_mb, "hop-forward");

    let arrival_input = EventSource::new()
        .connect(HopDispatcher::input, &dispatcher_mb)
        .register(&mut bench);

    let mut dispatcher = HopDispatcher::new(graph.clone(), service_outputs, busy_time.clone());
    dispatcher.completed.connect_sink(sink);
    bench = bench.add_model(dispatcher, dispatcher_mb, "hop-dispatcher");

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    let counts = split_by_rps(args.n, &load);
    for (api, count) in &counts {
        let rps = load[api].rps;
        poisson_arrivals(&simu, &arrival_input, api.clone(), rps, *count)?;
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
    for service_id in stats.utilization_pct.keys() {
        println!(
            "  {}: {:.2}",
            service_id,
            stats.utilization_pct[service_id]
        );
    }
    for (api, api_stats) in &stats.by_api {
        println!("API {api}:");
        println!(
            "  unloaded latency (p99): {:.4} ms",
            api_stats.unloaded_latency_p99_ms
        );
        println!("  SLO latency: {:.4} ms", api_stats.slo_latency_ms);
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
