mod load_balancer;
mod policy;
mod rng {
    pub use lb::rng::*;
}
mod server;

use clap::{Parser, ValueEnum};
use lb::load_registry::LoadRegistry;
use load_balancer::LoadBalancer;
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use policy::LoadBalancePolicyKind;
use lb::subset::{self, SubsetPolicyKind};
use rand::Rng;
use serde::Serialize;
use server::{ExpressEvictionPolicy, QueueDelayEvictionMode, Server, Task};
use std::io::{self, Write};
use std::time::Duration;

const MIN_DURATION_SECS: f32 = 1e-9;
const SERVICE_MEAN: f32 = 1.0;

fn sample_exp(rng: &mut impl Rng, mean: f32) -> f32 {
    // u in (0, 1]; avoid ln(0) when the uniform draw is exactly 0.
    let u = loop {
        let u = 1.0 - rng.random::<f32>();
        if u > 0.0 && u.is_finite() {
            break u;
        }
    };
    (-mean * u.ln()).max(MIN_DURATION_SECS)
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ServiceDistribution {
    Exponential,
    Constant,
    Bimodal,
}

struct BimodalConfig {
    modes: [f32; 2],
    probs: [f32; 2],
}

struct ServiceTimeConfig {
    mean: f32,
    dist: ServiceDistribution,
    bimodal: Option<BimodalConfig>,
}

const PROB_SUM_TOLERANCE: f32 = 1e-6;

fn mixture_mean(modes: &[f32; 2], probs: &[f32; 2]) -> f32 {
    modes[0] * probs[0] + modes[1] * probs[1]
}

fn select_bimodal_mode(rng: &mut impl Rng, config: &BimodalConfig) -> f32 {
    if rng.random::<f32>() < config.probs[0] {
        config.modes[0]
    } else {
        config.modes[1]
    }
}

fn sample_bimodal(rng: &mut impl Rng, config: &BimodalConfig) -> f32 {
    let mode_mean = select_bimodal_mode(rng, config);
    sample_exp(rng, mode_mean)
}

fn sample_service(rng: &mut impl Rng, service_time: &ServiceTimeConfig) -> f32 {
    match service_time.dist {
        ServiceDistribution::Exponential => sample_exp(rng, service_time.mean),
        ServiceDistribution::Constant => service_time.mean.max(MIN_DURATION_SECS),
        ServiceDistribution::Bimodal => {
            sample_bimodal(rng, service_time.bimodal.as_ref().expect("bimodal config"))
        }
    }
}

fn resolve_service_time(args: &Args) -> Result<ServiceTimeConfig, String> {
    match args.service_dist {
        ServiceDistribution::Bimodal => {
            let modes = args
                .service_modes
                .as_ref()
                .ok_or("--service-modes is required with --service-dist bimodal")?;
            let probs = args
                .service_mode_probs
                .as_ref()
                .ok_or("--service-mode-probs is required with --service-dist bimodal")?;
            if modes.len() != 2 {
                return Err(format!(
                    "--service-modes requires exactly 2 values, got {}",
                    modes.len()
                ));
            }
            if probs.len() != 2 {
                return Err(format!(
                    "--service-mode-probs requires exactly 2 values, got {}",
                    probs.len()
                ));
            }
            if modes.iter().any(|m| *m <= 0.0 || !m.is_finite()) {
                return Err("--service-modes values must be positive and finite".into());
            }
            if probs.iter().any(|p| *p <= 0.0 || !p.is_finite()) {
                return Err("--service-mode-probs values must be positive and finite".into());
            }
            let prob_sum: f32 = probs.iter().sum();
            if (prob_sum - 1.0).abs() > PROB_SUM_TOLERANCE {
                return Err(format!(
                    "--service-mode-probs must sum to 1, got {prob_sum}"
                ));
            }
            let modes_arr = [modes[0], modes[1]];
            let probs_arr = [probs[0], probs[1]];
            let mean = mixture_mean(&modes_arr, &probs_arr);
            Ok(ServiceTimeConfig {
                mean,
                dist: args.service_dist,
                bimodal: Some(BimodalConfig {
                    modes: modes_arr,
                    probs: probs_arr,
                }),
            })
        }
        _ => {
            if args.service_modes.is_some() || args.service_mode_probs.is_some() {
                return Err(
                    "--service-modes and --service-mode-probs are only valid with --service-dist bimodal"
                        .into(),
                );
            }
            Ok(ServiceTimeConfig {
                mean: SERVICE_MEAN,
                dist: args.service_dist,
                bimodal: None,
            })
        }
    }
}

fn exp_source(
    sim: &Simulation,
    input: &EventId<Task>,
    arrival_mean: f32,
    service_time: &ServiceTimeConfig,
    n: u32,
) -> Result<(), SchedulingError> {
    let scheduler = sim.scheduler();
    let t0 = sim.time();
    let mut offset = Duration::ZERO;

    rng::with_rng(|rng| {
        for _ in 0..n {
            offset += Duration::from_secs_f32(sample_exp(rng, arrival_mean));
            let duration = Duration::from_secs_f32(sample_service(rng, service_time));
            let task = Task::new(t0 + offset, duration);
            scheduler.schedule_event(offset, input, task)?;
        }
        Ok::<(), SchedulingError>(())
    })?;
    Ok(())
}

struct Rates {
    total_service_rate: f64,
    per_server_service_rate: f64,
    total_arrival_rate: f64,
    per_client_arrival_rate: f64,
}

fn compute_rates(args: &Args, service_mean: f32) -> Rates {
    let total_capacity = args.servers.max(1) * args.concurrency.max(1);
    let n_clients = args.clients.max(1);
    let capacity = total_capacity as f32;
    let arrival_mean = service_mean / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;
    let service_mean = f64::from(service_mean);
    Rates {
        total_service_rate: f64::from(total_capacity) / service_mean,
        per_server_service_rate: f64::from(args.concurrency.max(1)) / service_mean,
        total_arrival_rate: 1.0 / f64::from(arrival_mean),
        per_client_arrival_rate: 1.0 / f64::from(per_client_arrival_mean),
    }
}

struct ServiceStats {
    utilization_pct: f64,
    regular_utilization_pct: Option<f64>,
    express_utilization_pct: Option<f64>,
    unloaded_latency_p99: f64,
    e2e: Vec<f64>,
    processing_times: Vec<f64>,
    queueing_delays: Vec<f64>,
    regular_e2e: Option<Vec<f64>>,
    express_e2e: Option<Vec<f64>>,
    regular_queueing_delays: Option<Vec<f64>>,
    express_queueing_delays: Option<Vec<f64>>,
    pre_eviction_queueing_delays: Option<Vec<f64>>,
    post_eviction_queueing_delays: Option<Vec<f64>>,
}

struct ExpressLaneStatsConfig {
    n_regular: u32,
    express_size: u32,
    concurrency: u32,
}

#[derive(Serialize)]
struct RunOutput {
    total_service_rate: f64,
    per_server_service_rate: f64,
    total_arrival_rate: f64,
    per_client_arrival_rate: f64,
    utilization_pct: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    regular_utilization_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    express_utilization_pct: Option<f64>,
    unloaded_latency_p99: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    slo_latency: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prob_latency_gt_slo: Option<f64>,
    e2e: Vec<f64>,
    processing_times: Vec<f64>,
    queueing_delays: Vec<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    regular_e2e: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    express_e2e: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    regular_queueing_delays: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    express_queueing_delays: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pre_eviction_queueing_delays: Option<Vec<f64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_eviction_queueing_delays: Option<Vec<f64>>,
}

fn validate_slo(slo: Option<f64>) -> Result<Option<f64>, String> {
    match slo {
        None => Ok(None),
        Some(s) if s <= 0.0 || !s.is_finite() => {
            Err("--slo must be positive and finite".into())
        }
        Some(s) => Ok(Some(s)),
    }
}

fn prob_latency_gt_slo(e2e: &[f64], slo: f64) -> f64 {
    if e2e.is_empty() || slo <= 0.0 {
        return 0.0;
    }
    e2e.iter().filter(|&&latency| latency > slo).count() as f64 / e2e.len() as f64
}

fn pool_utilization_pct(busy: Duration, observation: Duration, pool_capacity: u32) -> f64 {
    if observation.is_zero() || pool_capacity == 0 {
        0.0
    } else {
        busy.as_secs_f64() / (observation.as_secs_f64() * f64::from(pool_capacity)) * 100.0
    }
}

fn duration_secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn calculate_stats(
    output: &mut EventQueueReader<Task>,
    observation: Duration,
    total_capacity: u32,
    express_lane: Option<&ExpressLaneStatsConfig>,
) -> Option<ServiceStats> {
    let mut task_samples: Vec<(f64, f64, bool, Option<f64>, Option<f64>)> = Vec::new();
    let mut busy = Duration::ZERO;
    let mut regular_busy = Duration::ZERO;
    let mut express_busy = Duration::ZERO;

    while let Some(task) = output.try_read() {
        busy += task.duration;
        if task.served_by_express {
            express_busy += task.duration;
        } else {
            regular_busy += task.duration;
        }
        let unloaded_ns = task.duration.as_nanos();
        if unloaded_ns == 0 {
            continue;
        }
        let e2e_ns = task.finish.duration_since(task.start).as_nanos();
        let (pre_eviction, post_eviction) = if task.served_by_express {
            let evicted_at = task.evicted_at.expect("express task must have evicted_at");
            let service_started_at = task
                .service_started_at
                .expect("express task must have service_started_at");
            (
                Some(duration_secs(evicted_at.duration_since(task.start))),
                Some(duration_secs(
                    service_started_at.duration_since(evicted_at),
                )),
            )
        } else {
            (None, None)
        };
        task_samples.push((
            e2e_ns as f64 / 1e9,
            unloaded_ns as f64 / 1e9,
            task.served_by_express,
            pre_eviction,
            post_eviction,
        ));
    }

    if task_samples.is_empty() {
        return None;
    }

    let mut unloaded_samples: Vec<f64> = task_samples
        .iter()
        .map(|(_, duration, _, _, _)| *duration)
        .collect();
    unloaded_samples.sort_by(f64::total_cmp);
    let unloaded_latency_p99 = percentile(&unloaded_samples, 99.0);
    if unloaded_latency_p99 == 0.0 {
        return None;
    }

    let e2e: Vec<f64> = task_samples.iter().map(|(e2e, _, _, _, _)| *e2e).collect();
    let processing_times: Vec<f64> = task_samples
        .iter()
        .map(|(_, duration, _, _, _)| *duration)
        .collect();
    let queueing_delays: Vec<f64> = task_samples
        .iter()
        .map(|(e2e, duration, _, _, _)| e2e - duration)
        .collect();

    let utilization_pct = pool_utilization_pct(busy, observation, total_capacity);

    let (
        regular_utilization_pct,
        express_utilization_pct,
        regular_e2e,
        express_e2e,
        regular_queueing_delays,
        express_queueing_delays,
        pre_eviction_queueing_delays,
        post_eviction_queueing_delays,
    ) = match express_lane {
        Some(cfg) => {
            let regular_capacity = cfg.n_regular * cfg.concurrency;
            let express_capacity = cfg.express_size * cfg.concurrency;
            let regular_e2e: Vec<f64> = task_samples
                .iter()
                .filter(|(_, _, express, _, _)| !express)
                .map(|(e2e, _, _, _, _)| *e2e)
                .collect();
            let express_e2e: Vec<f64> = task_samples
                .iter()
                .filter(|(_, _, express, _, _)| *express)
                .map(|(e2e, _, _, _, _)| *e2e)
                .collect();
            let regular_q: Vec<f64> = task_samples
                .iter()
                .filter(|(_, _, express, _, _)| !express)
                .map(|(e2e, duration, _, _, _)| e2e - duration)
                .collect();
            let express_q: Vec<f64> = task_samples
                .iter()
                .filter(|(_, _, express, _, _)| *express)
                .map(|(e2e, duration, _, _, _)| e2e - duration)
                .collect();
            let pre_q: Vec<f64> = task_samples
                .iter()
                .filter_map(|(_, _, _, pre, _)| *pre)
                .collect();
            let post_q: Vec<f64> = task_samples
                .iter()
                .filter_map(|(_, _, _, _, post)| *post)
                .collect();
            (
                Some(pool_utilization_pct(
                    regular_busy,
                    observation,
                    regular_capacity,
                )),
                Some(pool_utilization_pct(
                    express_busy,
                    observation,
                    express_capacity,
                )),
                Some(regular_e2e),
                Some(express_e2e),
                Some(regular_q),
                Some(express_q),
                Some(pre_q),
                Some(post_q),
            )
        }
        None => (None, None, None, None, None, None, None, None),
    };

    Some(ServiceStats {
        utilization_pct,
        regular_utilization_pct,
        express_utilization_pct,
        unloaded_latency_p99,
        e2e,
        processing_times,
        queueing_delays,
        regular_e2e,
        express_e2e,
        regular_queueing_delays,
        express_queueing_delays,
        pre_eviction_queueing_delays,
        post_eviction_queueing_delays,
    })
}

const HUMAN_PERCENTILES: [f64; 12] = [
    1.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 99.0, 100.0,
];

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let idx = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
    sorted[idx]
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

fn print_section_header(label: &str, count: usize) {
    println!("\n--- {label} (N={count}) ---");
}

fn print_human_stats(stats: &ServiceStats, rates: &Rates, slo: Option<f64>) {
    println!("total service rate: {:.4} tasks/s", rates.total_service_rate);
    println!(
        "per-server service rate: {:.4} tasks/s",
        rates.per_server_service_rate
    );
    println!("total arrival rate: {:.4} tasks/s", rates.total_arrival_rate);
    println!(
        "per-client arrival rate: {:.4} tasks/s",
        rates.per_client_arrival_rate
    );
    println!("utilization: {:.2}%", stats.utilization_pct);
    if let Some(regular) = stats.regular_utilization_pct {
        println!("regular utilization: {:.2}%", regular);
    }
    if let Some(express) = stats.express_utilization_pct {
        println!("express utilization: {:.2}%", express);
    }
    println!("unloaded latency (p99): {:.6}s", stats.unloaded_latency_p99);
    if let Some(slo) = slo {
        println!(
            "P(latency > SLO): {:.6}",
            prob_latency_gt_slo(&stats.e2e, slo)
        );
    }
    print_percentile_table("e2e latency (s):", &mut stats.e2e.clone());
    print_percentile_table("processing time (s):", &mut stats.processing_times.clone());
    print_percentile_table("queueing delay (s):", &mut stats.queueing_delays.clone());

    if let (
        Some(regular_e2e),
        Some(regular_q),
        Some(express_e2e),
        Some(express_q),
    ) = (
        stats.regular_e2e.as_ref(),
        stats.regular_queueing_delays.as_ref(),
        stats.express_e2e.as_ref(),
        stats.express_queueing_delays.as_ref(),
    ) {
        print_section_header("regular tasks", regular_e2e.len());
        print_percentile_table("e2e latency (s):", &mut regular_e2e.clone());
        print_percentile_table("queueing delay (s):", &mut regular_q.clone());

        print_section_header("evicted tasks", express_e2e.len());
        print_percentile_table("e2e latency (s):", &mut express_e2e.clone());
        print_percentile_table("queueing delay (s):", &mut express_q.clone());
        if let Some(pre) = stats.pre_eviction_queueing_delays.as_ref() {
            print_percentile_table("pre-eviction queueing delay (s):", &mut pre.clone());
        }
        if let Some(post) = stats.post_eviction_queueing_delays.as_ref() {
            print_percentile_table("post-eviction queueing delay (s):", &mut post.clone());
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 0.8)]
    load: f32,
    #[arg(long, default_value_t = 1_000_000)]
    n: u32,
    #[arg(long, value_enum, default_value_t = ServiceDistribution::Exponential)]
    service_dist: ServiceDistribution,
    #[arg(long, value_delimiter = ',')]
    service_modes: Option<Vec<f32>>,
    #[arg(long, value_delimiter = ',')]
    service_mode_probs: Option<Vec<f32>>,
    #[arg(long, default_value_t = 1)]
    servers: u32,
    #[arg(long, default_value_t = 1)]
    concurrency: u32,
    #[arg(long, value_enum, default_value_t = LoadBalancePolicyKind::PowerOfTwo)]
    lb_policy: LoadBalancePolicyKind,
    #[arg(long, default_value_t = 0)]
    lb_subset_size: u32,
    #[arg(long, value_enum, default_value_t = SubsetPolicyKind::Deterministic)]
    lb_subset_policy: SubsetPolicyKind,
    #[arg(long, default_value_t = 1)]
    clients: u32,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    slo: Option<f64>,
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 0)]
    verbose: u8,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
    #[arg(long)]
    expresslane: bool,
    #[arg(long)]
    express_size: Option<u32>,
    #[arg(long)]
    express_th: Option<u32>,
    #[arg(long)]
    express_del_th: Option<f64>,
    #[arg(long)]
    ideal: bool,
}

#[derive(Debug)]
enum ExpressEvictionConfig {
    QueueDepth(u32),
    QueueDelay {
        threshold: Duration,
        ideal: bool,
    },
    Combined {
        depth_threshold: u32,
        delay_threshold: Duration,
    },
}

#[derive(Debug)]
struct ExpressLaneConfig {
    express_size: u32,
    eviction: ExpressEvictionConfig,
}

fn validate_express_del_th(value: f64) -> Result<Duration, String> {
    if value > 0.0 && value.is_finite() {
        Ok(Duration::from_secs_f64(value))
    } else {
        Err("--express-del-th must be positive and finite".into())
    }
}

fn validate_expresslane(args: &Args) -> Result<Option<ExpressLaneConfig>, String> {
    let has_express_flags = args.express_size.is_some()
        || args.express_th.is_some()
        || args.express_del_th.is_some();

    if args.lb_policy.is_centralized() {
        if args.expresslane || has_express_flags {
            return Err("--expresslane is not supported with --lb-policy centralized".into());
        }
        return Ok(None);
    }

    if args.ideal {
        if !args.expresslane {
            return Err("--ideal requires --expresslane".into());
        }
        if args.express_th.is_some() {
            return Err("--ideal requires --express-del-th".into());
        }
        if args.express_del_th.is_none() {
            return Err("--ideal requires --express-del-th".into());
        }
    }

    if !args.expresslane {
        if has_express_flags {
            return Err(
                "--express-size, --express-th, and --express-del-th require --expresslane".into(),
            );
        }
        return Ok(None);
    }

    let express_size = args
        .express_size
        .ok_or("--express-size is required with --expresslane")?;

    if express_size == 0 {
        return Err("--express-size must be positive".into());
    }
    if express_size >= args.servers {
        return Err(format!(
            "--express-size ({express_size}) must be less than --servers ({})",
            args.servers
        ));
    }

    let eviction = match (args.express_th, args.express_del_th) {
        (None, None) => {
            return Err(
                "one of --express-th or --express-del-th is required with --expresslane".into(),
            );
        }
        (Some(express_th), None) => ExpressEvictionConfig::QueueDepth(express_th),
        (None, Some(express_del_th)) => ExpressEvictionConfig::QueueDelay {
            threshold: validate_express_del_th(express_del_th)?,
            ideal: args.ideal,
        },
        (Some(express_th), Some(express_del_th)) => ExpressEvictionConfig::Combined {
            depth_threshold: express_th,
            delay_threshold: validate_express_del_th(express_del_th)?,
        },
    };

    Ok(Some(ExpressLaneConfig {
        express_size,
        eviction,
    }))
}

fn split_tasks(n: u32, clients: u32) -> Vec<u32> {
    let clients = clients.max(1);
    let base = n / clients;
    let rem = n % clients;
    (0..clients).map(|i| base + u32::from(i < rem)).collect()
}

fn run_simulation(
    args: &Args,
    service_time: &ServiceTimeConfig,
    express_lane: Option<&ExpressLaneConfig>,
) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    if args.lb_policy.is_centralized() {
        return run_centralized_simulation(args, service_time);
    }
    run_push_simulation(args, service_time, express_lane)
}

fn schedule_initial_pulls(
    sim: &Simulation,
    pull_inputs: &[EventId<()>],
    concurrency: u32,
) -> Result<(), SchedulingError> {
    let scheduler = sim.scheduler();
    let delay = Duration::from_secs_f32(MIN_DURATION_SECS);
    for pull_input in pull_inputs {
        for _ in 0..concurrency {
            scheduler.schedule_event(delay, pull_input, ())?;
        }
    }
    Ok(())
}

fn run_centralized_simulation(
    args: &Args,
    service_time: &ServiceTimeConfig,
) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1) as usize;
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let mut bench = if args.seed.is_some() {
        SimInit::with_num_threads(1)
    } else {
        SimInit::new()
    };
    let (sink, mut output) = event_queue(SinkState::Enabled);

    let load_registry = LoadRegistry::new(n_servers);
    let server_mailboxes: Vec<Mailbox<Server>> = (0..n_servers).map(|_| Mailbox::new()).collect();

    let task_counts = split_tasks(args.n, args.clients.max(1));
    let mut inputs = Vec::with_capacity(n_clients);
    let mut pull_inputs = Vec::with_capacity(n_servers);

    let server_indices: Vec<usize> = (0..n_servers).collect();
    let mut load_balancer = LoadBalancer::new(
        args.lb_policy.build(),
        args.lb_policy,
        n_servers,
        server_indices,
        0,
        load_registry.clone(),
        false,
    );
    for j in 0..n_servers {
        load_balancer.outputs[j].connect(Server::input, &server_mailboxes[j]);
    }
    let lb_mailbox = Mailbox::new();
    let lb_address = lb_mailbox.address();

    for _ in 0..n_clients {
        let input = EventSource::new()
            .connect(LoadBalancer::input, &lb_mailbox)
            .register(&mut bench);
        inputs.push(input);
    }
    bench = bench.add_model(load_balancer, lb_mailbox, "central-load-balancer");

    for (i, server_mailbox) in server_mailboxes.into_iter().enumerate() {
        let mut release_outputs = vec![Output::default()];
        release_outputs[0].connect(LoadBalancer::release, &lb_address);

        let mut server = Server::new(
            concurrency,
            i,
            release_outputs,
            load_registry.clone(),
            None,
            false,
            None,
            true,
        );
        server
            .pull_output
            .connect(LoadBalancer::pull, &lb_address);
        server.output.connect_sink(sink.clone());
        let pull_input = EventSource::new()
            .connect(Server::request_pull, &server_mailbox)
            .register(&mut bench);
        pull_inputs.push(pull_input);
        bench = bench.add_model(server, server_mailbox, &format!("server-{i}"));
    }

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    schedule_initial_pulls(&simu, &pull_inputs, concurrency)?;

    let capacity = total_capacity as f32;
    let arrival_mean = service_time.mean / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;

    for (input, &client_n) in inputs.iter().zip(task_counts.iter()) {
        if client_n > 0 {
            exp_source(
                &simu,
                input,
                per_client_arrival_mean,
                service_time,
                client_n,
            )?;
        }
    }

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    Ok(calculate_stats(
        &mut output,
        observation,
        total_capacity,
        None,
    ))
}

fn run_push_simulation(
    args: &Args,
    service_time: &ServiceTimeConfig,
    express_lane: Option<&ExpressLaneConfig>,
) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1) as usize;
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let (n_regular, express_lb_id) = match express_lane {
        Some(cfg) => {
            let n_regular = n_servers - cfg.express_size as usize;
            (n_regular, n_clients)
        }
        None => (n_servers, n_clients),
    };

    let mut bench = if args.seed.is_some() {
        SimInit::with_num_threads(1)
    } else {
        SimInit::new()
    };
    let (sink, mut output) = event_queue(SinkState::Enabled);

    let load_registry = LoadRegistry::new(n_servers);
    let server_mailboxes: Vec<Mailbox<Server>> = (0..n_servers).map(|_| Mailbox::new()).collect();

    let task_counts = split_tasks(args.n, args.clients.max(1));
    let mut inputs = Vec::with_capacity(n_clients);
    let mut lb_addresses = Vec::with_capacity(n_clients);

    let client_lb_pool = if express_lane.is_some() {
        n_regular
    } else {
        n_servers
    };

    for i in 0..n_clients {
        let server_indices = subset::assign_subset(
            args.lb_subset_policy,
            client_lb_pool,
            i,
            args.lb_subset_size,
        );
        if args.verbose >= 1 {
            eprintln!("client {i} subset: {server_indices:?}");
        }
        let mut load_balancer = LoadBalancer::new(
            args.lb_policy.build(),
            args.lb_policy,
            client_lb_pool,
            server_indices,
            i,
            load_registry.clone(),
            false,
        );
        for j in 0..client_lb_pool {
            load_balancer.outputs[j].connect(Server::input, &server_mailboxes[j]);
        }
        let lb_mailbox = Mailbox::new();
        lb_addresses.push(lb_mailbox.address());
        let input = EventSource::new()
            .connect(LoadBalancer::input, &lb_mailbox)
            .register(&mut bench);
        bench = bench.add_model(load_balancer, lb_mailbox, &format!("load-balancer-{i}"));
        inputs.push(input);
    }

    let mut express_lb_address = None;
    let mut express_pull_inputs = Vec::new();
    if express_lane.is_some() {
        let express_indices: Vec<usize> = (n_regular..n_servers).collect();
        let mut express_lb = LoadBalancer::new(
            LoadBalancePolicyKind::Centralized.build(),
            LoadBalancePolicyKind::Centralized,
            n_servers,
            express_indices,
            express_lb_id,
            load_registry.clone(),
            true,
        );
        for j in n_regular..n_servers {
            express_lb.outputs[j].connect(Server::input, &server_mailboxes[j]);
        }
        let express_lb_mailbox = Mailbox::new();
        express_lb_address = Some(express_lb_mailbox.address());
        bench = bench.add_model(express_lb, express_lb_mailbox, "express-load-balancer");
    }

    let express_eviction = express_lane.map(|cfg| match cfg.eviction {
        ExpressEvictionConfig::QueueDepth(th) => ExpressEvictionPolicy::QueueDepth(th),
        ExpressEvictionConfig::QueueDelay { threshold, ideal } => {
            let mode = if ideal {
                QueueDelayEvictionMode::ImmediateIdeal
            } else {
                QueueDelayEvictionMode::Monitored
            };
            ExpressEvictionPolicy::QueueDelay { threshold, mode }
        }
        ExpressEvictionConfig::Combined {
            depth_threshold,
            delay_threshold,
        } => ExpressEvictionPolicy::Combined {
            depth_threshold,
            delay_threshold,
        },
    });
    for (i, server_mailbox) in server_mailboxes.into_iter().enumerate() {
        let is_express = express_lane.is_some() && i >= n_regular;
        let n_release = if is_express { n_clients + 1 } else { n_clients };
        let mut release_outputs: Vec<_> = (0..n_release).map(|_| Output::default()).collect();
        for (lb_id, lb_address) in lb_addresses.iter().enumerate() {
            release_outputs[lb_id].connect(LoadBalancer::release, lb_address);
        }
        if is_express {
            if let Some(express_addr) = express_lb_address.as_ref() {
                release_outputs[express_lb_id]
                    .connect(LoadBalancer::release, express_addr);
            }
        }

        let server_express_eviction = if express_lane.is_some() && !is_express {
            express_eviction
        } else {
            None
        };
        let server_express_lb_id = if is_express {
            Some(express_lb_id)
        } else {
            None
        };

        let mut server = Server::new(
            concurrency,
            i,
            release_outputs,
            load_registry.clone(),
            server_express_eviction,
            is_express,
            server_express_lb_id,
            is_express,
        );
        if express_lane.is_some() && !is_express {
            if let Some(express_addr) = express_lb_address.as_ref() {
                server
                    .express_output
                    .connect(LoadBalancer::input, express_addr);
            }
        }
        if is_express {
            if let Some(express_addr) = express_lb_address.as_ref() {
                server
                    .pull_output
                    .connect(LoadBalancer::pull, express_addr);
                let pull_input = EventSource::new()
                    .connect(Server::request_pull, &server_mailbox)
                    .register(&mut bench);
                express_pull_inputs.push(pull_input);
            }
        }
        server.output.connect_sink(sink.clone());
        bench = bench.add_model(server, server_mailbox, &format!("server-{i}"));
    }

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    if !express_pull_inputs.is_empty() {
        schedule_initial_pulls(&simu, &express_pull_inputs, concurrency)?;
    }

    let capacity = total_capacity as f32;
    let arrival_mean = service_time.mean / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;

    for (input, &client_n) in inputs.iter().zip(task_counts.iter()) {
        if client_n > 0 {
            exp_source(
                &simu,
                input,
                per_client_arrival_mean,
                service_time,
                client_n,
            )?;
        }
    }

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    let stats_config = express_lane.map(|cfg| ExpressLaneStatsConfig {
        n_regular: (n_servers - cfg.express_size as usize) as u32,
        express_size: cfg.express_size,
        concurrency,
    });
    Ok(calculate_stats(
        &mut output,
        observation,
        total_capacity,
        stats_config.as_ref(),
    ))
}

fn run_output(stats: Option<ServiceStats>, rates: &Rates, slo: Option<f64>) -> RunOutput {
    let (slo_latency, prob_latency_gt_slo) = match (stats.as_ref(), slo) {
        (Some(stats), Some(slo)) => (
            Some(slo),
            Some(prob_latency_gt_slo(&stats.e2e, slo)),
        ),
        _ => (None, None),
    };
    match stats {
        Some(stats) => RunOutput {
            total_service_rate: rates.total_service_rate,
            per_server_service_rate: rates.per_server_service_rate,
            total_arrival_rate: rates.total_arrival_rate,
            per_client_arrival_rate: rates.per_client_arrival_rate,
            utilization_pct: stats.utilization_pct,
            regular_utilization_pct: stats.regular_utilization_pct,
            express_utilization_pct: stats.express_utilization_pct,
            unloaded_latency_p99: stats.unloaded_latency_p99,
            slo_latency,
            prob_latency_gt_slo,
            e2e: stats.e2e,
            processing_times: stats.processing_times,
            queueing_delays: stats.queueing_delays,
            regular_e2e: stats.regular_e2e,
            express_e2e: stats.express_e2e,
            regular_queueing_delays: stats.regular_queueing_delays,
            express_queueing_delays: stats.express_queueing_delays,
            pre_eviction_queueing_delays: stats.pre_eviction_queueing_delays,
            post_eviction_queueing_delays: stats.post_eviction_queueing_delays,
        },
        None => RunOutput {
            total_service_rate: rates.total_service_rate,
            per_server_service_rate: rates.per_server_service_rate,
            total_arrival_rate: rates.total_arrival_rate,
            per_client_arrival_rate: rates.per_client_arrival_rate,
            utilization_pct: 0.0,
            regular_utilization_pct: None,
            express_utilization_pct: None,
            unloaded_latency_p99: 0.0,
            slo_latency,
            prob_latency_gt_slo,
            e2e: Vec::new(),
            processing_times: Vec::new(),
            queueing_delays: Vec::new(),
            regular_e2e: None,
            express_e2e: None,
            regular_queueing_delays: None,
            express_queueing_delays: None,
            pre_eviction_queueing_delays: None,
            post_eviction_queueing_delays: None,
        },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let slo = validate_slo(args.slo).map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
    let express_lane = validate_expresslane(&args)?;
    let service_time = resolve_service_time(&args)?;
    let rates = compute_rates(&args, service_time.mean);
    rng::enter_run(args.seed);
    let stats = run_simulation(&args, &service_time, express_lane.as_ref());
    rng::exit_run();
    let stats = stats?;

    match args.format {
        OutputFormat::Human => match stats {
            Some(stats) => print_human_stats(&stats, &rates, slo),
            None => println!("no completed tasks"),
        },
        OutputFormat::Json => {
            let output = run_output(stats, &rates, slo);
            let mut stdout = io::stdout().lock();
            serde_json::to_writer(&mut stdout, &output)?;
            stdout.write_all(b"\n")?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn bimodal_config(m0: f32, m1: f32, p0: f32) -> BimodalConfig {
        let modes = [m0, m1];
        let probs = [p0, 1.0 - p0];
        BimodalConfig { modes, probs }
    }

    #[test]
    fn mixture_mean_computes_weighted_average() {
        assert!((mixture_mean(&[0.1, 1.0], &[0.9, 0.1]) - 0.19).abs() < 1e-6);
        assert!((mixture_mean(&[0.5, 0.5], &[0.5, 0.5]) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn resolve_service_time_rejects_invalid_probs() {
        let args = Args::try_parse_from([
            "lb",
            "--service-dist",
            "bimodal",
            "--service-modes",
            "0.1,1.0",
            "--service-mode-probs",
            "0.6,0.3",
        ])
        .unwrap();
        let err = match resolve_service_time(&args) {
            Err(err) => err,
            Ok(_) => panic!("expected validation error"),
        };
        assert!(err.contains("must sum to 1"));
    }

    #[test]
    fn resolve_service_time_rejects_wrong_mode_count() {
        let args = Args::try_parse_from([
            "lb",
            "--service-dist",
            "bimodal",
            "--service-modes",
            "0.1",
            "--service-mode-probs",
            "0.5,0.5",
        ])
        .unwrap();
        let err = match resolve_service_time(&args) {
            Err(err) => err,
            Ok(_) => panic!("expected validation error"),
        };
        assert!(err.contains("exactly 2 values"));
    }

    #[test]
    fn resolve_service_time_rejects_modes_with_non_bimodal_dist() {
        let args = Args::try_parse_from([
            "lb",
            "--service-dist",
            "exponential",
            "--service-modes",
            "0.1,1.0",
        ])
        .unwrap();
        let err = match resolve_service_time(&args) {
            Err(err) => err,
            Ok(_) => panic!("expected validation error"),
        };
        assert!(err.contains("only valid with --service-dist bimodal"));
    }

    #[test]
    fn resolve_service_time_bimodal_sets_mean() {
        let args = Args::try_parse_from([
            "lb",
            "--service-dist",
            "bimodal",
            "--service-modes",
            "0.1,1.0",
            "--service-mode-probs",
            "0.9,0.1",
        ])
        .unwrap();
        let cfg = resolve_service_time(&args).unwrap();
        assert!((cfg.mean - 0.19).abs() < 1e-6);
        assert!(cfg.bimodal.is_some());
    }

    #[test]
    fn select_bimodal_mode_respects_probabilities() {
        let config = bimodal_config(0.1, 1.0, 0.7);
        let mut rng = StdRng::seed_from_u64(42);
        let n = 10_000;
        let mode0_count = (0..n)
            .filter(|_| select_bimodal_mode(&mut rng, &config) == 0.1)
            .count();
        let ratio = mode0_count as f32 / n as f32;
        assert!((ratio - 0.7).abs() < 0.02);
    }

    #[test]
    fn prob_latency_gt_slo_empty_samples() {
        assert_eq!(prob_latency_gt_slo(&[], 5.0), 0.0);
    }

    #[test]
    fn prob_latency_gt_slo_all_below() {
        assert_eq!(prob_latency_gt_slo(&[1.0, 2.0, 3.0], 5.0), 0.0);
    }

    #[test]
    fn prob_latency_gt_slo_some_above() {
        assert!((prob_latency_gt_slo(&[1.0, 6.0, 7.0, 2.0], 5.0) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn validate_slo_rejects_non_positive() {
        assert!(validate_slo(Some(0.0)).is_err());
        assert!(validate_slo(Some(-1.0)).is_err());
    }

    #[test]
    fn verbose_defaults_to_zero() {
        let args = Args::try_parse_from(["lb"]).unwrap();
        assert_eq!(args.verbose, 0);
    }

    #[test]
    fn verbose_count_flag() {
        let args = Args::try_parse_from(["lb", "-v"]).unwrap();
        assert_eq!(args.verbose, 1);
        let args = Args::try_parse_from(["lb", "-vv"]).unwrap();
        assert_eq!(args.verbose, 2);
    }

    #[test]
    fn validate_expresslane_rejects_centralized_policy() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-th",
                "5",
                "--lb-policy",
                "centralized",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("not supported with --lb-policy centralized"));
    }

    #[test]
    fn validate_expresslane_rejects_orphan_flags() {
        let err = validate_expresslane(
            &Args::try_parse_from(["lb", "--express-size", "2"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--expresslane"));

        let err = validate_expresslane(
            &Args::try_parse_from(["lb", "--express-th", "5"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--expresslane"));

        let err = validate_expresslane(
            &Args::try_parse_from(["lb", "--express-del-th", "0.5"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--expresslane"));
    }

    #[test]
    fn validate_expresslane_requires_size_and_one_threshold() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("one of --express-th or --express-del-th"));

        let err = validate_expresslane(
            &Args::try_parse_from(["lb", "--expresslane", "--express-th", "5"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--express-size"));
    }

    #[test]
    fn validate_expresslane_accepts_combined_thresholds() {
        let cfg = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-th",
                "5",
                "--express-del-th",
                "0.5",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap()
        .expect("expected express lane config");
        assert_eq!(cfg.express_size, 2);
        match cfg.eviction {
            ExpressEvictionConfig::Combined {
                depth_threshold,
                delay_threshold,
            } => {
                assert_eq!(depth_threshold, 5);
                assert_eq!(delay_threshold, Duration::from_secs_f64(0.5));
            }
            ExpressEvictionConfig::QueueDepth(_)
            | ExpressEvictionConfig::QueueDelay { .. } => {
                panic!("expected combined eviction")
            }
        }
    }

    #[test]
    fn validate_expresslane_rejects_invalid_delay_threshold() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-del-th",
                "0",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--express-del-th must be positive"));
    }

    #[test]
    fn validate_expresslane_rejects_invalid_size() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "0",
                "--express-th",
                "5",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("must be positive"));

        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "10",
                "--express-th",
                "5",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("must be less than"));
    }

    #[test]
    fn validate_expresslane_accepts_valid_config() {
        let cfg = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-th",
                "5",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap()
        .expect("expected express lane config");
        assert_eq!(cfg.express_size, 2);
        match cfg.eviction {
            ExpressEvictionConfig::QueueDepth(th) => assert_eq!(th, 5),
            ExpressEvictionConfig::QueueDelay { .. } => panic!("expected queue depth eviction"),
            ExpressEvictionConfig::Combined { .. } => panic!("expected queue depth eviction"),
        }
    }

    #[test]
    fn validate_expresslane_accepts_delay_threshold_config() {
        let cfg = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-del-th",
                "0.5",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap()
        .expect("expected express lane config");
        assert_eq!(cfg.express_size, 2);
        match cfg.eviction {
            ExpressEvictionConfig::QueueDelay { threshold, ideal } => {
                assert_eq!(threshold, Duration::from_secs_f64(0.5));
                assert!(!ideal);
            }
            ExpressEvictionConfig::QueueDepth(_) => panic!("expected queue delay eviction"),
            ExpressEvictionConfig::Combined { .. } => panic!("expected queue delay eviction"),
        }
    }

    #[test]
    fn validate_expresslane_accepts_ideal_delay_threshold_config() {
        let cfg = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-del-th",
                "0.5",
                "--ideal",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap()
        .expect("expected express lane config");
        assert_eq!(cfg.express_size, 2);
        match cfg.eviction {
            ExpressEvictionConfig::QueueDelay { threshold, ideal } => {
                assert_eq!(threshold, Duration::from_secs_f64(0.5));
                assert!(ideal);
            }
            ExpressEvictionConfig::QueueDepth(_) => panic!("expected queue delay eviction"),
            ExpressEvictionConfig::Combined { .. } => panic!("expected queue delay eviction"),
        }
    }

    #[test]
    fn validate_expresslane_rejects_ideal_without_expresslane() {
        let err = validate_expresslane(
            &Args::try_parse_from(["lb", "--ideal", "--express-del-th", "0.5"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--ideal requires --expresslane"));
    }

    #[test]
    fn validate_expresslane_rejects_ideal_with_express_th() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-th",
                "5",
                "--ideal",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--ideal requires --express-del-th"));
    }

    #[test]
    fn validate_expresslane_rejects_ideal_without_express_del_th() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--ideal",
                "--servers",
                "10",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--ideal requires --express-del-th"));
    }

    #[test]
    fn expresslane_subset_uses_regular_pool_only() {
        let n_regular = 8;
        let subset = subset::assign_subset(SubsetPolicyKind::Deterministic, n_regular, 0, 0);
        assert!(subset.iter().all(|&idx| idx < n_regular));
        assert_eq!(subset.len(), n_regular);
    }

    #[test]
    fn expresslane_simulation_completes_tasks() {
        let args = Args::try_parse_from([
            "lb",
            "--expresslane",
            "--express-size",
            "1",
            "--express-th",
            "2",
            "--servers",
            "3",
            "--n",
            "1000",
            "--load",
            "0.8",
            "--seed",
            "42",
        ])
        .unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref()).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert_eq!(stats.e2e.len(), 1000);
        assert!(stats.regular_utilization_pct.is_some());
        assert!(stats.express_utilization_pct.is_some());
        let regular_e2e = stats.regular_e2e.as_ref().unwrap();
        let express_e2e = stats.express_e2e.as_ref().unwrap();
        assert_eq!(regular_e2e.len() + express_e2e.len(), 1000);
        let regular_q = stats.regular_queueing_delays.as_ref().unwrap();
        let express_q = stats.express_queueing_delays.as_ref().unwrap();
        assert_eq!(regular_q.len() + express_q.len(), 1000);
        assert!(!express_q.is_empty(), "expected some express-served tasks");
        let pre_q = stats.pre_eviction_queueing_delays.as_ref().unwrap();
        let post_q = stats.post_eviction_queueing_delays.as_ref().unwrap();
        assert_eq!(pre_q.len(), express_q.len());
        assert_eq!(post_q.len(), express_q.len());
        for ((pre, post), total) in pre_q.iter().zip(post_q.iter()).zip(express_q.iter()) {
            assert!(
                (pre + post - total).abs() < 1e-9,
                "pre({pre}) + post({post}) should equal express queueing ({total})"
            );
        }
    }

    #[test]
    fn non_express_run_has_no_split_metrics() {
        let args = Args::try_parse_from(["lb", "--n", "100", "--seed", "42"]).unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        assert!(express_lane.is_none());
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref()).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert!(stats.regular_utilization_pct.is_none());
        assert!(stats.express_utilization_pct.is_none());
        assert!(stats.regular_e2e.is_none());
        assert!(stats.express_e2e.is_none());
        assert!(stats.regular_queueing_delays.is_none());
        assert!(stats.express_queueing_delays.is_none());
        assert!(stats.pre_eviction_queueing_delays.is_none());
        assert!(stats.post_eviction_queueing_delays.is_none());

        let output = run_output(Some(stats), &compute_rates(&args, service_time.mean), None);
        let json = serde_json::to_value(&output).unwrap();
        assert!(json.get("regular_utilization_pct").is_none());
        assert!(json.get("express_utilization_pct").is_none());
        assert!(json.get("regular_e2e").is_none());
        assert!(json.get("express_e2e").is_none());
        assert!(json.get("regular_queueing_delays").is_none());
        assert!(json.get("express_queueing_delays").is_none());
        assert!(json.get("pre_eviction_queueing_delays").is_none());
        assert!(json.get("post_eviction_queueing_delays").is_none());
    }
}
