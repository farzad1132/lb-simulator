mod load_balancer;
mod policy;
mod rng {
    pub use lb::rng::*;
}
mod server;

use clap::{Parser, ValueEnum};
use load_balancer::LoadBalancer;
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use policy::LoadBalancePolicyKind;
use rand::Rng;
use serde::Serialize;
use server::{Server, Task};
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
    unloaded_latency_p99: f64,
    e2e: Vec<f64>,
    processing_times: Vec<f64>,
    queueing_delays: Vec<f64>,
}

#[derive(Serialize)]
struct RunOutput {
    total_service_rate: f64,
    per_server_service_rate: f64,
    total_arrival_rate: f64,
    per_client_arrival_rate: f64,
    utilization_pct: f64,
    unloaded_latency_p99: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    slo_latency: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prob_latency_gt_slo: Option<f64>,
    e2e: Vec<f64>,
    processing_times: Vec<f64>,
    queueing_delays: Vec<f64>,
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

fn calculate_stats(
    output: &mut EventQueueReader<Task>,
    observation: Duration,
    total_capacity: u32,
) -> Option<ServiceStats> {
    let mut task_samples = Vec::new();
    let mut busy = Duration::ZERO;

    while let Some(task) = output.try_read() {
        busy += task.duration;
        let unloaded_ns = task.duration.as_nanos();
        if unloaded_ns == 0 {
            continue;
        }
        let e2e_ns = task.finish.duration_since(task.start).as_nanos();
        task_samples.push((e2e_ns as f64 / 1e9, unloaded_ns as f64 / 1e9));
    }

    if task_samples.is_empty() {
        return None;
    }

    let mut unloaded_samples: Vec<f64> =
        task_samples.iter().map(|(_, duration)| *duration).collect();
    unloaded_samples.sort_by(f64::total_cmp);
    let unloaded_latency_p99 = percentile(&unloaded_samples, 99.0);
    if unloaded_latency_p99 == 0.0 {
        return None;
    }

    let e2e: Vec<f64> = task_samples.iter().map(|(e2e, _)| *e2e).collect();
    let processing_times: Vec<f64> = task_samples.iter().map(|(_, duration)| *duration).collect();
    let queueing_delays: Vec<f64> = task_samples
        .iter()
        .map(|(e2e, duration)| e2e - duration)
        .collect();

    let utilization_pct = if observation.is_zero() || total_capacity == 0 {
        0.0
    } else {
        busy.as_secs_f64() / (observation.as_secs_f64() * f64::from(total_capacity)) * 100.0
    };

    Some(ServiceStats {
        utilization_pct,
        unloaded_latency_p99,
        e2e,
        processing_times,
        queueing_delays,
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
    #[arg(long, default_value_t = 1)]
    clients: u32,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    slo: Option<f64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

fn random_server_subset(n_servers: usize, subset_size: u32) -> Vec<usize> {
    let k = if subset_size == 0 {
        n_servers
    } else {
        (subset_size as usize).min(n_servers).max(1)
    };
    let mut indices: Vec<usize> = (0..n_servers).collect();
    rng::shuffle(&mut indices);
    indices.truncate(k);
    indices
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
) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1);
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let mut bench = if args.seed.is_some() {
        SimInit::with_num_threads(1)
    } else {
        SimInit::new()
    };
    let (sink, mut output) = event_queue(SinkState::Enabled);

    let server_mailboxes: Vec<Mailbox<Server>> = (0..n_servers).map(|_| Mailbox::new()).collect();

    let task_counts = split_tasks(args.n, n_clients);
    let mut inputs = Vec::with_capacity(n_clients as usize);
    let mut lb_addresses = Vec::with_capacity(n_clients as usize);

    for i in 0..n_clients as usize {
        let server_indices = random_server_subset(n_servers, args.lb_subset_size);
        let mut load_balancer = LoadBalancer::new(
            args.lb_policy.build(),
            n_servers,
            server_indices,
            i,
        );
        for j in 0..n_servers {
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

    for (i, server_mailbox) in server_mailboxes.into_iter().enumerate() {
        let mut release_outputs: Vec<_> = (0..n_clients as usize)
            .map(|_| Output::default())
            .collect();
        for (lb_id, lb_address) in lb_addresses.iter().enumerate() {
            release_outputs[lb_id].connect(LoadBalancer::release, lb_address);
        }
        let mut server = Server::new(concurrency, i, release_outputs);
        server.output.connect_sink(sink.clone());
        bench = bench.add_model(server, server_mailbox, &format!("server-{i}"));
    }

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

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
    Ok(calculate_stats(&mut output, observation, total_capacity))
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
            unloaded_latency_p99: stats.unloaded_latency_p99,
            slo_latency,
            prob_latency_gt_slo,
            e2e: stats.e2e,
            processing_times: stats.processing_times,
            queueing_delays: stats.queueing_delays,
        },
        None => RunOutput {
            total_service_rate: rates.total_service_rate,
            per_server_service_rate: rates.per_server_service_rate,
            total_arrival_rate: rates.total_arrival_rate,
            per_client_arrival_rate: rates.per_client_arrival_rate,
            utilization_pct: 0.0,
            unloaded_latency_p99: 0.0,
            slo_latency,
            prob_latency_gt_slo,
            e2e: Vec::new(),
            processing_times: Vec::new(),
            queueing_delays: Vec::new(),
        },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let slo = validate_slo(args.slo).map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
    let service_time = resolve_service_time(&args)?;
    let rates = compute_rates(&args, service_time.mean);
    rng::enter_run(args.seed);
    let stats = run_simulation(&args, &service_time);
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
}
