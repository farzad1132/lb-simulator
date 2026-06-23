mod load_balancer;
mod policy;
mod server;

use clap::{Parser, ValueEnum};
use load_balancer::LoadBalancer;
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use policy::LoadBalancePolicyKind;
use rand::Rng;
use serde::Serialize;
use server::{Server, Task};
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Duration;

const MIN_DURATION_SECS: f32 = 1e-9;
const SERVICE_MEAN: f32 = 1.0;
const SLO_MULTIPLIER: f64 = 5.0;

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
}

fn sample_service(rng: &mut impl Rng, mean: f32, dist: ServiceDistribution) -> f32 {
    match dist {
        ServiceDistribution::Exponential => sample_exp(rng, mean),
        ServiceDistribution::Constant => mean.max(MIN_DURATION_SECS),
    }
}

fn exp_source(
    sim: &Simulation,
    input: &EventId<Task>,
    arrival_mean: f32,
    service_mean: f32,
    n: u32,
    service_dist: ServiceDistribution,
) -> Result<(), SchedulingError> {
    let scheduler = sim.scheduler();
    let t0 = sim.time();
    let mut rng = rand::rng();
    let mut offset = Duration::ZERO;

    for _ in 0..n {
        offset += Duration::from_secs_f32(sample_exp(&mut rng, arrival_mean));
        let duration =
            Duration::from_secs_f32(sample_service(&mut rng, service_mean, service_dist));
        let task = Task::new(t0 + offset, duration);
        scheduler.schedule_event(offset, input, task)?;
    }
    Ok(())
}

struct ServiceStats {
    utilization_pct: f64,
    unloaded_latency_p99: f64,
    slo_latency: f64,
    e2e: Vec<f64>,
    queueing_delays: Vec<f64>,
}

#[derive(Serialize)]
struct RunOutput {
    utilization_pct: f64,
    unloaded_latency_p99: f64,
    slo_latency: f64,
    e2e: Vec<f64>,
    queueing_delays: Vec<f64>,
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

    let slo_latency = SLO_MULTIPLIER * unloaded_latency_p99;
    let e2e: Vec<f64> = task_samples.iter().map(|(e2e, _)| *e2e).collect();
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
        slo_latency,
        e2e,
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

fn print_human_stats(stats: &ServiceStats) {
    println!("utilization: {:.2}%", stats.utilization_pct);
    println!("unloaded latency (p99): {:.6}s", stats.unloaded_latency_p99);
    println!("SLO latency: {:.6}s", stats.slo_latency);
    print_percentile_table("e2e latency (s):", &mut stats.e2e.clone());
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
    #[arg(long, default_value_t = 1)]
    servers: u32,
    #[arg(long, default_value_t = 1)]
    concurrency: u32,
    #[arg(long, value_enum, default_value_t = LoadBalancePolicyKind::Random)]
    lb_policy: LoadBalancePolicyKind,
    #[arg(long, default_value_t = 1)]
    clients: u32,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

fn split_tasks(n: u32, clients: u32) -> Vec<u32> {
    let clients = clients.max(1);
    let base = n / clients;
    let rem = n % clients;
    (0..clients).map(|i| base + u32::from(i < rem)).collect()
}

fn run_simulation(
    args: &Args,
) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1);
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let mut bench = SimInit::new();
    let (sink, mut output) = event_queue(SinkState::Enabled);

    let server_mailboxes: Vec<Mailbox<Server>> = (0..n_servers).map(|_| Mailbox::new()).collect();
    let server_loads: Vec<Arc<AtomicU32>> =
        (0..n_servers).map(|_| Arc::new(AtomicU32::new(0))).collect();

    let task_counts = split_tasks(args.n, n_clients);
    let mut inputs = Vec::with_capacity(n_clients as usize);

    for i in 0..n_clients as usize {
        let mut load_balancer =
            LoadBalancer::new(args.lb_policy.build(), server_loads.clone(), n_servers);
        for j in 0..n_servers {
            load_balancer.outputs[j].connect(Server::input, &server_mailboxes[j]);
        }
        let lb_mailbox = Mailbox::new();
        let input = EventSource::new()
            .connect(LoadBalancer::input, &lb_mailbox)
            .register(&mut bench);
        bench = bench.add_model(load_balancer, lb_mailbox, &format!("load-balancer-{i}"));
        inputs.push(input);
    }

    for (i, server_mailbox) in server_mailboxes.into_iter().enumerate() {
        let mut server = Server::new(concurrency, server_loads[i].clone());
        server.output.connect_sink(sink.clone());
        bench = bench.add_model(server, server_mailbox, &format!("server-{i}"));
    }

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.init(t0)?;

    let capacity = total_capacity as f32;
    let arrival_mean = SERVICE_MEAN / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;

    for (input, &client_n) in inputs.iter().zip(task_counts.iter()) {
        if client_n > 0 {
            exp_source(
                &simu,
                input,
                per_client_arrival_mean,
                SERVICE_MEAN,
                client_n,
                args.service_dist,
            )?;
        }
    }

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    Ok(calculate_stats(&mut output, observation, total_capacity))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let stats = run_simulation(&args)?;

    match args.format {
        OutputFormat::Human => match stats {
            Some(stats) => print_human_stats(&stats),
            None => println!("no completed tasks"),
        },
        OutputFormat::Json => {
            let output = match stats {
                Some(stats) => RunOutput {
                    utilization_pct: stats.utilization_pct,
                    unloaded_latency_p99: stats.unloaded_latency_p99,
                    slo_latency: stats.slo_latency,
                    e2e: stats.e2e,
                    queueing_delays: stats.queueing_delays,
                },
                None => RunOutput {
                    utilization_pct: 0.0,
                    unloaded_latency_p99: 0.0,
                    slo_latency: 0.0,
                    e2e: Vec::new(),
                    queueing_delays: Vec::new(),
                },
            };
            let mut stdout = io::stdout().lock();
            serde_json::to_writer(&mut stdout, &output)?;
            stdout.write_all(b"\n")?;
        }
    }

    Ok(())
}
