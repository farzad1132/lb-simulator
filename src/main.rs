use clap::{Parser, ValueEnum};
use nexosim::model::{Context, Model, schedulable};
use nexosim::ports::{
    EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue,
};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};
use std::time::Duration;

const MIN_DURATION_SECS: f32 = 1e-9;

#[derive(Default, Deserialize, Serialize, Clone)]
pub struct Task {
    pub duration: Duration,
    pub finish: MonotonicTime,
    pub start: MonotonicTime,
}

impl Task {
    fn new(start: MonotonicTime, duration: Duration) -> Self {
        Self {
            duration: duration,
            finish: MonotonicTime::EPOCH,
            start: start,
        }
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct Server {
    pub output: Output<Task>,
    busy: bool,
    queue: Vec<Task>,
}

#[Model]
impl Server {
    fn begin_service(&mut self, task: Task, cx: &Context<Self>) {
        self.busy = true;
        if let Err(t) = cx.schedule_event(task.duration, schedulable!(Self::complete), task) {
            eprintln!("could not schedule complete. err: {}", t);
            self.busy = false;
        }
    }

    pub async fn input(&mut self, task: Task, cx: &Context<Self>) {
        if self.busy {
            self.queue.push(task);
        } else {
            self.begin_service(task, cx);
        }
    }

    #[nexosim(schedulable)]
    async fn complete(&mut self, mut task: Task, cx: &Context<Self>) {
        task.finish = cx.time();
        self.output.send(task).await;
        self.busy = false;
        if !self.queue.is_empty() {
            let next = self.queue.remove(0);
            self.begin_service(next, cx);
        }
    }
}

fn sample_exp(rng: &mut impl Rng, mean: f32) -> f32 {
    -mean * rng.random::<f32>().ln()
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ServiceDistribution {
    Exponential,
    Constant,
}

fn sample_service(rng: &mut impl Rng, mean: f32, dist: ServiceDistribution) -> f32 {
    let sample = match dist {
        ServiceDistribution::Exponential => sample_exp(rng, mean),
        ServiceDistribution::Constant => mean,
    };
    sample.max(MIN_DURATION_SECS)
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
    normalized_e2e: Vec<f64>,
    normalized_queueing_delays: Vec<f64>,
}

#[derive(Serialize)]
struct RunOutput {
    utilization_pct: f64,
    normalized_e2e: Vec<f64>,
    normalized_queueing_delays: Vec<f64>,
}

fn calculate_stats(
    output: &mut EventQueueReader<Task>,
    observation: Duration,
) -> Option<ServiceStats> {
    let mut normalized_e2e = Vec::new();
    let mut normalized_queueing_delays = Vec::new();
    let mut busy = Duration::ZERO;

    while let Some(task) = output.try_read() {
        busy += task.duration;
        let unloaded_ns = task.duration.as_nanos();
        if unloaded_ns == 0 {
            continue;
        }
        let e2e_ns = task.finish.duration_since(task.start).as_nanos();
        let unloaded = unloaded_ns as f64 / 1e9;
        let e2e = e2e_ns as f64 / 1e9;
        normalized_e2e.push(e2e / unloaded);
        normalized_queueing_delays.push((e2e - unloaded) / unloaded);
    }

    if normalized_e2e.is_empty() {
        return None;
    }

    let utilization_pct = if observation.is_zero() {
        0.0
    } else {
        busy.as_secs_f64() / observation.as_secs_f64() * 100.0
    };

    Some(ServiceStats {
        utilization_pct,
        normalized_e2e,
        normalized_queueing_delays,
    })
}

const HUMAN_PERCENTILES: [f64; 12] =
    [1.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 99.0, 100.0];

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
    print_percentile_table(
        "normalized e2e (slowdown):",
        &mut stats.normalized_e2e.clone(),
    );
    print_percentile_table(
        "normalized queueing delay:",
        &mut stats.normalized_queueing_delays.clone(),
    );
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
enum OutputFormat {
    #[default]
    Human,
    Json,
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 1.0)]
    arrival_mean: f32,
    #[arg(long, default_value_t = 0.8)]
    service_mean: f32,
    #[arg(long, default_value_t = 1_000_000)]
    n: u32,
    #[arg(long, value_enum, default_value_t = ServiceDistribution::Exponential)]
    service_dist: ServiceDistribution,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
}

fn run_simulation(args: &Args) -> Result<Option<ServiceStats>, nexosim::simulation::SimulationError> {
    let mut server = Server::default();
    let server_mailbox = Mailbox::new();
    let mut bench = SimInit::new();

    let input = EventSource::new()
        .connect(Server::input, &server_mailbox)
        .register(&mut bench);

    let (sink, mut output) = event_queue(SinkState::Enabled);
    server.output.connect_sink(sink);

    let t0 = MonotonicTime::EPOCH;
    let mut simu = bench.add_model(server, server_mailbox, "server").init(t0)?;

    exp_source(
        &simu,
        &input,
        args.arrival_mean,
        args.service_mean,
        args.n,
        args.service_dist,
    )?;

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    Ok(calculate_stats(&mut output, observation))
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
                    normalized_e2e: stats.normalized_e2e,
                    normalized_queueing_delays: stats.normalized_queueing_delays,
                },
                None => RunOutput {
                    utilization_pct: 0.0,
                    normalized_e2e: Vec::new(),
                    normalized_queueing_delays: Vec::new(),
                },
            };
            let mut stdout = io::stdout().lock();
            serde_json::to_writer(&mut stdout, &output)?;
            stdout.write_all(b"\n")?;
        }
    }

    Ok(())
}
