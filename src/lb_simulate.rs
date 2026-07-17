use crate::lb_pull_audit::LbPullAudit;
use crate::load_balancer::LoadBalancer;
use crate::policy::{ApproxSchedKind, LoadBalancePolicyKind, PullPolicyKind};
use crate::server::{
    DispatchMode, ExpressEvictionPolicy, QueueDelayEvictionMode, Server, Task,
};
use crate::sim_util;
use crate::subset::{self, SubsetPolicyKind};
use nexosim::ports::{EventQueueReader, EventSinkReader, EventSource, Output, SinkState, event_queue};
use nexosim::simulation::{EventId, Mailbox, SchedulingError, SimInit, Simulation};
use nexosim::time::MonotonicTime;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;

const MIN_DURATION_SECS: f32 = 1e-9;
const SERVICE_MEAN: f32 = 1.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LbServiceDistribution {
    Exponential,
    Constant,
    Bimodal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LbArrivalDistribution {
    Exponential,
    Constant,
}

struct BimodalConfig {
    modes: [f32; 2],
    probs: [f32; 2],
}

pub struct ServiceTimeConfig {
    pub mean: f32,
    dist: LbServiceDistribution,
    bimodal: Option<BimodalConfig>,
}

const PROB_SUM_TOLERANCE: f32 = 1e-6;

#[derive(Debug, Clone)]
pub enum ExpressEvictionConfig {
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

#[derive(Debug, Clone)]
pub struct ExpressLaneConfig {
    pub express_size: u32,
    pub eviction: ExpressEvictionConfig,
}

pub struct LbRunArgs {
    pub load: f32,
    pub n: u32,
    pub service_dist: LbServiceDistribution,
    pub arrival: LbArrivalDistribution,
    pub service_modes: Option<Vec<f32>>,
    pub service_mode_probs: Option<Vec<f32>>,
    pub servers: u32,
    pub concurrency: u32,
    pub lb_policy: LoadBalancePolicyKind,
    pub pull_policy: Option<PullPolicyKind>,
    pub lb_subset_size: u32,
    pub lb_subset_policy: SubsetPolicyKind,
    pub clients: u32,
    pub verbose: u8,
    pub approx_sched: Option<ApproxSchedKind>,
    pub pull_audit: Option<Arc<LbPullAudit>>,
    pub express_lane: Option<ExpressLaneConfig>,
    pub work_shedding: Option<Duration>,
}

pub struct LbServiceStats {
    pub utilization_pct: f64,
    pub regular_utilization_pct: Option<f64>,
    pub express_utilization_pct: Option<f64>,
    pub unloaded_latency_p99: f64,
    pub inter_arrival: Vec<f64>,
    pub inter_departure: Vec<f64>,
    pub e2e: Vec<f64>,
    pub processing_times: Vec<f64>,
    pub queueing_delays: Vec<f64>,
    pub regular_e2e: Option<Vec<f64>>,
    pub express_e2e: Option<Vec<f64>>,
    pub regular_queueing_delays: Option<Vec<f64>>,
    pub express_queueing_delays: Option<Vec<f64>>,
    pub pre_eviction_queueing_delays: Option<Vec<f64>>,
    pub post_eviction_queueing_delays: Option<Vec<f64>>,
    pub pct_shed_requests: Option<f64>,
}

struct ExpressLaneStatsConfig {
    n_regular: u32,
    express_size: u32,
    concurrency: u32,
}

pub fn resolve_service_time(args: &LbRunArgs) -> Result<ServiceTimeConfig, String> {
    match args.service_dist {
        LbServiceDistribution::Bimodal => {
            let modes = args
                .service_modes
                .as_ref()
                .ok_or("service_modes required for bimodal")?;
            let probs = args
                .service_mode_probs
                .as_ref()
                .ok_or("service_mode_probs required for bimodal")?;
            if modes.len() != 2 {
                return Err(format!(
                    "service_modes requires exactly 2 values, got {}",
                    modes.len()
                ));
            }
            if probs.len() != 2 {
                return Err(format!(
                    "service_mode_probs requires exactly 2 values, got {}",
                    probs.len()
                ));
            }
            if modes.iter().any(|m| *m <= 0.0 || !m.is_finite()) {
                return Err("service_modes values must be positive and finite".into());
            }
            if probs.iter().any(|p| *p <= 0.0 || !p.is_finite()) {
                return Err("service_mode_probs values must be positive and finite".into());
            }
            let prob_sum: f32 = probs.iter().sum();
            if (prob_sum - 1.0).abs() > PROB_SUM_TOLERANCE {
                return Err(format!("service_mode_probs must sum to 1, got {prob_sum}"));
            }
            let modes_arr = [modes[0], modes[1]];
            let probs_arr = [probs[0], probs[1]];
            let mean = modes_arr[0] * probs_arr[0] + modes_arr[1] * probs_arr[1];
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
                    "service_modes and service_mode_probs are only valid with bimodal".into(),
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

fn sample_exp(rng: &mut impl Rng, mean: f32) -> f32 {
    let u = loop {
        let u = 1.0 - rng.random::<f32>();
        if u > 0.0 && u.is_finite() {
            break u;
        }
    };
    (-mean * u.ln()).max(MIN_DURATION_SECS)
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
        LbServiceDistribution::Exponential => sample_exp(rng, service_time.mean),
        LbServiceDistribution::Constant => service_time.mean.max(MIN_DURATION_SECS),
        LbServiceDistribution::Bimodal => {
            sample_bimodal(rng, service_time.bimodal.as_ref().expect("bimodal config"))
        }
    }
}

fn sample_inter_arrival(
    rng: &mut impl Rng,
    arrival_dist: LbArrivalDistribution,
    per_client_arrival_mean: f32,
) -> f32 {
    match arrival_dist {
        LbArrivalDistribution::Exponential => sample_exp(rng, per_client_arrival_mean),
        LbArrivalDistribution::Constant => per_client_arrival_mean.max(MIN_DURATION_SECS),
    }
}

fn task_source(
    sim: &Simulation,
    input: &EventId<Task>,
    arrival_mean: f32,
    per_client_arrival_mean: f32,
    arrival_dist: LbArrivalDistribution,
    client_index: usize,
    service_time: &ServiceTimeConfig,
    n: u32,
) -> Result<(), SchedulingError> {
    let scheduler = sim.scheduler();
    let t0 = sim.time();
    let mut offset = match arrival_dist {
        LbArrivalDistribution::Exponential => Duration::ZERO,
        LbArrivalDistribution::Constant => {
            Duration::from_secs_f32((client_index as f32 * arrival_mean).max(MIN_DURATION_SECS))
        }
    };

    crate::rng::with_rng(|rng| {
        for _ in 0..n {
            if matches!(arrival_dist, LbArrivalDistribution::Exponential) {
                let gap = sample_inter_arrival(rng, arrival_dist, per_client_arrival_mean);
                offset += Duration::from_secs_f32(gap);
            }
            let duration = Duration::from_secs_f32(sample_service(rng, service_time));
            let task = Task::new(t0 + offset, duration);
            scheduler.schedule_event(offset, input, task)?;
            if matches!(arrival_dist, LbArrivalDistribution::Constant) {
                let gap = sample_inter_arrival(rng, arrival_dist, per_client_arrival_mean);
                offset += Duration::from_secs_f32(gap);
            }
        }
        Ok::<(), SchedulingError>(())
    })?;
    Ok(())
}

fn split_tasks(n: u32, clients: u32) -> Vec<u32> {
    let clients = clients.max(1);
    let base = n / clients;
    let rem = n % clients;
    (0..clients).map(|i| base + u32::from(i < rem)).collect()
}

fn duration_secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn time_secs(time: MonotonicTime) -> f64 {
    duration_secs(time.duration_since(MonotonicTime::EPOCH))
}

fn consecutive_diffs(times: &[f64]) -> Vec<f64> {
    if times.len() < 2 {
        return Vec::new();
    }
    let mut sorted = times.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.windows(2).map(|w| w[1] - w[0]).collect()
}

fn pool_utilization_pct(busy: Duration, observation: Duration, pool_capacity: u32) -> f64 {
    if observation.is_zero() || pool_capacity == 0 {
        0.0
    } else {
        busy.as_secs_f64() / (observation.as_secs_f64() * f64::from(pool_capacity)) * 100.0
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    let idx = ((sorted.len() - 1) as f64 * pct / 100.0).round() as usize;
    sorted[idx]
}

fn calculate_stats(
    output: &mut EventQueueReader<Task>,
    observation: Duration,
    total_capacity: u32,
    express_lane: Option<&ExpressLaneStatsConfig>,
    work_shedding: bool,
) -> Option<LbServiceStats> {
    let mut task_samples: Vec<(f64, f64, bool, Option<f64>, Option<f64>)> = Vec::new();
    let mut arrival_times: Vec<f64> = Vec::new();
    let mut departure_times: Vec<f64> = Vec::new();
    let mut busy = Duration::ZERO;
    let mut regular_busy = Duration::ZERO;
    let mut express_busy = Duration::ZERO;
    let mut total_requests = 0usize;
    let mut shed_requests = 0usize;

    while let Some(task) = output.try_read() {
        total_requests += 1;
        if work_shedding && task.shed_at.is_some() {
            shed_requests += 1;
        }
        arrival_times.push(time_secs(task.start));
        departure_times.push(time_secs(task.finish));
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
    let inter_arrival = consecutive_diffs(&arrival_times);
    let inter_departure = consecutive_diffs(&departure_times);

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

    let pct_shed_requests = if work_shedding && total_requests > 0 {
        Some(shed_requests as f64 / total_requests as f64 * 100.0)
    } else {
        None
    };

    Some(LbServiceStats {
        utilization_pct,
        regular_utilization_pct,
        express_utilization_pct,
        unloaded_latency_p99,
        inter_arrival,
        inter_departure,
        e2e,
        processing_times,
        queueing_delays,
        regular_e2e,
        express_e2e,
        regular_queueing_delays,
        express_queueing_delays,
        pre_eviction_queueing_delays,
        post_eviction_queueing_delays,
        pct_shed_requests,
    })
}

pub fn run(
    args: &LbRunArgs,
) -> Result<Option<LbServiceStats>, Box<dyn std::error::Error>> {
    let service_time = resolve_service_time(args)?;
    if args.lb_policy.is_centralized() {
        return run_centralized_simulation(args, &service_time).map_err(Into::into);
    }
    run_push_simulation(args, &service_time).map_err(Into::into)
}

fn run_centralized_simulation(
    args: &LbRunArgs,
    service_time: &ServiceTimeConfig,
) -> Result<Option<LbServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1) as usize;
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let mut bench = SimInit::with_num_threads(1);
    let (sink, mut output) = event_queue(SinkState::Enabled);

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
        false,
        None,
        None,
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
            None,
            None,
            false,
            None,
            DispatchMode::Centralized,
            None,
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

    sim_util::schedule_initial_pulls(&simu, &pull_inputs, concurrency)?;

    let capacity = total_capacity as f32;
    let arrival_mean = service_time.mean / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;

    for (client_index, (input, &client_n)) in inputs.iter().zip(task_counts.iter()).enumerate() {
        if client_n > 0 {
            task_source(
                &simu,
                input,
                arrival_mean,
                per_client_arrival_mean,
                args.arrival,
                client_index,
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
        false,
    ))
}

fn run_push_simulation(
    args: &LbRunArgs,
    service_time: &ServiceTimeConfig,
) -> Result<Option<LbServiceStats>, nexosim::simulation::SimulationError> {
    let n_clients = args.clients.max(1) as usize;
    let n_servers = args.servers.max(1) as usize;
    let concurrency = args.concurrency.max(1);
    let total_capacity = args.servers.max(1) * concurrency;

    let (n_regular, express_lb_id) = match &args.express_lane {
        Some(cfg) => {
            let n_regular = n_servers - cfg.express_size as usize;
            (n_regular, n_clients)
        }
        None => (n_servers, n_clients),
    };

    let mut bench = SimInit::with_num_threads(1);
    let (sink, mut output) = event_queue(SinkState::Enabled);

    let server_mailboxes: Vec<Mailbox<Server>> = (0..n_servers).map(|_| Mailbox::new()).collect();

    let task_counts = split_tasks(args.n, args.clients.max(1));
    let mut inputs = Vec::with_capacity(n_clients);
    let mut lb_addresses = Vec::with_capacity(n_clients);

    let client_lb_pool = if args.express_lane.is_some() {
        n_regular
    } else {
        n_servers
    };

    let is_approx = args.lb_policy.is_approx();
    let pull_audit = args.pull_audit.clone();

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
        let (policy, lb_policy) = if is_approx {
            (
                args.pull_policy
                    .expect("pull_policy validated before simulation")
                    .build(),
                LoadBalancePolicyKind::Approx,
            )
        } else {
            (args.lb_policy.build(), args.lb_policy)
        };
        let mut load_balancer = LoadBalancer::new(
            policy,
            lb_policy,
            client_lb_pool,
            server_indices,
            i,
            false,
            args.approx_sched,
            pull_audit.clone(),
        );
        for j in 0..client_lb_pool {
            load_balancer.outputs[j].connect(Server::input, &server_mailboxes[j]);
            if is_approx {
                load_balancer.pull_intent_outputs[j]
                    .connect(Server::receive_pull_intent, &server_mailboxes[j]);
            }
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
    if let Some(_express_lane) = &args.express_lane {
        let express_indices: Vec<usize> = (n_regular..n_servers).collect();
        let mut express_lb = LoadBalancer::new(
            LoadBalancePolicyKind::Centralized.build(),
            LoadBalancePolicyKind::Centralized,
            n_servers,
            express_indices,
            express_lb_id,
            true,
            None,
            None,
        );
        for j in n_regular..n_servers {
            express_lb.outputs[j].connect(Server::input, &server_mailboxes[j]);
        }
        let express_lb_mailbox = Mailbox::new();
        express_lb_address = Some(express_lb_mailbox.address());
        bench = bench.add_model(express_lb, express_lb_mailbox, "express-load-balancer");
    }

    let express_eviction = args.express_lane.as_ref().map(|cfg| match cfg.eviction {
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
        let is_express = args.express_lane.is_some() && i >= n_regular;
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

        let server_express_eviction = if args.express_lane.is_some() && !is_express {
            express_eviction
        } else {
            None
        };
        let server_express_lb_id = if is_express {
            Some(express_lb_id)
        } else {
            None
        };

        let dispatch_mode = if is_express {
            DispatchMode::Centralized
        } else if is_approx {
            DispatchMode::Approx
        } else {
            DispatchMode::Push
        };

        let server_work_shedding = if !is_express && dispatch_mode == DispatchMode::Push {
            args.work_shedding
        } else {
            None
        };

        let server_pull_audit = if is_approx && !is_express {
            pull_audit.clone()
        } else {
            None
        };

        let mut server = Server::new(
            concurrency,
            i,
            release_outputs,
            server_express_eviction,
            server_work_shedding,
            is_express,
            server_express_lb_id,
            dispatch_mode,
            server_pull_audit,
        );
        if is_approx && !is_express {
            let mut pull_outputs: Vec<_> = (0..n_clients).map(|_| Output::default()).collect();
            for (lb_id, lb_address) in lb_addresses.iter().enumerate() {
                pull_outputs[lb_id].connect(LoadBalancer::pull, lb_address);
            }
            server.set_pull_outputs(pull_outputs);
        }
        if args.express_lane.is_some() && !is_express {
            if let Some(express_addr) = express_lb_address.as_ref() {
                server
                    .express_output
                    .connect(LoadBalancer::input, express_addr);
            }
        }
        if server_work_shedding.is_some() {
            let mut shed_outputs: Vec<_> = (0..n_clients).map(|_| Output::default()).collect();
            for (lb_id, lb_address) in lb_addresses.iter().enumerate() {
                shed_outputs[lb_id].connect(LoadBalancer::input, lb_address);
            }
            server.set_shed_outputs(shed_outputs);
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
        sim_util::schedule_initial_pulls(&simu, &express_pull_inputs, concurrency)?;
    }

    let capacity = total_capacity as f32;
    let arrival_mean = service_time.mean / (args.load * capacity);
    let per_client_arrival_mean = arrival_mean * n_clients as f32;

    for (client_index, (input, &client_n)) in inputs.iter().zip(task_counts.iter()).enumerate() {
        if client_n > 0 {
            task_source(
                &simu,
                input,
                arrival_mean,
                per_client_arrival_mean,
                args.arrival,
                client_index,
                service_time,
                client_n,
            )?;
        }
    }

    simu.run()?;

    let observation = simu.time().duration_since(t0);
    let stats_config = args.express_lane.as_ref().map(|cfg| ExpressLaneStatsConfig {
        n_regular: (n_servers - cfg.express_size as usize) as u32,
        express_size: cfg.express_size,
        concurrency,
    });
    Ok(calculate_stats(
        &mut output,
        observation,
        total_capacity,
        stats_config.as_ref(),
        args.work_shedding.is_some(),
    ))
}
