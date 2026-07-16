use clap::{Parser, ValueEnum};
use lb::lb_simulate::{LbArrivalDistribution, LbRunArgs, LbServiceDistribution, LbServiceStats};
use lb::policy::{validate_no_bind, validate_pull_policy, LoadBalancePolicyKind, PullPolicyKind};
use lb::subset::SubsetPolicyKind;
use serde::Serialize;
use std::io::{self, Write};
use std::time::Duration;

mod rng {
    pub use lb::rng::*;
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ServiceDistribution {
    Exponential,
    Constant,
    Bimodal,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ArrivalDistribution {
    Exponential,
    Constant,
}

struct ServiceTimeConfig {
    mean: f32,
}

fn resolve_service_time(args: &Args) -> Result<ServiceTimeConfig, String> {
    let cfg = lb::lb_simulate::resolve_service_time(&lb_run_args_from_cli(args, None, None))?;
    Ok(ServiceTimeConfig { mean: cfg.mean })
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
    inter_arrival: Vec<f64>,
    inter_departure: Vec<f64>,
    e2e: Vec<f64>,
    processing_times: Vec<f64>,
    queueing_delays: Vec<f64>,
    regular_e2e: Option<Vec<f64>>,
    express_e2e: Option<Vec<f64>>,
    regular_queueing_delays: Option<Vec<f64>>,
    express_queueing_delays: Option<Vec<f64>>,
    pre_eviction_queueing_delays: Option<Vec<f64>>,
    post_eviction_queueing_delays: Option<Vec<f64>>,
    pct_shed_requests: Option<f64>,
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
    inter_arrival: Vec<f64>,
    inter_departure: Vec<f64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pct_shed_requests: Option<f64>,
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
    if let Some(pct) = stats.pct_shed_requests {
        println!("shed requests: {pct:.2}%");
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
    #[arg(long, value_enum, default_value_t = ArrivalDistribution::Exponential)]
    arrival: ArrivalDistribution,
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
    #[arg(long, value_enum)]
    pull_policy: Option<PullPolicyKind>,
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
    #[arg(long)]
    shed_delay: Option<f64>,
    #[arg(long)]
    no_bind: bool,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

    if args.lb_policy.is_centralized() || args.lb_policy.is_approx() {
        let policy = if args.lb_policy.is_centralized() {
            "centralized"
        } else {
            "approx"
        };
        if args.expresslane || has_express_flags {
            return Err(format!(
                "--expresslane is not supported with --lb-policy {policy}"
            ));
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

fn validate_shed_delay(value: f64) -> Result<Duration, String> {
    if value > 0.0 && value.is_finite() {
        Ok(Duration::from_secs_f64(value))
    } else {
        Err("--shed-delay must be positive and finite".into())
    }
}

fn validate_work_shedding(args: &Args) -> Result<Option<Duration>, String> {
    if args.shed_delay.is_none() {
        return Ok(None);
    }

    if args.lb_policy.is_centralized() || args.lb_policy.is_approx() {
        let policy = if args.lb_policy.is_centralized() {
            "centralized"
        } else {
            "approx"
        };
        return Err(format!(
            "--shed-delay is not supported with --lb-policy {policy}"
        ));
    }

    let has_express_flags = args.expresslane
        || args.express_size.is_some()
        || args.express_th.is_some()
        || args.express_del_th.is_some();
    if has_express_flags {
        return Err("--shed-delay cannot be combined with --expresslane".into());
    }

    validate_shed_delay(args.shed_delay.expect("checked above"))
        .map(Some)
}

fn service_stats_from_lb(stats: LbServiceStats) -> ServiceStats {
    ServiceStats {
        utilization_pct: stats.utilization_pct,
        regular_utilization_pct: stats.regular_utilization_pct,
        express_utilization_pct: stats.express_utilization_pct,
        unloaded_latency_p99: stats.unloaded_latency_p99,
        inter_arrival: stats.inter_arrival,
        inter_departure: stats.inter_departure,
        e2e: stats.e2e,
        processing_times: stats.processing_times,
        queueing_delays: stats.queueing_delays,
        regular_e2e: stats.regular_e2e,
        express_e2e: stats.express_e2e,
        regular_queueing_delays: stats.regular_queueing_delays,
        express_queueing_delays: stats.express_queueing_delays,
        pre_eviction_queueing_delays: stats.pre_eviction_queueing_delays,
        post_eviction_queueing_delays: stats.post_eviction_queueing_delays,
        pct_shed_requests: stats.pct_shed_requests,
    }
}

fn lb_run_args_from_cli(
    args: &Args,
    express_lane: Option<ExpressLaneConfig>,
    work_shedding: Option<Duration>,
) -> LbRunArgs {
    LbRunArgs {
        load: args.load,
        n: args.n,
        service_dist: match args.service_dist {
            ServiceDistribution::Exponential => LbServiceDistribution::Exponential,
            ServiceDistribution::Constant => LbServiceDistribution::Constant,
            ServiceDistribution::Bimodal => LbServiceDistribution::Bimodal,
        },
        arrival: match args.arrival {
            ArrivalDistribution::Exponential => LbArrivalDistribution::Exponential,
            ArrivalDistribution::Constant => LbArrivalDistribution::Constant,
        },
        service_modes: args.service_modes.clone(),
        service_mode_probs: args.service_mode_probs.clone(),
        servers: args.servers,
        concurrency: args.concurrency,
        lb_policy: args.lb_policy,
        pull_policy: args.pull_policy,
        lb_subset_size: args.lb_subset_size,
        lb_subset_policy: args.lb_subset_policy,
        clients: args.clients,
        verbose: args.verbose,
        no_bind: args.no_bind,
        pull_audit: None,
        express_lane: express_lane.map(|cfg| lb::lb_simulate::ExpressLaneConfig {
            express_size: cfg.express_size,
            eviction: match cfg.eviction {
                ExpressEvictionConfig::QueueDepth(th) => {
                    lb::lb_simulate::ExpressEvictionConfig::QueueDepth(th)
                }
                ExpressEvictionConfig::QueueDelay { threshold, ideal } => {
                    lb::lb_simulate::ExpressEvictionConfig::QueueDelay { threshold, ideal }
                }
                ExpressEvictionConfig::Combined {
                    depth_threshold,
                    delay_threshold,
                } => lb::lb_simulate::ExpressEvictionConfig::Combined {
                    depth_threshold,
                    delay_threshold,
                },
            },
        }),
        work_shedding,
    }
}

fn run_simulation(
    args: &Args,
    _service_time: &ServiceTimeConfig,
    express_lane: Option<&ExpressLaneConfig>,
    work_shedding: Option<Duration>,
) -> Result<Option<ServiceStats>, Box<dyn std::error::Error>> {
    let lb_args = lb_run_args_from_cli(args, express_lane.cloned(), work_shedding);
    lb::lb_simulate::run(&lb_args).map(|stats| stats.map(service_stats_from_lb))
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
            inter_arrival: stats.inter_arrival,
            inter_departure: stats.inter_departure,
            e2e: stats.e2e,
            processing_times: stats.processing_times,
            queueing_delays: stats.queueing_delays,
            regular_e2e: stats.regular_e2e,
            express_e2e: stats.express_e2e,
            regular_queueing_delays: stats.regular_queueing_delays,
            express_queueing_delays: stats.express_queueing_delays,
            pre_eviction_queueing_delays: stats.pre_eviction_queueing_delays,
            post_eviction_queueing_delays: stats.post_eviction_queueing_delays,
            pct_shed_requests: stats.pct_shed_requests,
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
            inter_arrival: Vec::new(),
            inter_departure: Vec::new(),
            e2e: Vec::new(),
            processing_times: Vec::new(),
            queueing_delays: Vec::new(),
            regular_e2e: None,
            express_e2e: None,
            regular_queueing_delays: None,
            express_queueing_delays: None,
            pre_eviction_queueing_delays: None,
            post_eviction_queueing_delays: None,
            pct_shed_requests: None,
        },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.lb_policy.is_ms_only() {
        let name = match args.lb_policy {
            LoadBalancePolicyKind::Cl => "cl",
            LoadBalancePolicyKind::ClLr => "cl-lr",
            LoadBalancePolicyKind::Corr => "corr",
            _ => unreachable!(),
        };
        return Err(format!(
            "--lb-policy {name} is not supported by the lb simulator"
        )
        .into());
    }
    validate_pull_policy(args.lb_policy, args.pull_policy).map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
    validate_no_bind(args.lb_policy, args.no_bind).map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
    let slo = validate_slo(args.slo).map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
    let express_lane = validate_expresslane(&args)?;
    let work_shedding = validate_work_shedding(&args)?;
    let service_time = resolve_service_time(&args)?;
    let rates = compute_rates(&args, service_time.mean);
    rng::enter_run(args.seed);
    let stats = run_simulation(&args, &service_time, express_lane.as_ref(), work_shedding);
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
    use lb::subset::assign_subset;

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
        assert!(err.contains("only valid with bimodal"));
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
    fn validate_expresslane_rejects_approx_policy() {
        let err = validate_expresslane(
            &Args::try_parse_from([
                "lb",
                "--expresslane",
                "--express-size",
                "2",
                "--express-th",
                "5",
                "--lb-policy",
                "approx",
                "--pull-policy",
                "power-of-two",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("not supported with --lb-policy approx"));
    }

    #[test]
    fn validate_pull_policy_required_for_approx() {
        let args = Args::try_parse_from(["lb", "--lb-policy", "approx"]).unwrap();
        let err = validate_pull_policy(args.lb_policy, args.pull_policy).unwrap_err();
        assert!(err.contains("--pull-policy is required"));
    }

    #[test]
    fn validate_pull_policy_rejected_without_approx() {
        let args = Args::try_parse_from([
            "lb",
            "--lb-policy",
            "power-of-two",
            "--pull-policy",
            "least-request",
        ])
        .unwrap();
        let err = validate_pull_policy(args.lb_policy, args.pull_policy).unwrap_err();
        assert!(err.contains("--pull-policy is only valid"));
    }

    #[test]
    fn validate_no_bind_rejected_without_approx() {
        let args = Args::try_parse_from([
            "lb",
            "--lb-policy",
            "power-of-two",
            "--no-bind",
        ])
        .unwrap();
        let err = validate_no_bind(args.lb_policy, args.no_bind).unwrap_err();
        assert!(err.contains("--no-bind is only valid with --lb-policy approx"));
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
    fn validate_work_shedding_accepts_positive_threshold() {
        let threshold = validate_work_shedding(
            &Args::try_parse_from(["lb", "--shed-delay", "0.5"]).unwrap(),
        )
        .unwrap()
        .expect("expected work shedding config");
        assert_eq!(threshold, Duration::from_secs_f64(0.5));
    }

    #[test]
    fn validate_work_shedding_rejects_non_positive_threshold() {
        let err = validate_work_shedding(
            &Args::try_parse_from(["lb", "--shed-delay", "0"]).unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("--shed-delay must be positive"));
    }

    #[test]
    fn validate_work_shedding_rejects_centralized() {
        let err = validate_work_shedding(
            &Args::try_parse_from([
                "lb",
                "--shed-delay",
                "0.5",
                "--lb-policy",
                "centralized",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("not supported with --lb-policy centralized"));
    }

    #[test]
    fn validate_work_shedding_rejects_approx() {
        let err = validate_work_shedding(
            &Args::try_parse_from([
                "lb",
                "--shed-delay",
                "0.5",
                "--lb-policy",
                "approx",
                "--pull-policy",
                "power-of-two",
            ])
            .unwrap(),
        )
        .unwrap_err();
        assert!(err.contains("not supported with --lb-policy approx"));
    }

    #[test]
    fn validate_work_shedding_rejects_expresslane() {
        let err = validate_work_shedding(
            &Args::try_parse_from([
                "lb",
                "--shed-delay",
                "0.5",
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
        .unwrap_err();
        assert!(err.contains("cannot be combined with --expresslane"));
    }

    #[test]
    fn work_shedding_simulation_completes_tasks() {
        let args = Args::try_parse_from([
            "lb",
            "--shed-delay",
            "2.0",
            "--servers",
            "4",
            "--n",
            "200",
            "--load",
            "0.3",
            "--seed",
            "42",
        ])
        .unwrap();
        let work_shedding = validate_work_shedding(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, None, work_shedding).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert_eq!(stats.e2e.len(), 200);
        let pct = stats.pct_shed_requests.expect("expected shed metric");
        assert!((0.0..=100.0).contains(&pct));

        let output = run_output(Some(stats), &compute_rates(&args, service_time.mean), None);
        let json = serde_json::to_value(&output).unwrap();
        assert!(json.get("pct_shed_requests").is_some());
    }

    #[test]
    fn work_shedding_reports_pct_under_load() {
        let args = Args::try_parse_from([
            "lb",
            "--shed-delay",
            "2.0",
            "--servers",
            "3",
            "--concurrency",
            "1",
            "--n",
            "50",
            "--load",
            "0.85",
            "--seed",
            "42",
        ])
        .unwrap();
        let work_shedding = validate_work_shedding(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, None, work_shedding).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        let pct = stats.pct_shed_requests.expect("expected shed metric");
        assert!((0.0..=100.0).contains(&pct));
    }

    #[test]
    fn expresslane_subset_uses_regular_pool_only() {
        let n_regular = 8;
        let subset = assign_subset(SubsetPolicyKind::Deterministic, n_regular, 0, 0);
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
        let stats = run_simulation(&args, &service_time, express_lane.as_ref(), None).unwrap();
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
    fn constant_arrival_single_client_has_uniform_inter_arrival() {
        let args = Args::try_parse_from([
            "lb",
            "--arrival",
            "constant",
            "--clients",
            "1",
            "--n",
            "50",
            "--seed",
            "42",
        ])
        .unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        let capacity = args.servers.max(1) * args.concurrency.max(1);
        let arrival_mean = service_time.mean / (args.load * capacity as f32);
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref(), None).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert_eq!(stats.inter_arrival.len(), 49);
        for gap in &stats.inter_arrival {
            assert!(
                (gap - f64::from(arrival_mean)).abs() < 1e-6,
                "expected inter-arrival {arrival_mean}, got {gap}"
            );
        }
    }

    #[test]
    fn constant_arrival_multi_client_offsets_produce_uniform_global_spacing() {
        let args = Args::try_parse_from([
            "lb",
            "--arrival",
            "constant",
            "--clients",
            "3",
            "--n",
            "30",
            "--seed",
            "42",
        ])
        .unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        let capacity = args.servers.max(1) * args.concurrency.max(1);
        let arrival_mean = service_time.mean / (args.load * capacity as f32);
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref(), None).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert_eq!(stats.inter_arrival.len(), 29);
        for gap in &stats.inter_arrival {
            assert!(
                (gap - f64::from(arrival_mean)).abs() < 1e-6,
                "expected global inter-arrival {arrival_mean}, got {gap}"
            );
        }
    }

    #[test]
    fn inter_arrival_and_departure_lengths_match_task_count() {
        let args = Args::try_parse_from(["lb", "--n", "500", "--seed", "42"]).unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref(), None).unwrap();
        rng::exit_run();
        let stats = stats.expect("expected completed tasks");
        assert_eq!(stats.inter_arrival.len(), 499);
        assert_eq!(stats.inter_departure.len(), 499);
        assert_eq!(stats.e2e.len(), 500);
    }

    #[test]
    fn non_express_run_has_no_split_metrics() {
        let args = Args::try_parse_from(["lb", "--n", "100", "--seed", "42"]).unwrap();
        let express_lane = validate_expresslane(&args).unwrap();
        assert!(express_lane.is_none());
        let service_time = resolve_service_time(&args).unwrap();
        rng::enter_run(args.seed);
        let stats = run_simulation(&args, &service_time, express_lane.as_ref(), None).unwrap();
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
        assert!(stats.pct_shed_requests.is_none());

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
        assert!(json.get("pct_shed_requests").is_none());
    }
}
