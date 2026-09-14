use clap::Parser;
use lb::microservice::{
    EqScaleSpec, MsArgs, MsServiceDistribution, MsStats, OutputFormat, collect_eq_scale,
    parse_eq_scale_spec, print_human_stats, run,
};
use lb::policy::{
    AmphiQueueSchedKind, CentralizedSchedKind, LoadBalancePolicyKind, PullPolicyKind,
    validate_amphiqueue_share, validate_centralized_sched, validate_jbsq_n,
    validate_prequal_subset,
};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::io::{self, Write};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    callgraph: PathBuf,
    #[arg(long)]
    load_file: PathBuf,
    #[arg(long, default_value_t = 1_000_000)]
    n: u32,
    #[arg(long, value_enum, default_value = "power-of-two")]
    lb_policy: LoadBalancePolicyKind,
    #[arg(long, value_enum)]
    pull_policy: Option<PullPolicyKind>,
    #[arg(long, default_value_t = 0)]
    lb_subset_size: u32,
    #[arg(long, value_enum, default_value_t = SubsetPolicyKind::Deterministic)]
    lb_subset_policy: SubsetPolicyKind,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    rps: Option<f64>,
    #[arg(long)]
    slo_ms: Option<f64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
    #[arg(long)]
    trace: bool,
    #[arg(long, default_value_t = 5)]
    trace_limit: u32,
    #[arg(long, default_value_t = 0)]
    scale: u32,
    #[arg(
        long,
        value_name = "SPEC",
        action = clap::ArgAction::Append,
        value_parser = parse_eq_scale_spec,
        help = "Add N cpu and N replicas (every service, or NAME=N for one tier) and stretch that tier's avg_rt by (cpu+N)/cpu so processing capacity stays equivalent"
    )]
    eq_scale: Vec<EqScaleSpec>,
    #[arg(long, value_enum, default_value_t = SchedulingPolicyKind::Fifo)]
    scheduling: SchedulingPolicyKind,
    #[arg(long, value_enum, default_value_t = CentralizedSchedKind::Fcfs)]
    centralized_sched: CentralizedSchedKind,
    #[arg(long, value_enum, default_value_t = MsServiceDistribution::Exp)]
    service_dist: MsServiceDistribution,
    #[arg(long, value_enum)]
    amphiqueue_sched: Option<AmphiQueueSchedKind>,
    #[arg(long, default_value_t = 1)]
    amphiqueue_share: u32,
    #[arg(long)]
    jbsq_n: Option<u32>,
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 0)]
    verbose: u8,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Args::parse();
    validate_prequal_subset(cli.lb_policy, cli.lb_subset_size)?;
    validate_amphiqueue_share(cli.lb_policy, cli.amphiqueue_share)?;
    validate_centralized_sched(cli.lb_policy, cli.centralized_sched)?;
    validate_jbsq_n(cli.lb_policy, cli.jbsq_n)?;
    let (eq_scale, eq_scale_overrides) = collect_eq_scale(&cli.eq_scale)?;
    let args = MsArgs {
        callgraph: cli.callgraph,
        load_file: cli.load_file,
        n: cli.n,
        lb_policy: cli.lb_policy,
        pull_policy: cli.pull_policy,
        lb_subset_size: cli.lb_subset_size,
        lb_subset_policy: cli.lb_subset_policy,
        seed: cli.seed,
        rps: cli.rps,
        slo_ms: cli.slo_ms,
        format: cli.format,
        trace: cli.trace,
        trace_limit: cli.trace_limit,
        scale: cli.scale,
        scheduling: cli.scheduling,
        centralized_sched: cli.centralized_sched,
        service_dist: cli.service_dist,
        verbose: cli.verbose,
        pull_audit: None,
        centralized_audit: None,
        jbsq_audit: None,
        amphiqueue_sched: cli.amphiqueue_sched,
        amphiqueue_share: cli.amphiqueue_share,
        jbsq_n: cli.jbsq_n,
        eq_scale,
        eq_scale_overrides,
    };

    let stats = run(&args)?;

    match args.format {
        OutputFormat::Human => match stats {
            Some(stats) => print_human_stats(&stats),
            None => println!("no completed requests"),
        },
        OutputFormat::Json => {
            let output = stats.unwrap_or(MsStats {
                microservice_utilization_pct: Default::default(),
                server_utilization_pct: Default::default(),
                server_avg_queue_inflight: Default::default(),
                server_avg_queue: Default::default(),
                by_api: Default::default(),
                by_microservice: Default::default(),
                microservice_order: Default::default(),
                total_processing_p99_ms: 0.0,
                per_request_cumulative_queueing_ms: Default::default(),
            });
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
    use clap::Parser;

    #[test]
    fn default_lb_policy_is_power_of_two() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::PowerOfTwo);
        assert_eq!(cli.lb_subset_policy, SubsetPolicyKind::Deterministic);
        assert_eq!(cli.scheduling, SchedulingPolicyKind::Fifo);
        assert_eq!(cli.centralized_sched, CentralizedSchedKind::Fcfs);
        assert_eq!(cli.scale, 0);
        assert!(cli.eq_scale.is_empty());
        assert_eq!(cli.rps, None);
        assert_eq!(cli.slo_ms, None);
    }

    #[test]
    fn parses_load_overrides() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--rps",
            "2500",
            "--slo-ms",
            "12.5",
        ]);
        assert_eq!(cli.rps, Some(2500.0));
        assert_eq!(cli.slo_ms, Some(12.5));
    }

    #[test]
    fn verbose_defaults_to_zero() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
        ]);
        assert_eq!(cli.verbose, 0);
    }

    #[test]
    fn parses_amphiqueue_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::AmphiQueue);
        assert_eq!(cli.pull_policy, Some(PullPolicyKind::LeastRequest));
    }

    #[test]
    fn parses_cl_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "cl",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::Cl);
    }

    #[test]
    fn parses_cl_lr_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "cl-lr",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::ClLr);
    }

    #[test]
    fn parses_cl_r_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "cl-r",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::ClR);
    }

    #[test]
    fn parses_cl_rr_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "cl-rr",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::ClRr);
    }

    #[test]
    fn parses_corr_lb_policy() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "corr",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::Corr);
    }

    #[test]
    fn parses_jbsq_lb_policy_and_n() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "jbsq",
            "--jbsq-n",
            "3",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::Jbsq);
        assert_eq!(cli.jbsq_n, Some(3));
    }

    #[test]
    fn parses_scheduling_edf() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--scheduling",
            "edf",
        ]);
        assert_eq!(cli.scheduling, SchedulingPolicyKind::Edf);
    }

    #[test]
    fn parses_centralized_sched_edf() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "centralized",
            "--centralized-sched",
            "edf",
        ]);
        assert_eq!(cli.centralized_sched, CentralizedSchedKind::Edf);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::Centralized);
    }

    #[test]
    fn rejects_centralized_sched_edf_without_centralized() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--centralized-sched",
            "edf",
        ]);
        let err = validate_centralized_sched(cli.lb_policy, cli.centralized_sched).unwrap_err();
        assert!(err.contains("only valid with --lb-policy centralized"));
    }

    #[test]
    fn default_service_dist_is_exp() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
        ]);
        assert_eq!(cli.service_dist, MsServiceDistribution::Exp);
    }

    #[test]
    fn parses_service_dist_fixed() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--service-dist",
            "fixed",
        ]);
        assert_eq!(cli.service_dist, MsServiceDistribution::Fixed);
    }

    #[test]
    fn parses_service_dist_bimodal() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--service-dist",
            "bimodal",
        ]);
        assert_eq!(cli.service_dist, MsServiceDistribution::Bimodal);
    }

    #[test]
    fn parses_amphiqueue_sched_edf() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf",
        ]);
        assert_eq!(cli.amphiqueue_sched, Some(AmphiQueueSchedKind::Edf));
    }

    #[test]
    fn parses_amphiqueue_sched_edf_plus() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf+",
        ]);
        assert_eq!(cli.amphiqueue_sched, Some(AmphiQueueSchedKind::EdfPlus));
    }

    #[test]
    fn parses_amphiqueue_sched_fcfs() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "fcfs",
        ]);
        assert_eq!(cli.amphiqueue_sched, Some(AmphiQueueSchedKind::Fcfs));
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::AmphiQueue);
    }

    #[test]
    fn parses_amphiqueue_share_defaults_and_flags() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--lb-policy",
            "amphiqueue-share",
            "--pull-policy",
            "power-of-two",
            "--amphiqueue-share",
            "4",
            "--amphiqueue-sched",
            "edf",
        ]);
        assert_eq!(cli.lb_policy, LoadBalancePolicyKind::AmphiQueueShare);
        assert_eq!(cli.amphiqueue_share, 4);
        assert_eq!(cli.pull_policy, Some(PullPolicyKind::PowerOfTwo));
        assert_eq!(cli.amphiqueue_sched, Some(AmphiQueueSchedKind::Edf));
        assert_eq!(
            Args::parse_from([
                "ms",
                "--callgraph",
                "tests/fanin/callgraph.json",
                "--load-file",
                "tests/fanin/load.json",
            ])
            .amphiqueue_share,
            1
        );
    }

    #[test]
    fn verbose_count_flag() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "-v",
        ]);
        assert_eq!(cli.verbose, 1);
    }

    #[test]
    fn parses_eq_scale_all() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--eq-scale",
            "10",
        ]);
        assert_eq!(cli.eq_scale, vec![EqScaleSpec::All(10)]);
    }

    #[test]
    fn parses_eq_scale_named() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--eq-scale",
            "backend2=10",
        ]);
        assert_eq!(
            cli.eq_scale,
            vec![EqScaleSpec::Named("backend2".into(), 10)]
        );
    }

    #[test]
    fn parses_eq_scale_all_and_named() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--eq-scale",
            "10",
            "--eq-scale",
            "backend2=10",
        ]);
        assert_eq!(
            cli.eq_scale,
            vec![
                EqScaleSpec::All(10),
                EqScaleSpec::Named("backend2".into(), 10)
            ]
        );
    }
}
