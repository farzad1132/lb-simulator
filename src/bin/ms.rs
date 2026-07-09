use clap::Parser;
use lb::microservice::{MsArgs, MsStats, OutputFormat, print_human_stats, run};
use lb::policy::LoadBalancePolicyKind;
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
    #[arg(long, value_enum, default_value_t = SchedulingPolicyKind::Fifo)]
    scheduling: SchedulingPolicyKind,
    #[arg(long)]
    force_fixed_svc: bool,
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 0)]
    verbose: u8,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Args::parse();
    let args = MsArgs {
        callgraph: cli.callgraph,
        load_file: cli.load_file,
        n: cli.n,
        lb_policy: cli.lb_policy,
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
        force_fixed_svc: cli.force_fixed_svc,
        verbose: cli.verbose,
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
                by_api: Default::default(),
                by_microservice: Default::default(),
                microservice_order: Default::default(),
                total_processing_p99_ms: 0.0,
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
        assert_eq!(cli.scale, 0);
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
    fn parses_force_fixed_svc() {
        let cli = Args::parse_from([
            "ms",
            "--callgraph",
            "tests/fanin/callgraph.json",
            "--load-file",
            "tests/fanin/load.json",
            "--force-fixed-svc",
        ]);
        assert!(cli.force_fixed_svc);
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
}
