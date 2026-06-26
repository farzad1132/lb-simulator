use clap::Parser;
use lb::microservice::{MsArgs, MsStats, OutputFormat, print_human_stats, run};
use lb::policy::LoadBalancePolicyKind;
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
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Human)]
    format: OutputFormat,
    #[arg(long)]
    trace: bool,
    #[arg(long, default_value_t = 5)]
    trace_limit: u32,
    #[arg(long, default_value_t = 0)]
    scale: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Args::parse();
    let args = MsArgs {
        callgraph: cli.callgraph,
        load_file: cli.load_file,
        n: cli.n,
        lb_policy: cli.lb_policy,
        lb_subset_size: cli.lb_subset_size,
        seed: cli.seed,
        format: cli.format,
        trace: cli.trace,
        trace_limit: cli.trace_limit,
        scale: cli.scale,
    };

    let stats = run(&args)?;

    match args.format {
        OutputFormat::Human => match stats {
            Some(stats) => print_human_stats(&stats),
            None => println!("no completed requests"),
        },
        OutputFormat::Json => {
            let output = stats.unwrap_or(MsStats {
                utilization_pct: Default::default(),
                replica_utilization_pct: Default::default(),
                by_api: Default::default(),
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
        assert_eq!(cli.scale, 0);
    }
}
