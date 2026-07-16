use std::env;
use std::process::Command;

const BASE_ARGS: &[&str] = &[
    "--format",
    "json",
    "--clients",
    "1",
    "--servers",
    "1",
    "--concurrency",
    "1",
    "--load",
    "2.0",
    "--n",
    "500",
    "--seed",
    "42",
    "--arrival",
    "constant",
    "--service-dist",
    "constant",
];

struct PolicyCase {
    name: &'static str,
    extra_args: &'static [&'static str],
}

const POLICIES: &[PolicyCase] = &[
    PolicyCase {
        name: "random",
        extra_args: &["--lb-policy", "random"],
    },
    PolicyCase {
        name: "power-of-two",
        extra_args: &["--lb-policy", "power-of-two"],
    },
    PolicyCase {
        name: "round-robin",
        extra_args: &["--lb-policy", "round-robin"],
    },
    PolicyCase {
        name: "least-request",
        extra_args: &["--lb-policy", "least-request"],
    },
    PolicyCase {
        name: "centralized",
        extra_args: &["--lb-policy", "centralized"],
    },
    PolicyCase {
        name: "approx",
        extra_args: &["--lb-policy", "approx", "--pull-policy", "random"],
    },
];

fn run_lb_json(lb_binary: &str, extra_args: &[&str]) -> serde_json::Value {
    let mut args: Vec<&str> = BASE_ARGS.to_vec();
    args.extend(extra_args);

    let output = Command::new(lb_binary)
        .args(&args)
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "lb run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    serde_json::from_str(&stdout).expect("invalid json output")
}

fn percentile(values: &mut [f64], pct: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    let idx = ((values.len() - 1) as f64 * pct / 100.0).round() as usize;
    values[idx]
}

struct PolicyStats {
    name: &'static str,
    utilization_pct: f64,
    p50: f64,
    p99: f64,
}

fn collect_stats(name: &'static str, stats: &serde_json::Value) -> PolicyStats {
    let utilization_pct = stats["utilization_pct"]
        .as_f64()
        .expect("utilization_pct missing");
    let e2e = stats["e2e"]
        .as_array()
        .expect("e2e array missing");
    assert_eq!(e2e.len(), 500, "{name}: expected 500 completed tasks");

    let mut samples: Vec<f64> = e2e.iter().map(|v| v.as_f64().expect("e2e value")).collect();
    PolicyStats {
        name,
        utilization_pct,
        p50: percentile(&mut samples.clone(), 50.0),
        p99: percentile(&mut samples, 99.0),
    }
}

#[test]
fn lb_all_policies_similar_with_single_server() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let results: Vec<PolicyStats> = POLICIES
        .iter()
        .map(|policy| {
            let stats = run_lb_json(&lb_binary, policy.extra_args);
            collect_stats(policy.name, &stats)
        })
        .collect();

    for stats in &results {
        assert!(
            stats.utilization_pct > 0.0,
            "{}: expected positive utilization",
            stats.name
        );
        assert!(
            stats.utilization_pct <= 100.0,
            "{}: utilization {:.2}% exceeds 100%",
            stats.name,
            stats.utilization_pct
        );
    }

    let min_p99 = results.iter().map(|s| s.p99).fold(f64::INFINITY, f64::min);
    let max_p99 = results.iter().map(|s| s.p99).fold(f64::NEG_INFINITY, f64::max);
    assert!(
        max_p99 / min_p99 < 1.15,
        "p99 e2e spread too large across policies (min={min_p99:.4}, max={max_p99:.4}): {:?}",
        results
            .iter()
            .map(|s| (s.name, s.p99))
            .collect::<Vec<_>>()
    );

    let min_p50 = results.iter().map(|s| s.p50).fold(f64::INFINITY, f64::min);
    let max_p50 = results.iter().map(|s| s.p50).fold(f64::NEG_INFINITY, f64::max);
    assert!(
        max_p50 / min_p50 < 1.05,
        "p50 e2e spread too large across policies (min={min_p50:.4}, max={max_p50:.4}): {:?}",
        results
            .iter()
            .map(|s| (s.name, s.p50))
            .collect::<Vec<_>>()
    );
}
