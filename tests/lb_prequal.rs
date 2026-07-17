use std::env;
use std::process::Command;

#[test]
fn lb_prequal_completes_with_sane_utilization() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "1000",
            "--servers",
            "10",
            "--clients",
            "2",
            "--lb-policy",
            "prequal",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "lb prequal run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let utilization = stats["utilization_pct"]
        .as_f64()
        .expect("utilization_pct missing");
    assert!(utilization > 0.0, "expected positive utilization");
    assert!(utilization <= 100.0, "utilization should not exceed 100%");
    assert_eq!(stats["e2e"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn lb_rejects_prequal_with_subsetting() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "100",
            "--servers",
            "10",
            "--lb-policy",
            "prequal",
            "--lb-subset-size",
            "3",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--lb-subset-size is not supported with --lb-policy prequal"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn lb_rejects_pull_policy_with_prequal() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "100",
            "--servers",
            "2",
            "--lb-policy",
            "prequal",
            "--pull-policy",
            "least-request",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is only valid with --lb-policy approx"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_prequal() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");

    let output = Command::new(&ms_binary)
        .args([
            "--format",
            "json",
            "--n",
            "10",
            "--callgraph",
            "tests/chain/3/callgraph.json",
            "--load-file",
            "tests/chain/3/load.json",
            "--lb-policy",
            "prequal",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("prequal is not supported by the ms simulator"),
        "unexpected stderr: {stderr}"
    );
}
