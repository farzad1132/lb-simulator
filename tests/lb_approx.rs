use std::env;
use std::process::Command;

#[test]
fn lb_approx_completes_with_sane_utilization() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "1000",
            "--servers",
            "2",
            "--clients",
            "2",
            "--lb-policy",
            "approx",
            "--pull-policy",
            "least-request",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "lb approx run failed: {}",
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
fn lb_approx_requires_pull_policy() {
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
            "approx",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is required with --lb-policy approx"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn lb_rejects_pull_policy_without_approx() {
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
            "power-of-two",
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
fn lb_approx_rejects_expresslane() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "100",
            "--servers",
            "4",
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
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not supported with --lb-policy approx"),
        "unexpected stderr: {stderr}"
    );
}
