use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn lb_centralized_completes_with_sane_utilization() {
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
            "centralized",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "lb centralized run failed: {}",
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
fn lb_centralized_rejects_expresslane() {
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
            "centralized",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not supported with --lb-policy centralized"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_centralized_policy() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();
    let callgraph = root.join("tests/client_server/single_replica/callgraph.json");
    let load_file = root.join("tests/client_server/single_replica/load.json");

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            callgraph.to_str().unwrap(),
            "--load-file",
            load_file.to_str().unwrap(),
            "--n",
            "100",
            "--lb-policy",
            "centralized",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not supported by the ms simulator"),
        "unexpected stderr: {stderr}"
    );
}
