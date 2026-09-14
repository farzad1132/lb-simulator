use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn ms_amphiqueue_completes_on_chain_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "1000",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms amphiqueue run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"]
        .as_object()
        .expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_amphiqueue_requires_pull_policy() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "amphiqueue",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is required with --lb-policy amphiqueue"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_pull_policy_without_amphiqueue() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "power-of-two",
            "--pull-policy",
            "least-request",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is only valid with --lb-policy amphiqueue"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_amphiqueue_no_bind_completes_on_chain_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "1000",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "fcfs",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms amphiqueue no-bind run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"]
        .as_object()
        .expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_rejects_amphiqueue_sched_without_amphiqueue() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "power-of-two",
            "--amphiqueue-sched",
            "fcfs",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-sched is only valid with --lb-policy amphiqueue"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_amphiqueue_no_bind_edf_completes_on_chain_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "1000",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms amphiqueue edf run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"]
        .as_object()
        .expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_amphiqueue_no_bind_edf_plus_completes_on_chain_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "1000",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf+",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms amphiqueue edf+ run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"]
        .as_object()
        .expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_rejects_amphiqueue_sched_edf_on_lb_binary() {
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
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-sched edf/edf+ is only supported by the ms simulator"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_amphiqueue_sched_edf_plus_on_lb_binary() {
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
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "edf+",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-sched edf/edf+ is only supported by the ms simulator"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_amphiqueue_sched_edf_without_amphiqueue() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "power-of-two",
            "--amphiqueue-sched",
            "edf",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-sched is only valid with --lb-policy amphiqueue"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_amphiqueue_share_completes_on_chain_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "500",
            "--lb-policy",
            "amphiqueue-share",
            "--amphiqueue-share",
            "3",
            "--pull-policy",
            "least-request",
            "--amphiqueue-sched",
            "fcfs",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms amphiqueue-share run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    assert_eq!(
        stats["by_api"]["handle"]["e2e_ms"]
            .as_array()
            .map(|a| a.len()),
        Some(500)
    );
}

#[test]
fn ms_amphiqueue_share_requires_pull_policy() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "amphiqueue-share",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is required"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_amphiqueue_share_flag_without_policy() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "amphiqueue",
            "--pull-policy",
            "least-request",
            "--amphiqueue-share",
            "2",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-share is only valid with --lb-policy amphiqueue-share"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_amphiqueue_share_zero() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "100",
            "--lb-policy",
            "amphiqueue-share",
            "--amphiqueue-share",
            "0",
            "--pull-policy",
            "least-request",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--amphiqueue-share must be >= 1"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn lb_rejects_amphiqueue_share_policy() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--lb-policy",
            "amphiqueue-share",
            "--pull-policy",
            "least-request",
            "--servers",
            "2",
            "--clients",
            "2",
            "--n",
            "10",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("amphiqueue-share is not supported by the lb simulator"),
        "unexpected stderr: {stderr}"
    );
}
