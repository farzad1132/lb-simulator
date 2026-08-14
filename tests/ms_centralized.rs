use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn ms_centralized_completes_on_chain_topology() {
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
            "centralized",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms centralized run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"].as_object().expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_centralized_completes_on_fanin_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/fanin/multi/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/fanin/multi/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "500",
            "--lb-policy",
            "centralized",
            "--seed",
            "7",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms centralized fanin run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let utilization = stats["microservice_utilization_pct"]
        .as_object()
        .expect("microservice_utilization_pct");
    for (_, pct) in utilization {
        let pct = pct.as_f64().expect("utilization pct");
        assert!(pct >= 0.0);
        assert!(pct <= 100.0);
    }
}

#[test]
fn ms_centralized_subset_smoke_completes() {
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
            "centralized",
            "--lb-subset-size",
            "5",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms centralized subset run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"].as_object().expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(500));
}

#[test]
fn ms_centralized_rejects_non_divisor_subset_size() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--n",
            "100",
            "--lb-policy",
            "centralized",
            "--lb-subset-size",
            "3",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must evenly divide") || stderr.contains("centralized subsetting"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_centralized_rejects_random_subset_policy() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--n",
            "100",
            "--lb-policy",
            "centralized",
            "--lb-subset-size",
            "5",
            "--lb-subset-policy",
            "random",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("random is not supported") || stderr.contains("deterministic"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_centralized_rejects_caller_not_divisible_by_subset_count() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    // fanin/multi: shared has 4 replicas; k=2 → S=2; backend1 has 3 replicas → 3 % 2 != 0
    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/fanin/multi/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/fanin/multi/load.json").to_str().unwrap(),
            "--n",
            "100",
            "--lb-policy",
            "centralized",
            "--lb-subset-size",
            "2",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("must be divisible by the subset count")
            || stderr.contains("must evenly divide")
            || stderr.contains("centralized subsetting")
            || stderr.contains("central pull-queue subsetting"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_cl_rejects_subset_size() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/chain/3/callgraph.json").to_str().unwrap(),
            "--load-file",
            root.join("tests/chain/3/load.json").to_str().unwrap(),
            "--n",
            "100",
            "--lb-policy",
            "cl",
            "--lb-subset-size",
            "3",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not supported with --lb-policy cl, cl-lr, cl-r, cl-rr, or corr"),
        "unexpected stderr: {stderr}"
    );
}
