use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn ms_prequal_completes_on_chain_topology() {
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
            "prequal",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms prequal run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"].as_object().expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));

    let utilization = stats["microservice_utilization_pct"]
        .as_object()
        .expect("microservice_utilization_pct");
    for (_, pct) in utilization {
        let pct = pct.as_f64().expect("utilization pct");
        assert!(pct > 0.0, "expected positive utilization");
        assert!(pct <= 100.0, "utilization should not exceed 100%");
    }
}

#[test]
fn ms_rejects_prequal_with_subsetting() {
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
            "prequal",
            "--lb-subset-size",
            "3",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--lb-subset-size is not supported with --lb-policy prequal"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn ms_rejects_pull_policy_with_prequal() {
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
            "prequal",
            "--pull-policy",
            "least-request",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--pull-policy is only valid with --lb-policy approx"),
        "unexpected stderr: {stderr}"
    );
}
