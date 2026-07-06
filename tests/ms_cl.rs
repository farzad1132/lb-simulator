use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn ms_cl_completes_on_chain_topology() {
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
            "cl",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms cl run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"].as_object().expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_cl_completes_on_fanin_topology() {
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
            "cl",
            "--seed",
            "7",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms cl fanin run failed: {}",
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
fn lb_rejects_cl_policy() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "100",
            "--servers",
            "4",
            "--lb-policy",
            "cl",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not supported by the lb simulator"),
        "unexpected stderr: {stderr}"
    );
}
