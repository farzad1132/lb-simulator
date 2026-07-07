use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn ms_edf_completes_on_chain_topology() {
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
            "--scheduling",
            "edf",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms edf run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["handle"].as_object().expect("by_api.handle");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(1000));
}

#[test]
fn ms_edf_completes_on_caller_queue_topology() {
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");
    let root = repo_root();

    let output = Command::new(&ms_binary)
        .args([
            "--callgraph",
            root.join("tests/caller_queue/callgraph.json")
                .to_str()
                .unwrap(),
            "--load-file",
            root.join("tests/caller_queue/load.json").to_str().unwrap(),
            "--format",
            "json",
            "--n",
            "500",
            "--scheduling",
            "edf",
            "--seed",
            "42",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms edf caller_queue run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).expect("stdout not utf-8");
    let stats: serde_json::Value = serde_json::from_str(&stdout).expect("invalid json output");
    let by_api = stats["by_api"]["g1"].as_object().expect("by_api.g1");
    assert_eq!(by_api["e2e_ms"].as_array().map(|a| a.len()), Some(500));
}
