use std::env;
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn parse_subset_len(line: &str) -> Option<usize> {
    let (_, rest) = line.split_once("subset: [")?;
    let (inner, _) = rest.split_once(']')?;
    let inner = inner.trim();
    if inner.is_empty() {
        return Some(0);
    }
    Some(inner.split(',').count())
}

#[test]
fn ms_edge_ignores_subset_size_for_power_of_two() {
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
            "power-of-two",
            "--lb-subset-size",
            "5",
            "--seed",
            "42",
            "-v",
        ])
        .output()
        .expect("failed to spawn ms");

    assert!(
        output.status.success(),
        "ms p2c subset run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut edge_len = None;
    let mut outbound_len = None;
    for line in stderr.lines() {
        if line.starts_with("api ") && line.contains("subset:") {
            edge_len = parse_subset_len(line);
        } else if line.starts_with("server ") && line.contains("subset:") && outbound_len.is_none()
        {
            outbound_len = parse_subset_len(line);
        }
    }

    assert_eq!(
        edge_len,
        Some(10),
        "EdgeBalancer should use full entry pool (10 replicas); stderr:\n{stderr}"
    );
    assert_eq!(
        outbound_len,
        Some(5),
        "ReplicaBalancer outbound should honor k=5; stderr:\n{stderr}"
    );
}
