use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::str;

fn run_ms_trace(args: &[&str]) -> (String, String, i32) {
    let bin = env!("CARGO_BIN_EXE_ms");
    let output = Command::new(bin)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run ms");
    let stdout = str::from_utf8(&output.stdout).expect("stdout utf8").to_string();
    let stderr = str::from_utf8(&output.stderr).expect("stderr utf8").to_string();
    let code = output.status.code().unwrap_or(-1);
    (stdout, stderr, code)
}

fn assert_in_order(haystack: &str, needles: &[&str]) {
    let mut start = 0;
    for needle in needles {
        let pos = haystack[start..]
            .find(needle)
            .unwrap_or_else(|| panic!("missing trace line containing `{needle}`\n{haystack}"));
        start += pos + needle.len();
    }
}

#[test]
fn fanin_f1_request_flow_trace() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let callgraph = root.join("tests/fanin/single/callgraph.json");
    let load = root.join("tests/fanin/single/load.json");

    let (stdout, stderr, code) = run_ms_trace(&[
        "--callgraph",
        callgraph.to_str().unwrap(),
        "--load-file",
        load.to_str().unwrap(),
        "--n",
        "1",
        "--seed",
        "7",
        "--trace",
        "--trace-limit",
        "1",
        "--format",
        "json",
    ]);
    assert_eq!(code, 0, "ms failed: stderr={stderr}");
    assert!(stdout.contains("\"f1\""), "expected f1 stats on stdout");

    assert_in_order(
        &stderr,
        &[
            "UserArrival api=f1 entry=frontend:f1",
            "EdgeBalancer(api=f1)",
            "Server(frontend/0) serve start endpoint=frontend:f1",
            "ReplicaBalancer(frontend/0) outbound target=backend1",
            "Server(backend1/0) serve start endpoint=backend1:f2",
            "ReplicaBalancer(backend1/0) outbound target=shared",
            "Server(shared/0) serve start endpoint=shared:f5",
            "return -> frontend/0 resume=frontend:f1 sibling=1",
            "ReplicaBalancer(frontend/0) outbound target=backend2",
            "Server(backend2/0) serve start endpoint=backend2:f4",
            "ReplicaBalancer(backend2/0) outbound target=shared",
            "return -> frontend/0 resume=frontend:f1 sibling=2",
            "UserArrival complete api=f1",
        ],
    );
}
