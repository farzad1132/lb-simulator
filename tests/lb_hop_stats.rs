use std::env;
use std::process::Command;

fn mean(values: &[f64]) -> f64 {
    assert!(!values.is_empty());
    values.iter().sum::<f64>() / values.len() as f64
}

fn f64_array(stats: &serde_json::Value, path: &[&str]) -> Vec<f64> {
    let mut cur = stats;
    for key in path {
        cur = &cur[*key];
    }
    cur.as_array()
        .unwrap_or_else(|| panic!("missing array at {}", path.join(".")))
        .iter()
        .map(|v| v.as_f64().expect("array element not f64"))
        .collect()
}

#[test]
fn push_queueing_is_mostly_on_server_hop() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "5000",
            "--servers",
            "1",
            "--clients",
            "1",
            "--concurrency",
            "1",
            "--load",
            "0.95",
            "--service-dist",
            "exponential",
            "--arrival",
            "exponential",
            "--lb-policy",
            "power-of-two",
            "--seed",
            "7",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "push hop run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stats: serde_json::Value =
        serde_json::from_str(&String::from_utf8(output.stdout).unwrap()).unwrap();

    assert_eq!(
        stats["hop_order"],
        serde_json::json!(["client", "server"])
    );

    let client_q = f64_array(&stats, &["by_hop", "client", "queueing_delay"]);
    let server_q = f64_array(&stats, &["by_hop", "server", "queueing_delay"]);
    let cum_server = f64_array(&stats, &["by_hop", "server", "cumulative_queueing_delay"]);

    assert!(mean(&client_q) < 1e-9, "push client queueing should be ~0");
    assert!(
        mean(&server_q) > 0.01,
        "push server queueing should be positive under load, got {}",
        mean(&server_q)
    );

    for (i, (&c, &s)) in client_q.iter().zip(server_q.iter()).enumerate() {
        assert!(
            (cum_server[i] - (c + s)).abs() < 1e-9,
            "cumulative mismatch at {i}"
        );
    }

    let util = stats["server_utilization_pct"]["server"]["0"]
        .as_f64()
        .expect("server util missing");
    assert!(util > 0.0 && util <= 105.0, "util={util}");

    let occ = stats["server_avg_queue_inflight"]["server"]["0"]
        .as_f64()
        .expect("server occupancy missing");
    assert!(occ >= 0.0, "occupancy={occ}");
}

#[test]
fn approx_queueing_is_mostly_on_client_hop() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "5000",
            "--servers",
            "1",
            "--clients",
            "1",
            "--concurrency",
            "1",
            "--load",
            "0.95",
            "--service-dist",
            "exponential",
            "--arrival",
            "exponential",
            "--lb-policy",
            "approx",
            "--pull-policy",
            "power-of-two",
            "--seed",
            "7",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "approx hop run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stats: serde_json::Value =
        serde_json::from_str(&String::from_utf8(output.stdout).unwrap()).unwrap();

    let client_q = f64_array(&stats, &["by_hop", "client", "queueing_delay"]);
    let server_q = f64_array(&stats, &["by_hop", "server", "queueing_delay"]);

    assert!(mean(&server_q) < 1e-9, "approx server queueing should be ~0");
    assert!(
        mean(&client_q) > 0.01,
        "approx client queueing should be positive under load, got {}",
        mean(&client_q)
    );

    let client_occ = stats["server_avg_queue_inflight"]["client"]["0"]
        .as_f64()
        .expect("client occupancy missing");
    assert!(client_occ >= 0.0, "client occupancy={client_occ}");
}

#[test]
fn centralized_queueing_is_mostly_on_client_hop() {
    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");

    let output = Command::new(&lb_binary)
        .args([
            "--format",
            "json",
            "--n",
            "5000",
            "--servers",
            "1",
            "--clients",
            "2",
            "--concurrency",
            "1",
            "--load",
            "0.95",
            "--service-dist",
            "exponential",
            "--arrival",
            "exponential",
            "--lb-policy",
            "centralized",
            "--seed",
            "7",
        ])
        .output()
        .expect("failed to spawn lb");

    assert!(
        output.status.success(),
        "centralized hop run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stats: serde_json::Value =
        serde_json::from_str(&String::from_utf8(output.stdout).unwrap()).unwrap();

    let client_q = f64_array(&stats, &["by_hop", "client", "queueing_delay"]);
    let server_q = f64_array(&stats, &["by_hop", "server", "queueing_delay"]);

    assert!(mean(&server_q) < 1e-9, "centralized server queueing should be ~0");
    assert!(
        mean(&client_q) > 0.01,
        "centralized client queueing should be positive under load, got {}",
        mean(&client_q)
    );
}
