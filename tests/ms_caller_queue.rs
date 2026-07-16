use lb::microservice::{MsArgs, OutputFormat, run};
use lb::policy::{LoadBalancePolicyKind, PullPolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;

fn caller_queue_args(seed: u64, n: u32, lb_policy: LoadBalancePolicyKind) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let pull_policy = if lb_policy.is_approx() {
        Some(PullPolicyKind::LeastRequest)
    } else {
        None
    };
    MsArgs {
        callgraph: root.join("tests/caller_queue/callgraph.json"),
        load_file: root.join("tests/caller_queue/load.json"),
        n,
        lb_policy,
        pull_policy,
        lb_subset_size: 0,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        seed: Some(seed),
        rps: None,
        slo_ms: None,
        format: OutputFormat::Json,
        trace: false,
        trace_limit: 5,
        scale: 0,
        verbose: 0,
        scheduling: SchedulingPolicyKind::Fifo,
        force_fixed_svc: false,
        pull_audit: None,
        no_bind: false,
        approx_sched: SchedulingPolicyKind::Fifo,
    }
}

fn queueing_p50(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

#[test]
fn g1_nested_call_completes_with_queueing() {
    let stats = run(&caller_queue_args(42, 500, LoadBalancePolicyKind::LeastRequest))
        .unwrap()
        .expect("stats");
    let api = &stats.by_api["g1"];
    assert_eq!(api.e2e_ms.len(), 500);

    let mut e2e = api.e2e_ms.clone();
    e2e.sort_by(f64::total_cmp);
    let mut processing = api.processing_time_ms.clone();
    processing.sort_by(f64::total_cmp);

    let e2e_p50 = e2e[e2e.len() / 2];
    let proc_p50 = processing[processing.len() / 2];
    assert!(
        e2e_p50 > proc_p50,
        "e2e p50 ({e2e_p50}) should exceed processing p50 ({proc_p50}) when caller queueing applies"
    );
}

#[test]
fn approx_caller_queueing_excludes_downstream_blocking() {
    let stats = run(&caller_queue_args(42, 500, LoadBalancePolicyKind::Approx))
        .unwrap()
        .expect("stats");

    let frontend = &stats.by_microservice["frontend"];
    let backend = &stats.by_microservice["backend1"];
    assert_eq!(frontend.queueing_delay_ms.len(), 500);
    assert_eq!(backend.queueing_delay_ms.len(), 500);

    for i in 0..500 {
        let backend_reconstructed =
            backend.queueing_delay_ms[i] + backend.processing_time_ms[i];
        assert!(
            (backend_reconstructed - backend.response_time_ms[i]).abs() < 1e-3,
            "visit {i}: leaf queueing+proc should equal response"
        );
    }

    let mut frontend_own: Vec<f64> = frontend
        .queueing_delay_ms
        .iter()
        .zip(frontend.processing_time_ms.iter())
        .map(|(q, p)| q + p)
        .collect();
    frontend_own.sort_by(f64::total_cmp);
    let mut frontend_rt = frontend.response_time_ms.clone();
    frontend_rt.sort_by(f64::total_cmp);
    assert!(
        frontend_own[frontend_own.len() / 2] < frontend_rt[frontend_rt.len() / 2] - 1.0,
        "frontend own time p50 should be well below total response when downstream is slow"
    );

    assert!(
        queueing_p50(&frontend.queueing_delay_ms) > 0.0,
        "approx frontend queueing p50 should be positive"
    );
}

#[test]
fn approx_caller_lb_queue_increases_server_avg_occupancy() {
    let stats = run(&caller_queue_args(42, 500, LoadBalancePolicyKind::Approx))
        .unwrap()
        .expect("stats");
    let lr_stats = run(&caller_queue_args(42, 500, LoadBalancePolicyKind::LeastRequest))
        .unwrap()
        .expect("stats");

    let approx_occ = stats.server_avg_queue_inflight["frontend"][&0];
    let lr_occ = lr_stats.server_avg_queue_inflight["frontend"][&0];
    assert!(
        approx_occ > lr_occ,
        "approx caller LB queue should increase frontend avg occupancy (approx={approx_occ}, lr={lr_occ})"
    );
}

#[test]
fn f1_nested_callgraph_completes() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stats = run(&MsArgs {
        callgraph: root.join("tests/fanin/single/callgraph.json"),
        load_file: root.join("tests/fanin/single/load.json"),
        n: 200,
        lb_policy: LoadBalancePolicyKind::LeastRequest,
        pull_policy: None,
        lb_subset_size: 0,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        seed: Some(7),
        rps: None,
        slo_ms: None,
        format: OutputFormat::Json,
        trace: false,
        trace_limit: 5,
        scale: 0,
        verbose: 0,
        scheduling: SchedulingPolicyKind::Fifo,
        force_fixed_svc: false,
        pull_audit: None,
        no_bind: false,
        approx_sched: SchedulingPolicyKind::Fifo,
    })
    .unwrap()
    .expect("stats");
    assert_eq!(stats.by_api["f1"].e2e_ms.len(), 200);
}
