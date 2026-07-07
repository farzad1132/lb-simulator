use lb::microservice::{MsArgs, OutputFormat, run};
use lb::policy::LoadBalancePolicyKind;
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;

fn caller_queue_args(seed: u64, n: u32) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/caller_queue/callgraph.json"),
        load_file: root.join("tests/caller_queue/load.json"),
        n,
        lb_policy: LoadBalancePolicyKind::LeastRequest,
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
    }
}

#[test]
fn g1_nested_call_completes_with_queueing() {
    let stats = run(&caller_queue_args(42, 500))
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
fn f1_nested_callgraph_completes() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let stats = run(&MsArgs {
        callgraph: root.join("tests/fanin/single/callgraph.json"),
        load_file: root.join("tests/fanin/single/load.json"),
        n: 200,
        lb_policy: LoadBalancePolicyKind::LeastRequest,
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
    })
    .unwrap()
    .expect("stats");
    assert_eq!(stats.by_api["f1"].e2e_ms.len(), 200);
}
