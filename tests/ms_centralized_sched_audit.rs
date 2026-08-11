use lb::microservice::{MsArgs, MsCentralizedAudit, MsServiceDistribution, OutputFormat, run};
use lb::policy::{CentralizedSchedKind, LoadBalancePolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;
use std::sync::Arc;

fn chain3_centralized_args(
    n: u32,
    seed: u64,
    centralized_sched: CentralizedSchedKind,
    audit: Option<Arc<MsCentralizedAudit>>,
) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n,
        lb_policy: LoadBalancePolicyKind::Centralized,
        pull_policy: None,
        lb_subset_size: 0,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        seed: Some(seed),
        rps: None,
        slo_ms: None,
        format: OutputFormat::Json,
        trace: false,
        trace_limit: 0,
        scale: 0,
        verbose: 0,
        scheduling: SchedulingPolicyKind::Fifo,
        centralized_sched,
        service_dist: MsServiceDistribution::Exp,
        pull_audit: None,
        centralized_audit: audit,
        jbsq_audit: None,
        approx_sched: None,
        approx_share: 1,
        jbsq_n: None,
    }
}

#[test]
fn ms_centralized_edf_trace_invariants() {
    let audit = MsCentralizedAudit::new();
    let stats = run(&chain3_centralized_args(
        800,
        99,
        CentralizedSchedKind::Edf,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 800);

    for target in ["backend1", "backend2"] {
        audit
            .validate_centralized_edf(target)
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
}

#[test]
fn ms_centralized_fcfs_trace_invariants() {
    let audit = MsCentralizedAudit::new();
    let stats = run(&chain3_centralized_args(
        800,
        99,
        CentralizedSchedKind::Fcfs,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 800);

    for target in ["backend1", "backend2"] {
        audit
            .validate_centralized_fcfs(target)
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
}

#[test]
fn ms_centralized_edf_differs_from_fcfs() {
    let fcfs_audit = MsCentralizedAudit::new();
    run(&chain3_centralized_args(
        800,
        99,
        CentralizedSchedKind::Fcfs,
        Some(fcfs_audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    fcfs_audit
        .validate_centralized_fcfs("backend1")
        .expect("fcfs invariants");

    let edf_audit = MsCentralizedAudit::new();
    run(&chain3_centralized_args(
        800,
        99,
        CentralizedSchedKind::Edf,
        Some(edf_audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    edf_audit
        .validate_centralized_edf("backend1")
        .expect("edf invariants");

    let fcfs_order = fcfs_audit.dispatch_request_ids("backend1");
    let edf_order = edf_audit.dispatch_request_ids("backend1");
    assert_eq!(fcfs_order.len(), edf_order.len());
    assert!(
        fcfs_order != edf_order,
        "expected EDF to change at least one centralized dispatch order under backlog"
    );
}
