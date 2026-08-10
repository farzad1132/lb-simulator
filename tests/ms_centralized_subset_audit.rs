use lb::microservice::{MsArgs, MsCentralizedAudit, MsServiceDistribution, OutputFormat, run};
use lb::policy::{CentralizedSchedKind, LoadBalancePolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;
use std::sync::Arc;

fn chain3_centralized_args(
    n: u32,
    seed: u64,
    subset_size: u32,
    audit: Option<Arc<MsCentralizedAudit>>,
) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n,
        lb_policy: LoadBalancePolicyKind::Centralized,
        pull_policy: None,
        lb_subset_size: subset_size,
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
        centralized_sched: CentralizedSchedKind::Fcfs,
        service_dist: MsServiceDistribution::Exp,
        pull_audit: None,
        centralized_audit: audit,
        approx_sched: None,
        approx_share: 1,
    }
}

#[test]
fn ms_centralized_subset_disjoint_trace() {
    let audit = MsCentralizedAudit::new();
    // chain/3: each service has 10 replicas; k=5 → S=2 disjoint subsets
    let stats = run(&chain3_centralized_args(400, 42, 5, Some(audit.clone())))
        .unwrap()
        .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 400);

    for target in ["backend1", "backend2"] {
        audit
            .validate_disjoint_subsets(target, 10, 5)
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
}

#[test]
fn ms_centralized_per_subset_shared_lb_trace() {
    let audit = MsCentralizedAudit::new();
    let stats = run(&chain3_centralized_args(400, 7, 5, Some(audit.clone())))
        .unwrap()
        .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 400);

    for target in ["backend1", "backend2"] {
        audit
            .validate_centralized_per_subset(target, 10, 5)
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
}

#[test]
fn ms_centralized_caller_maps_to_subset_lb_trace() {
    let audit = MsCentralizedAudit::new();
    let stats = run(&chain3_centralized_args(400, 99, 5, Some(audit.clone())))
        .unwrap()
        .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 400);

    for target in ["backend1", "backend2"] {
        audit
            .validate_caller_lb_mapping(target, 10, 5)
            .unwrap_or_else(|e| panic!("{target}: {e}"));
    }
}
