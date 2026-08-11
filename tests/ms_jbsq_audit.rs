use lb::microservice::{MsArgs, MsJbsqAudit, MsServiceDistribution, OutputFormat, run};
use lb::policy::{CentralizedSchedKind, LoadBalancePolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;
use std::sync::Arc;

fn chain3_args(
    n: u32,
    seed: u64,
    lb_policy: LoadBalancePolicyKind,
    jbsq_n: Option<u32>,
    centralized_sched: CentralizedSchedKind,
    service_dist: MsServiceDistribution,
    jbsq_audit: Option<Arc<MsJbsqAudit>>,
) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n,
        lb_policy,
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
        service_dist,
        pull_audit: None,
        centralized_audit: None,
        jbsq_audit,
        approx_sched: None,
        approx_share: 1,
        jbsq_n,
    }
}

fn chain3_jbsq_args(
    n: u32,
    seed: u64,
    jbsq_n: u32,
    centralized_sched: CentralizedSchedKind,
    audit: Option<Arc<MsJbsqAudit>>,
) -> MsArgs {
    chain3_args(
        n,
        seed,
        LoadBalancePolicyKind::Jbsq,
        Some(jbsq_n),
        centralized_sched,
        MsServiceDistribution::Exp,
        audit,
    )
}

/// n=1: pull only when idle (occupancy + pending < 1).
#[test]
fn ms_jbsq_n1_pull_bound_invariants() {
    let audit = MsJbsqAudit::new();
    let stats = run(&chain3_jbsq_args(
        800,
        42,
        1,
        CentralizedSchedKind::Fcfs,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 800);

    audit.validate().expect("jbsq n=1 audit invariants");
    assert!(audit.pull_sent_count() > 0);
    assert!(audit.pull_arrived_count() > 0);
}

/// n=2 with per-replica concurrency 1 on chain/3: local queueing should appear.
#[test]
fn ms_jbsq_n2_local_queue_and_bound() {
    let audit = MsJbsqAudit::new();
    let stats = run(&chain3_jbsq_args(
        2000,
        7,
        2,
        CentralizedSchedKind::Fcfs,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 2000);

    audit.validate().expect("jbsq n=2 audit invariants");
    assert!(
        audit.observed_local_queueing(),
        "expected local queueing when jbsq_n=2 > concurrency=1"
    );
}

#[test]
fn ms_jbsq_edf_completes_with_audit() {
    let audit = MsJbsqAudit::new();
    let stats = run(&chain3_jbsq_args(
        800,
        99,
        2,
        CentralizedSchedKind::Edf,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 800);
    audit.validate().expect("jbsq edf audit invariants");
}

/// On chain/3, per-replica concurrency is 1, so jbsq `--jbsq-n 1` is pull-on-idle
/// like centralized and must match latencies under the same seed.
#[test]
fn ms_jbsq_n1_matches_centralized() {
    let n = 1000u32;
    let seed = 42u64;
    let centralized = run(&chain3_args(
        n,
        seed,
        LoadBalancePolicyKind::Centralized,
        None,
        CentralizedSchedKind::Fcfs,
        MsServiceDistribution::Fixed,
        None,
    ))
    .unwrap()
    .expect("centralized");
    let jbsq = run(&chain3_args(
        n,
        seed,
        LoadBalancePolicyKind::Jbsq,
        Some(1),
        CentralizedSchedKind::Fcfs,
        MsServiceDistribution::Fixed,
        None,
    ))
    .unwrap()
    .expect("jbsq n=1");

    assert_eq!(
        centralized.by_api["handle"].e2e_ms,
        jbsq.by_api["handle"].e2e_ms,
        "jbsq n=1 should match centralized e2e latencies"
    );
    assert_eq!(
        centralized.by_api["handle"].processing_time_ms,
        jbsq.by_api["handle"].processing_time_ms,
        "jbsq n=1 should match centralized processing times"
    );
}
