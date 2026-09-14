use lb::microservice::{AmphiQueuePullAudit, MsArgs, MsServiceDistribution, OutputFormat, run};
use lb::policy::{
    AmphiQueueSchedKind, CentralizedSchedKind, LoadBalancePolicyKind, PullPolicyKind,
};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::collections::HashMap;
use std::path::PathBuf;

fn amphiqueue_args(
    callgraph: PathBuf,
    load_file: PathBuf,
    n: u32,
    seed: u64,
    amphiqueue_sched: Option<AmphiQueueSchedKind>,
    pull_policy: PullPolicyKind,
    audit: Option<std::sync::Arc<AmphiQueuePullAudit>>,
) -> MsArgs {
    MsArgs {
        callgraph,
        load_file,
        n,
        lb_policy: LoadBalancePolicyKind::AmphiQueue,
        pull_policy: Some(pull_policy),
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
        centralized_sched: CentralizedSchedKind::Fcfs,
        service_dist: MsServiceDistribution::Exp,
        pull_audit: audit,
        centralized_audit: None,
        jbsq_audit: None,
        amphiqueue_sched,
        amphiqueue_share: 1,
        jbsq_n: None,
        eq_scale: None,
        eq_scale_overrides: HashMap::new(),
    }
}

fn run_with_audit(args: &MsArgs) -> lb::microservice::MsStats {
    run(args).unwrap().expect("simulation should complete")
}

#[test]
fn ms_no_bind_trace_invariants() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        Some(AmphiQueueSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");
}

#[test]
fn ms_no_bind_pulls_oldest_not_intent_id() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        Some(AmphiQueueSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");

    let mismatches: Vec<_> = audit
        .pull_fulfilled_events()
        .into_iter()
        .filter(|(intent_id, pulled_id, _)| intent_id != pulled_id)
        .collect();
    assert!(
        !mismatches.is_empty(),
        "expected at least one pull where intent_request_id != pulled_request_id"
    );
    for (intent_id, pulled_id, head_id) in audit.pull_fulfilled_events() {
        assert_eq!(
            head_id,
            Some(pulled_id),
            "pulled request must match recorded queue head"
        );
        let _ = intent_id;
    }
}

#[test]
fn ms_no_bind_multi_caller_independent_fcfs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/fanin/multi/callgraph.json"),
        root.join("tests/fanin/multi/load.json"),
        400,
        7,
        Some(AmphiQueueSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["f1"].e2e_ms.len(), 400);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");
}

#[test]
fn ms_bound_pull_trace_regression() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        200,
        42,
        None,
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 200);
    audit.validate_common().expect("common invariants");
    audit.validate_bound().expect("bound pull invariants");
}
