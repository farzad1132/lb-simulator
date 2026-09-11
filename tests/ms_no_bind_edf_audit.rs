use lb::microservice::{AmphiQueuePullAudit, MsArgs, MsServiceDistribution, OutputFormat, run};
use lb::policy::{AmphiQueueSchedKind, CentralizedSchedKind, LoadBalancePolicyKind, PullPolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;

fn amphiqueue_args(
    callgraph: PathBuf,
    load_file: PathBuf,
    n: u32,
    seed: u64,
    amphiqueue_sched: AmphiQueueSchedKind,
    audit: Option<std::sync::Arc<AmphiQueuePullAudit>>,
) -> MsArgs {
    MsArgs {
        callgraph,
        load_file,
        n,
        lb_policy: LoadBalancePolicyKind::AmphiQueue,
        pull_policy: Some(PullPolicyKind::LeastRequest),
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
        amphiqueue_sched: Some(amphiqueue_sched),
        amphiqueue_share: 1,
        jbsq_n: None,
    }
}

fn run_with_audit(args: &MsArgs) -> lb::microservice::MsStats {
    run(args)
        .unwrap()
        .expect("simulation should complete")
}

#[test]
fn ms_no_bind_edf_trace_invariants() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        AmphiQueueSchedKind::Edf,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind_edf().expect("no-bind edf invariants");
}

#[test]
fn ms_no_bind_edf_pulls_earliest_deadline_not_intent_id() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        AmphiQueueSchedKind::Edf,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind_edf().expect("no-bind edf invariants");

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
fn ms_no_bind_edf_multi_caller_independent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = AmphiQueuePullAudit::new();
    let args = amphiqueue_args(
        root.join("tests/fanin/multi/callgraph.json"),
        root.join("tests/fanin/multi/load.json"),
        400,
        7,
        AmphiQueueSchedKind::Edf,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["f1"].e2e_ms.len(), 400);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind_edf().expect("no-bind edf invariants");
}

#[test]
fn ms_no_bind_edf_differs_from_fcfs() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let callgraph = root.join("tests/chain/3/callgraph.json");
    let load_file = root.join("tests/chain/3/load.json");

    let fcfs_audit = AmphiQueuePullAudit::new();
    let fcfs_args = amphiqueue_args(
        callgraph.clone(),
        load_file.clone(),
        500,
        99,
        AmphiQueueSchedKind::Fcfs,
        Some(fcfs_audit.clone()),
    );
    run_with_audit(&fcfs_args);
    fcfs_audit.validate_no_bind().expect("fcfs invariants");

    let edf_audit = AmphiQueuePullAudit::new();
    let edf_args = amphiqueue_args(
        callgraph,
        load_file,
        500,
        99,
        AmphiQueueSchedKind::Edf,
        Some(edf_audit.clone()),
    );
    run_with_audit(&edf_args);
    edf_audit.validate_no_bind_edf().expect("edf invariants");

    let fcfs_order = fcfs_audit.pull_fulfilled_request_ids();
    let edf_order = edf_audit.pull_fulfilled_request_ids();
    assert_eq!(fcfs_order.len(), edf_order.len());
    assert!(
        fcfs_order != edf_order,
        "expected EDF to change at least one pull fulfillment order under backlog"
    );
}
