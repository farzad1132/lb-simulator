use lb::microservice::{ApproxPullAudit, MsArgs, MsServiceDistribution, OutputFormat, run};
use lb::policy::{ApproxSchedKind, CentralizedSchedKind, LoadBalancePolicyKind, PullPolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;

fn approx_args(
    callgraph: PathBuf,
    load_file: PathBuf,
    n: u32,
    seed: u64,
    approx_sched: ApproxSchedKind,
    audit: Option<std::sync::Arc<ApproxPullAudit>>,
) -> MsArgs {
    MsArgs {
        callgraph,
        load_file,
        n,
        lb_policy: LoadBalancePolicyKind::Approx,
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
        approx_sched: Some(approx_sched),
        approx_share: 1,
    }
}

fn run_with_audit(args: &MsArgs) -> lb::microservice::MsStats {
    run(args)
        .unwrap()
        .expect("simulation should complete")
}

#[test]
fn ms_no_bind_edf_plus_trace_invariants() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = ApproxPullAudit::new();
    let args = approx_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        ApproxSchedKind::EdfPlus,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit
        .validate_no_bind_edf_plus()
        .expect("no-bind edf+ invariants");
}

#[test]
fn ms_no_bind_edf_plus_pulls_earliest_deadline_not_intent_id() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = ApproxPullAudit::new();
    let args = approx_args(
        root.join("tests/chain/3/callgraph.json"),
        root.join("tests/chain/3/load.json"),
        500,
        99,
        ApproxSchedKind::EdfPlus,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common invariants");
    audit
        .validate_no_bind_edf_plus()
        .expect("no-bind edf+ invariants");

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
fn ms_no_bind_edf_plus_multi_caller_independent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = ApproxPullAudit::new();
    let args = approx_args(
        root.join("tests/fanin/multi/callgraph.json"),
        root.join("tests/fanin/multi/load.json"),
        400,
        7,
        ApproxSchedKind::EdfPlus,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args);
    assert_eq!(stats.by_api["f1"].e2e_ms.len(), 400);
    audit.validate_common().expect("common invariants");
    audit
        .validate_no_bind_edf_plus()
        .expect("no-bind edf+ invariants");
}

#[test]
fn ms_no_bind_edf_plus_differs_from_edf_intent_drain() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let callgraph = root.join("tests/fanin/multi/callgraph.json");
    let load_file = root.join("tests/fanin/multi/load.json");

    let edf_audit = ApproxPullAudit::new();
    let edf_args = approx_args(
        callgraph.clone(),
        load_file.clone(),
        400,
        7,
        ApproxSchedKind::Edf,
        Some(edf_audit.clone()),
    );
    run_with_audit(&edf_args);
    edf_audit.validate_no_bind_edf().expect("edf invariants");

    let edf_plus_audit = ApproxPullAudit::new();
    let edf_plus_args = approx_args(
        callgraph,
        load_file,
        400,
        7,
        ApproxSchedKind::EdfPlus,
        Some(edf_plus_audit.clone()),
    );
    run_with_audit(&edf_plus_args);
    edf_plus_audit
        .validate_no_bind_edf_plus()
        .expect("edf+ invariants");

    let edf_drains = edf_audit.intent_drained_request_ids();
    let edf_plus_drains = edf_plus_audit.intent_drained_request_ids();
    assert_eq!(edf_drains.len(), edf_plus_drains.len());
    assert!(
        edf_drains != edf_plus_drains,
        "expected edf+ intent-queue EDF to change at least one intent drain order vs edf"
    );
}