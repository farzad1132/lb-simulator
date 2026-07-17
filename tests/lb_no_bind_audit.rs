use lb::lb_pull_audit::LbPullAudit;
use lb::lb_simulate::{LbArrivalDistribution, LbRunArgs, LbServiceDistribution};
use lb::policy::{ApproxSchedKind, LoadBalancePolicyKind, PullPolicyKind};
use lb::rng;
use lb::subset::SubsetPolicyKind;
use std::sync::Arc;

fn approx_args(
    n: u32,
    _seed: u64,
    approx_sched: Option<ApproxSchedKind>,
    pull_policy: PullPolicyKind,
    servers: u32,
    concurrency: u32,
    clients: u32,
    load: f32,
    audit: Option<Arc<LbPullAudit>>,
) -> LbRunArgs {
    LbRunArgs {
        load,
        n,
        service_dist: LbServiceDistribution::Constant,
        arrival: LbArrivalDistribution::Constant,
        service_modes: None,
        service_mode_probs: None,
        servers,
        concurrency,
        lb_policy: LoadBalancePolicyKind::Approx,
        pull_policy: Some(pull_policy),
        lb_subset_size: 0,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        clients,
        verbose: 0,
        approx_sched,
        pull_audit: audit,
        express_lane: None,
        work_shedding: None,
    }
}

fn run_with_audit(args: &LbRunArgs, seed: u64) -> lb::lb_simulate::LbServiceStats {
    rng::enter_run(Some(seed));
    let stats = lb::lb_simulate::run(args)
        .unwrap()
        .expect("simulation should complete");
    rng::exit_run();
    stats
}

#[test]
fn lb_no_bind_trace_invariants() {
    let audit = LbPullAudit::new();
    let args = approx_args(
        200,
        99,
        Some(ApproxSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        2,
        2,
        2,
        2.0,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args, 99);
    assert_eq!(stats.e2e.len(), 200);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");
}

#[test]
fn lb_no_bind_pulls_oldest_not_intent_id() {
    let audit = LbPullAudit::new();
    let args = approx_args(
        200,
        99,
        Some(ApproxSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        2,
        2,
        2,
        2.0,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args, 99);
    assert_eq!(stats.e2e.len(), 200);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");

    let mismatches: Vec<_> = audit
        .pull_fulfilled_events()
        .into_iter()
        .filter(|(intent_id, pulled_id, _)| intent_id.map(|i| i != *pulled_id).unwrap_or(false))
        .collect();
    assert!(
        !mismatches.is_empty(),
        "expected at least one pull where intent_request_id != pulled_task_id"
    );
    for (intent_id, pulled_id, head_id) in audit.pull_fulfilled_events() {
        assert_eq!(
            head_id,
            Some(pulled_id),
            "pulled task must match recorded queue head"
        );
        let _ = intent_id;
    }
}

#[test]
fn lb_no_bind_multi_client_independent_fcfs() {
    let audit = LbPullAudit::new();
    let args = approx_args(
        200,
        99,
        Some(ApproxSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        2,
        2,
        2,
        2.0,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args, 99);
    assert_eq!(stats.e2e.len(), 200);
    audit.validate_common().expect("common invariants");
    audit.validate_no_bind().expect("no-bind invariants");
}

#[test]
fn lb_bound_pull_trace_regression() {
    let audit = LbPullAudit::new();
    let args = approx_args(
        100,
        42,
        None,
        PullPolicyKind::LeastRequest,
        2,
        1,
        1,
        2.0,
        Some(audit.clone()),
    );
    let stats = run_with_audit(&args, 42);
    assert_eq!(stats.e2e.len(), 100);
    audit.validate_common().expect("common invariants");
    audit.validate_bound().expect("bound pull invariants");
}
