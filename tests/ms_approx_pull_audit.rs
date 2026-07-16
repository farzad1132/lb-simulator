use lb::microservice::{ApproxPullAudit, MsArgs, OutputFormat, run};
use lb::policy::{LoadBalancePolicyKind, PullPolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::path::PathBuf;

fn chain3_approx_args(n: u32, seed: u64, audit: Option<std::sync::Arc<ApproxPullAudit>>) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
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
        force_fixed_svc: false,
        pull_audit: audit,
        no_bind: false,
        approx_sched: SchedulingPolicyKind::Fifo,
    }
}

/// Exercises approx pull with many concurrent requests and checks that:
/// 1. pull intents are delivered to the intended downstream replica,
/// 2. intent queues grow/shrink with correct depth accounting,
/// 3. downstream replicas pop intents in FIFO order under capacity limits, and
/// 4. upstream balancers pull the bound request_id on the matching rb_id.
#[test]
fn ms_approx_pull_intent_and_bound_pull_invariants() {
    let audit = ApproxPullAudit::new();
    let stats = run(&chain3_approx_args(500, 42, Some(audit.clone())))
        .unwrap()
        .expect("simulation should complete");

    assert_eq!(
        stats.by_api["handle"].e2e_ms.len(),
        500,
        "all requests should finish"
    );

    audit
        .validate_bound()
        .expect("approx pull audit invariants should hold");
}

/// Same invariants under a different seed and higher load (regression for port routing).
#[test]
fn ms_approx_pull_invariants_under_heavier_load() {
    let audit = ApproxPullAudit::new();
    let stats = run(&chain3_approx_args(2000, 7, Some(audit.clone())))
        .unwrap()
        .expect("simulation should complete");

    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 2000);
    audit.validate_bound().expect("audit should pass at n=2000");
}

/// Pull policy variant: power-of-two target selection still preserves binding invariants.
#[test]
fn ms_approx_pull_invariants_with_power_of_two_pull_policy() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let audit = ApproxPullAudit::new();
    let stats = run(&MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n: 800,
        lb_policy: LoadBalancePolicyKind::Approx,
        pull_policy: Some(PullPolicyKind::PowerOfTwo),
        lb_subset_size: 0,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        seed: Some(99),
        rps: None,
        slo_ms: None,
        format: OutputFormat::Json,
        trace: false,
        trace_limit: 0,
        scale: 0,
        verbose: 0,
        scheduling: SchedulingPolicyKind::Fifo,
        force_fixed_svc: false,
        pull_audit: Some(audit.clone()),
        no_bind: false,
        approx_sched: SchedulingPolicyKind::Fifo,
    })
    .unwrap()
    .expect("simulation should complete");

    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 800);
    audit.validate_bound().expect("audit should pass with P2C pull");
}
