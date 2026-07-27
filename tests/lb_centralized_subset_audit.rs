use lb::lb_centralized_audit::LbCentralizedAudit;
use lb::lb_simulate::{LbArrivalDistribution, LbRunArgs, LbServiceDistribution};
use lb::policy::LoadBalancePolicyKind;
use lb::rng;
use lb::subset::SubsetPolicyKind;
use std::sync::Arc;

fn centralized_args(
    n: u32,
    servers: u32,
    clients: u32,
    subset_size: u32,
    audit: Option<Arc<LbCentralizedAudit>>,
) -> LbRunArgs {
    LbRunArgs {
        load: 0.8,
        n,
        service_dist: LbServiceDistribution::Constant,
        arrival: LbArrivalDistribution::Constant,
        service_modes: None,
        service_mode_probs: None,
        servers,
        concurrency: 1,
        lb_policy: LoadBalancePolicyKind::Centralized,
        pull_policy: None,
        lb_subset_size: subset_size,
        lb_subset_policy: SubsetPolicyKind::Deterministic,
        clients,
        verbose: 0,
        approx_sched: None,
        pull_audit: None,
        centralized_audit: audit,
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
fn lb_centralized_subset_disjoint_trace() {
    let audit = LbCentralizedAudit::new();
    let args = centralized_args(400, 12, 2, 6, Some(audit.clone()));
    let stats = run_with_audit(&args, 42);
    assert_eq!(stats.e2e.len(), 400);
    audit
        .validate_disjoint_subsets(12, 6)
        .expect("subsets must be disjoint");
}

#[test]
fn lb_centralized_per_subset_shared_lb_trace() {
    let audit = LbCentralizedAudit::new();
    // 4 clients, 2 subsets → clients {0,2} share LB 0; {1,3} share LB 1
    let args = centralized_args(400, 12, 4, 6, Some(audit.clone()));
    let stats = run_with_audit(&args, 7);
    assert_eq!(stats.e2e.len(), 400);
    audit
        .validate_centralized_per_subset(12, 6)
        .expect("each subset must use one shared centralized LB");
}

#[test]
fn lb_centralized_subset_smoke_completes() {
    let args = centralized_args(200, 12, 2, 6, None);
    let stats = run_with_audit(&args, 99);
    assert_eq!(stats.e2e.len(), 200);
    assert!(stats.utilization_pct > 0.0);
}
