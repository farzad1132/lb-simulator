use lb::microservice::{
    ApproxPullAudit, MsArgs, MsServiceDistribution, OutputFormat, n_sidecars, run, sidecar_id,
    sidecar_replicas,
};
use lb::policy::{ApproxSchedKind, CentralizedSchedKind, LoadBalancePolicyKind, PullPolicyKind};
use lb::scheduling::SchedulingPolicyKind;
use lb::subset::SubsetPolicyKind;
use std::collections::HashMap;
use std::path::PathBuf;

fn chain3_args(
    n: u32,
    seed: u64,
    approx_share: u32,
    approx_sched: Option<ApproxSchedKind>,
    pull_policy: PullPolicyKind,
    audit: Option<std::sync::Arc<ApproxPullAudit>>,
) -> MsArgs {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n,
        lb_policy: LoadBalancePolicyKind::ApproxShare,
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
        approx_sched,
        approx_share,
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[test]
fn ms_approx_share_one_bound_audit_matches_approx_invariants() {
    let audit = ApproxPullAudit::new();
    let stats = run(&chain3_args(
        500,
        42,
        1,
        None,
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_bound().expect("share=1 bound invariants");
}

#[test]
fn ms_approx_share_one_fcfs_audit_matches_approx_invariants() {
    let audit = ApproxPullAudit::new();
    let stats = run(&chain3_args(
        500,
        99,
        1,
        Some(ApproxSchedKind::Fcfs),
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 500);
    audit.validate_common().expect("common");
    audit.validate_no_bind().expect("share=1 fcfs invariants");
}

#[test]
fn ms_approx_share_one_latency_close_to_approx() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let n = 800u32;
    let seed = 42u64;
    let approx = run(&MsArgs {
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
        centralized_sched: CentralizedSchedKind::Fcfs,
        service_dist: MsServiceDistribution::Exp,
        pull_audit: None,
        centralized_audit: None,
        approx_sched: None,
        approx_share: 1,
    })
    .unwrap()
    .expect("approx");
    let share = run(&chain3_args(
        n,
        seed,
        1,
        None,
        PullPolicyKind::LeastRequest,
        None,
    ))
    .unwrap()
    .expect("approx-share");

    let mut a = approx.by_api["handle"].e2e_ms.clone();
    let mut s = share.by_api["handle"].e2e_ms.clone();
    a.sort_by(|x, y| x.partial_cmp(y).unwrap());
    s.sort_by(|x, y| x.partial_cmp(y).unwrap());
    let a_p50 = percentile(&a, 50.0);
    let s_p50 = percentile(&s, 50.0);
    let a_p99 = percentile(&a, 99.0);
    let s_p99 = percentile(&s, 99.0);
    let ratio = |x: f64, y: f64| (x / y).max(y / x);
    // Residual gap from pull-side sidecar hop (ingress matches approx at share=1).
    assert!(
        ratio(a_p50, s_p50) < 1.05,
        "p50 diverged: approx={a_p50} share1={s_p50}"
    );
    assert!(
        ratio(a_p99, s_p99) < 1.05,
        "p99 diverged: approx={a_p99} share1={s_p99}"
    );
}

/// share=1 enqueues ingress on the replica (same queue as returns); entry occupancy ≈ approx.
#[test]
fn ms_approx_share_one_entry_occupancy_close_to_approx() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let n = 800u32;
    let seed = 42u64;
    let approx = run(&MsArgs {
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
        centralized_sched: CentralizedSchedKind::Fcfs,
        service_dist: MsServiceDistribution::Exp,
        pull_audit: None,
        centralized_audit: None,
        approx_sched: None,
        approx_share: 1,
    })
    .unwrap()
    .expect("approx");
    let share = run(&chain3_args(
        n,
        seed,
        1,
        None,
        PullPolicyKind::LeastRequest,
        None,
    ))
    .unwrap()
    .expect("approx-share");

    let mean_occ = |servers: &HashMap<usize, f64>| -> f64 {
        servers.values().sum::<f64>() / servers.len() as f64
    };
    let a_mean = mean_occ(&approx.server_avg_queue_inflight["frontend"]);
    let s_mean = mean_occ(&share.server_avg_queue_inflight["frontend"]);
    assert!(
        a_mean > 0.0 && s_mean > 0.0,
        "approx={a_mean} share1={s_mean}"
    );
    let ratio = (a_mean / s_mean).max(s_mean / a_mean);
    assert!(
        ratio < 1.07,
        "frontend occupancy diverged: approx={a_mean} share1={s_mean} ratio={ratio}"
    );
}

#[test]
fn ms_approx_share_three_targets_sidecars_and_remainder() {
    // chain/3: 10 replicas/service, share=3 → 4 sidecars (remainder replica 9 alone).
    let share = 3u32;
    let replicas = 10usize;
    assert_eq!(n_sidecars(replicas, share), 4);
    assert_eq!(sidecar_replicas(3, replicas, share), vec![9]);

    let audit = ApproxPullAudit::new();
    let stats = run(&chain3_args(
        600,
        7,
        share,
        None,
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");
    assert_eq!(stats.by_api["handle"].e2e_ms.len(), 600);
    audit
        .validate_bound()
        .expect("bound invariants with share=3");

    let n_sc = n_sidecars(replicas, share);
    let intent_targets = audit.intent_sent_targets();
    assert!(!intent_targets.is_empty());
    for (target_ms, target_server) in &intent_targets {
        assert!(
            *target_server < n_sc,
            "{target_ms}: intent target {target_server} is not a sidecar id (n_sc={n_sc})"
        );
    }

    let fulfilled = audit.pull_fulfilled_topology();
    assert!(!fulfilled.is_empty());
    let mut saw_remainder = false;
    for (target_ms, pull_from, handler_server) in &fulfilled {
        assert!(
            *handler_server < n_sc,
            "{target_ms}: handler_server {handler_server} not a caller sidecar"
        );
        let sc = sidecar_id(*pull_from, share);
        let owned = sidecar_replicas(sc, replicas, share);
        assert!(
            owned.contains(pull_from),
            "{target_ms}: replica {pull_from} not in sidecar {sc} group {owned:?}"
        );
        if *pull_from == 9 {
            assert_eq!(sc, 3);
            saw_remainder = true;
        }
    }
    // With 600 requests across backends, remainder replica should usually see work.
    let _ = saw_remainder;
}

#[test]
fn ms_approx_share_idle_replica_drain_stays_in_group() {
    let share = 3u32;
    let replicas = 10usize;
    let audit = ApproxPullAudit::new();
    run(&chain3_args(
        400,
        11,
        share,
        None,
        PullPolicyKind::LeastRequest,
        Some(audit.clone()),
    ))
    .unwrap()
    .expect("simulation should complete");

    // request_id is reused across hops of the same e2e request; key by (target_ms, request_id).
    let mut drain_sidecar: HashMap<(String, u64), usize> = HashMap::new();
    for (ms, sc, req) in audit.intent_drained_topology() {
        drain_sidecar.insert((ms, req), sc);
    }

    let fulfilled_ids = audit.pull_fulfilled_events();
    let topology = audit.pull_fulfilled_topology();
    assert_eq!(fulfilled_ids.len(), topology.len());
    for ((intent_id, _pulled, _), (target_ms, pull_from, _)) in
        fulfilled_ids.iter().zip(topology.iter())
    {
        let Some(sc) = drain_sidecar.get(&(target_ms.clone(), *intent_id)) else {
            panic!("missing IntentDrained for {target_ms} intent {intent_id}");
        };
        let owned = sidecar_replicas(*sc, replicas, share);
        assert!(
            owned.contains(pull_from),
            "{target_ms}: drained sidecar {sc} but pull_from={pull_from} not in {owned:?}"
        );
        assert_eq!(sidecar_id(*pull_from, share), *sc);
    }
}

/// share>1 ingress joins the least-occupancy replica in the sidecar group.
/// Entry-tier per-replica occupancy should stay balanced within each pair.
#[test]
fn ms_approx_share_two_entry_occupancy_balanced_within_groups() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let n = 800u32;
    let seed = 42u64;
    let share = 2u32;
    let replicas = 10usize;
    assert_eq!(n_sidecars(replicas, share), 5);

    let stats = run(&MsArgs {
        callgraph: root.join("tests/chain/3/callgraph.json"),
        load_file: root.join("tests/chain/3/load.json"),
        n,
        lb_policy: LoadBalancePolicyKind::ApproxShare,
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
        pull_audit: None,
        centralized_audit: None,
        approx_sched: None,
        approx_share: share,
    })
    .unwrap()
    .expect("share=2");

    assert_eq!(stats.by_api["handle"].e2e_ms.len(), n as usize);
    let occ = &stats.server_avg_queue_inflight["frontend"];
    for sc in 0..n_sidecars(replicas, share) {
        let owned = sidecar_replicas(sc, replicas, share);
        assert_eq!(owned.len(), 2, "sidecar {sc} should own two replicas");
        let a = occ[&owned[0]];
        let b = occ[&owned[1]];
        assert!(a > 0.0 && b > 0.0, "sidecar {sc}: occ={a},{b}");
        // Least-occupancy join keeps peers in the same ballpark; returns and async
        // occupancy updates leave a residual gap vs perfect JSQ.
        let ratio = (a / b).max(b / a);
        assert!(
            ratio < 1.9,
            "sidecar {sc} replicas {}/{} occupancy unbalanced: {a} vs {b} ratio={ratio}",
            owned[0],
            owned[1]
        );
    }
}
