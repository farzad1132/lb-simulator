use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::rng;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum AmphiQueueSchedKind {
    Fcfs,
    Edf,
    #[value(name = "edf+")]
    EdfPlus,
}

/// Shared DownstreamBalancer pull-queue discipline for `--lb-policy centralized`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum CentralizedSchedKind {
    #[default]
    Fcfs,
    Edf,
}

impl CentralizedSchedKind {
    pub fn uses_edf(self) -> bool {
        matches!(self, Self::Edf)
    }
}

impl AmphiQueueSchedKind {
    /// Outbound balancer queues use EDF insert for `edf` and `edf+`.
    pub fn outbound_uses_edf(self) -> bool {
        matches!(self, Self::Edf | Self::EdfPlus)
    }

    /// Replica pull-intent queues use EDF insert only for `edf+`.
    pub fn intent_queue_uses_edf(self) -> bool {
        matches!(self, Self::EdfPlus)
    }

    /// `edf` / `edf+` are only supported by the `ms` simulator.
    pub fn requires_ms(self) -> bool {
        matches!(self, Self::Edf | Self::EdfPlus)
    }
}

pub trait LoadBalancePolicy: Send {
    fn select(&mut self, loads: &[u32]) -> usize;
}

pub struct RandomPolicy;

impl LoadBalancePolicy for RandomPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        rng::random_usize_range(0..loads.len())
    }
}

pub struct PowerOfTwoPolicy;

impl LoadBalancePolicy for PowerOfTwoPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let n = loads.len();
        if n <= 1 {
            return 0;
        }
        let i = rng::random_usize_range(0..n);
        let j = rng::random_usize_range(0..n);
        if loads[i] <= loads[j] { i } else { j }
    }
}

pub struct RoundRobinPolicy {
    order: Vec<usize>,
    next: usize,
}

impl RoundRobinPolicy {
    fn ensure_order(&mut self, n: usize) {
        if self.order.len() != n {
            self.order = (0..n).collect();
            rng::shuffle(&mut self.order);
            self.next = 0;
        }
    }
}

impl LoadBalancePolicy for RoundRobinPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let n = loads.len();
        if n == 0 {
            return 0;
        }
        self.ensure_order(n);
        let local_idx = self.order[self.next % n];
        self.next += 1;
        local_idx
    }
}

pub struct LeastRequestPolicy;

impl LoadBalancePolicy for LeastRequestPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let min_load = *loads.iter().min().unwrap_or(&0);
        let tied: Vec<usize> = loads
            .iter()
            .enumerate()
            .filter(|&(_, &load)| load == min_load)
            .map(|(i, _)| i)
            .collect();
        if tied.is_empty() {
            return 0;
        }
        tied[rng::random_usize_range(0..tied.len())]
    }
}

pub struct CentralizedPolicy;

impl LoadBalancePolicy for CentralizedPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let _ = loads;
        0
    }
}

pub struct AmphiQueuePolicy;

impl LoadBalancePolicy for AmphiQueuePolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let _ = loads;
        0
    }
}

pub struct PrequalPolicy;

impl LoadBalancePolicy for PrequalPolicy {
    fn select(&mut self, loads: &[u32]) -> usize {
        let _ = loads;
        0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum PullPolicyKind {
    Random,
    #[value(name = "power-of-two")]
    PowerOfTwo,
    #[value(name = "round-robin")]
    RoundRobin,
    #[value(name = "least-request")]
    LeastRequest,
}

impl PullPolicyKind {
    pub fn build(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::Random => LoadBalancePolicyKind::Random.build(),
            Self::PowerOfTwo => LoadBalancePolicyKind::PowerOfTwo.build(),
            Self::RoundRobin => LoadBalancePolicyKind::RoundRobin.build(),
            Self::LeastRequest => LoadBalancePolicyKind::LeastRequest.build(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum LoadBalancePolicyKind {
    Random,
    PowerOfTwo,
    RoundRobin,
    LeastRequest,
    Centralized,
    #[value(name = "jbsq")]
    Jbsq,
    #[value(name = "amphiqueue")]
    AmphiQueue,
    #[value(name = "amphiqueue-share")]
    AmphiQueueShare,
    #[value(name = "prequal")]
    Prequal,
    #[value(name = "cl")]
    Cl,
    #[value(name = "cl-lr")]
    ClLr,
    #[value(name = "cl-r")]
    ClR,
    #[value(name = "cl-rr")]
    ClRr,
    #[value(name = "corr")]
    Corr,
}

impl LoadBalancePolicyKind {
    pub fn build(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::Random => Box::new(RandomPolicy),
            Self::PowerOfTwo => Box::new(PowerOfTwoPolicy),
            Self::RoundRobin => Box::new(RoundRobinPolicy {
                order: Vec::new(),
                next: 0,
            }),
            Self::LeastRequest => Box::new(LeastRequestPolicy),
            Self::Centralized | Self::Jbsq => Box::new(CentralizedPolicy),
            Self::AmphiQueue | Self::AmphiQueueShare => Box::new(AmphiQueuePolicy),
            Self::Prequal => Box::new(PrequalPolicy),
            Self::Cl | Self::ClLr | Self::ClR | Self::ClRr | Self::Corr => {
                Box::new(PowerOfTwoPolicy)
            }
        }
    }

    pub fn is_centralized(self) -> bool {
        matches!(self, Self::Centralized)
    }

    pub fn is_jbsq(self) -> bool {
        matches!(self, Self::Jbsq)
    }

    /// Shared DownstreamBalancer pull queue (centralized or jbsq).
    pub fn uses_central_pull_queue(self) -> bool {
        matches!(self, Self::Centralized | Self::Jbsq)
    }

    pub fn is_amphiqueue(self) -> bool {
        matches!(self, Self::AmphiQueue)
    }

    pub fn is_amphiqueue_share(self) -> bool {
        matches!(self, Self::AmphiQueueShare)
    }

    /// AmphiQueue pull-intent protocol (decentralized or shared-sidecar).
    pub fn uses_amphiqueue_protocol(self) -> bool {
        matches!(self, Self::AmphiQueue | Self::AmphiQueueShare)
    }

    pub fn is_prequal(self) -> bool {
        matches!(self, Self::Prequal)
    }

    pub fn is_pull_based(self) -> bool {
        matches!(
            self,
            Self::Centralized | Self::Jbsq | Self::AmphiQueue | Self::AmphiQueueShare
        )
    }

    pub fn is_cl(self) -> bool {
        matches!(self, Self::Cl)
    }

    pub fn is_corr(self) -> bool {
        matches!(self, Self::Corr)
    }

    pub fn is_ms_only(self) -> bool {
        matches!(
            self,
            Self::Cl
                | Self::ClLr
                | Self::ClR
                | Self::ClRr
                | Self::Corr
                | Self::AmphiQueueShare
                | Self::Jbsq
        )
    }

    pub fn uses_shared_downstream(self) -> bool {
        matches!(
            self,
            Self::Cl
                | Self::ClLr
                | Self::ClR
                | Self::ClRr
                | Self::Centralized
                | Self::Jbsq
                | Self::Corr
        )
    }

    pub fn ingress_policy(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::Cl
            | Self::ClLr
            | Self::ClR
            | Self::ClRr
            | Self::Centralized
            | Self::Jbsq
            | Self::Corr
            | Self::AmphiQueue
            | Self::AmphiQueueShare
            | Self::Prequal => Box::new(PowerOfTwoPolicy),
            other => other.build(),
        }
    }

    pub fn downstream_push_policy(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::ClLr => Box::new(LeastRequestPolicy),
            Self::ClR => Box::new(RandomPolicy),
            Self::ClRr => Box::new(RoundRobinPolicy {
                order: Vec::new(),
                next: 0,
            }),
            Self::Cl => Box::new(PowerOfTwoPolicy),
            _ => Box::new(PowerOfTwoPolicy),
        }
    }
}

pub fn validate_pull_policy(
    lb_policy: LoadBalancePolicyKind,
    pull_policy: Option<PullPolicyKind>,
) -> Result<(), String> {
    match (lb_policy.uses_amphiqueue_protocol(), pull_policy) {
        (true, None) => {
            Err("--pull-policy is required with --lb-policy amphiqueue or amphiqueue-share".into())
        }
        (false, Some(_)) => Err(
            "--pull-policy is only valid with --lb-policy amphiqueue or amphiqueue-share".into(),
        ),
        _ => Ok(()),
    }
}

pub fn validate_amphiqueue_sched(
    lb_policy: LoadBalancePolicyKind,
    amphiqueue_sched: Option<AmphiQueueSchedKind>,
    allow_edf: bool,
) -> Result<(), String> {
    let Some(amphiqueue_sched) = amphiqueue_sched else {
        return Ok(());
    };
    if !lb_policy.uses_amphiqueue_protocol() {
        return Err(
            "--amphiqueue-sched is only valid with --lb-policy amphiqueue or amphiqueue-share"
                .into(),
        );
    }
    if amphiqueue_sched.requires_ms() && !allow_edf {
        return Err("--amphiqueue-sched edf/edf+ is only supported by the ms simulator".into());
    }
    Ok(())
}

/// `--centralized-sched edf` is only valid with `--lb-policy centralized` or `jbsq`.
/// Default `fcfs` is allowed with any policy (no-op when not a central pull queue).
pub fn validate_centralized_sched(
    lb_policy: LoadBalancePolicyKind,
    centralized_sched: CentralizedSchedKind,
) -> Result<(), String> {
    if centralized_sched.uses_edf() && !lb_policy.uses_central_pull_queue() {
        return Err(
            "--centralized-sched edf is only valid with --lb-policy centralized or jbsq".into(),
        );
    }
    Ok(())
}

/// `--jbsq-n` is required with `--lb-policy jbsq` (no default) and must be >= 1.
pub fn validate_jbsq_n(
    lb_policy: LoadBalancePolicyKind,
    jbsq_n: Option<u32>,
) -> Result<(), String> {
    match (lb_policy.is_jbsq(), jbsq_n) {
        (true, None) => Err("--jbsq-n is required with --lb-policy jbsq".into()),
        (true, Some(0)) => Err("--jbsq-n must be >= 1 with --lb-policy jbsq".into()),
        (false, Some(_)) => Err("--jbsq-n is only valid with --lb-policy jbsq".into()),
        _ => Ok(()),
    }
}

pub fn validate_amphiqueue_share(
    lb_policy: LoadBalancePolicyKind,
    amphiqueue_share: u32,
) -> Result<(), String> {
    if lb_policy.is_amphiqueue_share() {
        if amphiqueue_share == 0 {
            return Err("--amphiqueue-share must be >= 1 with --lb-policy amphiqueue-share".into());
        }
        return Ok(());
    }
    if amphiqueue_share != 1 {
        return Err("--amphiqueue-share is only valid with --lb-policy amphiqueue-share".into());
    }
    Ok(())
}

pub fn validate_prequal_subset(
    lb_policy: LoadBalancePolicyKind,
    lb_subset_size: u32,
) -> Result<(), String> {
    if lb_policy.is_prequal() && lb_subset_size > 0 {
        return Err("--lb-subset-size is not supported with --lb-policy prequal".into());
    }
    if lb_policy.is_amphiqueue_share() && lb_subset_size > 0 {
        return Err("--lb-subset-size is not supported with --lb-policy amphiqueue-share".into());
    }
    Ok(())
}

/// Central pull-queue subsetting is a strict partition: `k` must divide `n_servers`,
/// clients must be divisible by the subset count, and only deterministic assignment
/// is allowed. No-op unless policy uses a central pull queue and `lb_subset_size > 0`.
pub fn validate_centralized_subset(
    lb_policy: LoadBalancePolicyKind,
    servers: u32,
    clients: u32,
    lb_subset_size: u32,
    lb_subset_policy: crate::subset::SubsetPolicyKind,
) -> Result<(), String> {
    if !lb_policy.uses_central_pull_queue() || lb_subset_size == 0 {
        return Ok(());
    }

    let policy_name = if lb_policy.is_jbsq() {
        "jbsq"
    } else {
        "centralized"
    };

    if lb_subset_policy != crate::subset::SubsetPolicyKind::Deterministic {
        return Err(format!(
            "--lb-subset-policy random is not supported with --lb-policy {policy_name}; use deterministic"
        ));
    }

    let n = servers.max(1) as usize;
    let k = (lb_subset_size as usize).min(n).max(1);
    if n % k != 0 {
        return Err(format!(
            "--lb-subset-size {lb_subset_size} must evenly divide --servers {servers} with --lb-policy {policy_name}"
        ));
    }

    let subset_count = n / k;
    let n_clients = clients.max(1) as usize;
    if n_clients % subset_count != 0 {
        return Err(format!(
            "--clients {clients} must be divisible by the subset count ({subset_count} = servers/subset-size) with --lb-policy {policy_name}"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centralized_policy_kind_is_centralized() {
        assert!(LoadBalancePolicyKind::Centralized.is_centralized());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_centralized());
    }

    #[test]
    fn amphiqueue_policy_kind_is_amphiqueue() {
        assert!(LoadBalancePolicyKind::AmphiQueue.is_amphiqueue());
        assert!(!LoadBalancePolicyKind::AmphiQueueShare.is_amphiqueue());
        assert!(LoadBalancePolicyKind::AmphiQueue.uses_amphiqueue_protocol());
        assert!(LoadBalancePolicyKind::AmphiQueueShare.uses_amphiqueue_protocol());
        assert!(LoadBalancePolicyKind::AmphiQueueShare.is_amphiqueue_share());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_amphiqueue());
    }

    #[test]
    fn prequal_policy_kind_flags() {
        assert!(LoadBalancePolicyKind::Prequal.is_prequal());
        assert!(!LoadBalancePolicyKind::Prequal.is_pull_based());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_prequal());
    }

    #[test]
    fn prequal_ingress_is_power_of_two() {
        crate::rng::enter_run(Some(42));
        let mut prequal = LoadBalancePolicyKind::Prequal.ingress_policy();
        let loads = [3u32, 0, 7, 2];
        let prequal_pick = prequal.select(&loads);

        crate::rng::enter_run(Some(42));
        let mut p2c = PowerOfTwoPolicy;
        assert_eq!(p2c.select(&loads), prequal_pick);
        crate::rng::exit_run();
    }

    #[test]
    fn validate_prequal_subset_rejects_nonzero() {
        assert!(validate_prequal_subset(LoadBalancePolicyKind::Prequal, 0).is_ok());
        let err = validate_prequal_subset(LoadBalancePolicyKind::Prequal, 3).unwrap_err();
        assert!(err.contains("--lb-subset-size is not supported"));
        assert!(validate_prequal_subset(LoadBalancePolicyKind::PowerOfTwo, 3).is_ok());
    }

    #[test]
    fn validate_centralized_subset_ok_partition() {
        use crate::subset::SubsetPolicyKind;
        assert!(
            validate_centralized_subset(
                LoadBalancePolicyKind::Centralized,
                12,
                2,
                6,
                SubsetPolicyKind::Deterministic,
            )
            .is_ok()
        );
        assert!(
            validate_centralized_subset(
                LoadBalancePolicyKind::Centralized,
                12,
                4,
                6,
                SubsetPolicyKind::Deterministic,
            )
            .is_ok()
        );
        assert!(
            validate_centralized_subset(
                LoadBalancePolicyKind::Centralized,
                12,
                3,
                0,
                SubsetPolicyKind::Random,
            )
            .is_ok()
        );
        assert!(
            validate_centralized_subset(
                LoadBalancePolicyKind::PowerOfTwo,
                12,
                3,
                5,
                SubsetPolicyKind::Random,
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_centralized_subset_rejects_non_divisor() {
        use crate::subset::SubsetPolicyKind;
        let err = validate_centralized_subset(
            LoadBalancePolicyKind::Centralized,
            12,
            2,
            5,
            SubsetPolicyKind::Deterministic,
        )
        .unwrap_err();
        assert!(err.contains("must evenly divide"));
    }

    #[test]
    fn validate_centralized_subset_rejects_clients_not_divisible() {
        use crate::subset::SubsetPolicyKind;
        let err = validate_centralized_subset(
            LoadBalancePolicyKind::Centralized,
            12,
            3,
            6,
            SubsetPolicyKind::Deterministic,
        )
        .unwrap_err();
        assert!(err.contains("must be divisible by the subset count"));
    }

    #[test]
    fn validate_centralized_subset_rejects_random() {
        use crate::subset::SubsetPolicyKind;
        let err = validate_centralized_subset(
            LoadBalancePolicyKind::Centralized,
            12,
            2,
            6,
            SubsetPolicyKind::Random,
        )
        .unwrap_err();
        assert!(err.contains("random is not supported"));
    }

    #[test]
    fn amphiqueue_ingress_is_power_of_two() {
        crate::rng::enter_run(Some(42));
        let mut amphiqueue = LoadBalancePolicyKind::AmphiQueue.ingress_policy();
        let loads = [3u32, 0, 7, 2];
        let amphiqueue_pick = amphiqueue.select(&loads);

        crate::rng::enter_run(Some(42));
        let mut centralized = LoadBalancePolicyKind::Centralized.ingress_policy();
        assert_eq!(centralized.select(&loads), amphiqueue_pick);
        crate::rng::exit_run();
    }

    #[test]
    fn validate_pull_policy_required_for_amphiqueue() {
        let err = validate_pull_policy(LoadBalancePolicyKind::AmphiQueue, None).unwrap_err();
        assert!(err.contains("--pull-policy is required"));
    }

    #[test]
    fn validate_pull_policy_rejected_without_amphiqueue() {
        let err = validate_pull_policy(
            LoadBalancePolicyKind::PowerOfTwo,
            Some(PullPolicyKind::LeastRequest),
        )
        .unwrap_err();
        assert!(err.contains("--pull-policy is only valid"));
    }

    #[test]
    fn pull_policy_kind_build_delegates_to_push_policies() {
        let loads = [5u32, 1, 3];
        let mut pull_lr = PullPolicyKind::LeastRequest.build();
        let mut lb_lr = LoadBalancePolicyKind::LeastRequest.build();
        assert_eq!(pull_lr.select(&loads), lb_lr.select(&loads));
    }

    #[test]
    fn cl_policy_kind_is_cl() {
        assert!(LoadBalancePolicyKind::Cl.is_cl());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_cl());
    }

    #[test]
    fn corr_policy_kind_is_corr() {
        assert!(LoadBalancePolicyKind::Corr.is_corr());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_corr());
    }

    #[test]
    fn uses_shared_downstream_for_cl_centralized_and_corr() {
        assert!(LoadBalancePolicyKind::Cl.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::ClLr.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::ClR.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::ClRr.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::Centralized.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::Jbsq.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::Corr.uses_shared_downstream());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.uses_shared_downstream());
    }

    #[test]
    fn jbsq_policy_kind_flags() {
        assert!(LoadBalancePolicyKind::Jbsq.is_jbsq());
        assert!(LoadBalancePolicyKind::Jbsq.uses_central_pull_queue());
        assert!(LoadBalancePolicyKind::Centralized.uses_central_pull_queue());
        assert!(!LoadBalancePolicyKind::Jbsq.is_centralized());
        assert!(LoadBalancePolicyKind::Jbsq.is_pull_based());
        assert!(LoadBalancePolicyKind::Jbsq.is_ms_only());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_jbsq());
    }

    #[test]
    fn is_ms_only_for_cl_cl_lr_corr_and_amphiqueue_share() {
        assert!(LoadBalancePolicyKind::Cl.is_ms_only());
        assert!(LoadBalancePolicyKind::ClLr.is_ms_only());
        assert!(LoadBalancePolicyKind::ClR.is_ms_only());
        assert!(LoadBalancePolicyKind::ClRr.is_ms_only());
        assert!(LoadBalancePolicyKind::Corr.is_ms_only());
        assert!(LoadBalancePolicyKind::AmphiQueueShare.is_ms_only());
        assert!(LoadBalancePolicyKind::Jbsq.is_ms_only());
        assert!(!LoadBalancePolicyKind::AmphiQueue.is_ms_only());
        assert!(!LoadBalancePolicyKind::Centralized.is_ms_only());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_ms_only());
    }

    #[test]
    fn validate_jbsq_n_rules() {
        assert!(validate_jbsq_n(LoadBalancePolicyKind::Jbsq, Some(1)).is_ok());
        assert!(validate_jbsq_n(LoadBalancePolicyKind::Jbsq, Some(3)).is_ok());
        let err = validate_jbsq_n(LoadBalancePolicyKind::Jbsq, None).unwrap_err();
        assert!(err.contains("--jbsq-n is required"));
        let err = validate_jbsq_n(LoadBalancePolicyKind::Jbsq, Some(0)).unwrap_err();
        assert!(err.contains("must be >= 1"));
        let err = validate_jbsq_n(LoadBalancePolicyKind::Centralized, Some(2)).unwrap_err();
        assert!(err.contains("only valid with --lb-policy jbsq"));
        assert!(validate_jbsq_n(LoadBalancePolicyKind::PowerOfTwo, None).is_ok());
    }

    #[test]
    fn validate_centralized_sched_allows_edf_for_jbsq() {
        assert!(
            validate_centralized_sched(LoadBalancePolicyKind::Jbsq, CentralizedSchedKind::Edf,)
                .is_ok()
        );
        let err = validate_centralized_sched(
            LoadBalancePolicyKind::PowerOfTwo,
            CentralizedSchedKind::Edf,
        )
        .unwrap_err();
        assert!(err.contains("centralized or jbsq"));
    }

    #[test]
    fn validate_amphiqueue_share_rules() {
        assert!(validate_amphiqueue_share(LoadBalancePolicyKind::AmphiQueueShare, 1).is_ok());
        assert!(validate_amphiqueue_share(LoadBalancePolicyKind::AmphiQueueShare, 3).is_ok());
        let err = validate_amphiqueue_share(LoadBalancePolicyKind::AmphiQueueShare, 0).unwrap_err();
        assert!(err.contains("must be >= 1"));
        assert!(validate_amphiqueue_share(LoadBalancePolicyKind::AmphiQueue, 1).is_ok());
        let err = validate_amphiqueue_share(LoadBalancePolicyKind::AmphiQueue, 2).unwrap_err();
        assert!(err.contains("only valid with --lb-policy amphiqueue-share"));
        assert!(validate_prequal_subset(LoadBalancePolicyKind::AmphiQueueShare, 0).is_ok());
        let err = validate_prequal_subset(LoadBalancePolicyKind::AmphiQueueShare, 3).unwrap_err();
        assert!(err.contains("--lb-subset-size is not supported"));
    }

    #[test]
    fn cl_lr_ingress_is_power_of_two() {
        crate::rng::enter_run(Some(42));
        let mut cl = LoadBalancePolicyKind::Cl.ingress_policy();
        let loads = [3u32, 0, 7, 2];
        let cl_pick = cl.select(&loads);

        crate::rng::enter_run(Some(42));
        let mut cl_lr = LoadBalancePolicyKind::ClLr.ingress_policy();
        assert_eq!(cl_lr.select(&loads), cl_pick);
        crate::rng::exit_run();
    }

    #[test]
    fn cl_r_and_cl_rr_ingress_is_power_of_two() {
        crate::rng::enter_run(Some(42));
        let mut cl = LoadBalancePolicyKind::Cl.ingress_policy();
        let loads = [3u32, 0, 7, 2];
        let cl_pick = cl.select(&loads);

        crate::rng::enter_run(Some(42));
        let mut cl_r = LoadBalancePolicyKind::ClR.ingress_policy();
        assert_eq!(cl_r.select(&loads), cl_pick);

        crate::rng::enter_run(Some(42));
        let mut cl_rr = LoadBalancePolicyKind::ClRr.ingress_policy();
        assert_eq!(cl_rr.select(&loads), cl_pick);
        crate::rng::exit_run();
    }

    #[test]
    fn cl_lr_downstream_is_least_request() {
        let mut policy = LoadBalancePolicyKind::ClLr.downstream_push_policy();
        let loads = [5u32, 1, 3];
        assert_eq!(policy.select(&loads), 1);
    }

    #[test]
    fn cl_r_downstream_is_random() {
        crate::rng::enter_run(Some(7));
        let mut expected = RandomPolicy;
        let loads = [5u32, 1, 3, 2];
        let expected_pick = expected.select(&loads);

        crate::rng::enter_run(Some(7));
        let mut policy = LoadBalancePolicyKind::ClR.downstream_push_policy();
        assert_eq!(policy.select(&loads), expected_pick);
        crate::rng::exit_run();
    }

    #[test]
    fn cl_rr_downstream_is_round_robin() {
        crate::rng::enter_run(Some(11));
        let mut expected = RoundRobinPolicy {
            order: Vec::new(),
            next: 0,
        };
        let loads = [5u32, 1, 3, 2];
        let expected_picks: Vec<usize> = (0..8).map(|_| expected.select(&loads)).collect();

        crate::rng::enter_run(Some(11));
        let mut policy = LoadBalancePolicyKind::ClRr.downstream_push_policy();
        let actual_picks: Vec<usize> = (0..8).map(|_| policy.select(&loads)).collect();
        assert_eq!(actual_picks, expected_picks);
        crate::rng::exit_run();
    }

    #[test]
    fn validate_amphiqueue_sched_requires_amphiqueue_and_ms_for_edf() {
        assert!(
            validate_amphiqueue_sched(
                LoadBalancePolicyKind::AmphiQueue,
                Some(AmphiQueueSchedKind::Edf),
                true,
            )
            .is_ok()
        );
        assert!(
            validate_amphiqueue_sched(
                LoadBalancePolicyKind::AmphiQueueShare,
                Some(AmphiQueueSchedKind::EdfPlus),
                true,
            )
            .is_ok()
        );
        assert!(
            validate_amphiqueue_sched(
                LoadBalancePolicyKind::AmphiQueue,
                Some(AmphiQueueSchedKind::Fcfs),
                false,
            )
            .is_ok()
        );
        assert!(validate_amphiqueue_sched(LoadBalancePolicyKind::AmphiQueue, None, false).is_ok());
        let err = validate_amphiqueue_sched(
            LoadBalancePolicyKind::PowerOfTwo,
            Some(AmphiQueueSchedKind::Fcfs),
            false,
        )
        .unwrap_err();
        assert!(err.contains("amphiqueue"));
        let err = validate_amphiqueue_sched(
            LoadBalancePolicyKind::AmphiQueue,
            Some(AmphiQueueSchedKind::Edf),
            false,
        )
        .unwrap_err();
        assert!(err.contains("ms simulator"));
        let err = validate_amphiqueue_sched(
            LoadBalancePolicyKind::AmphiQueue,
            Some(AmphiQueueSchedKind::EdfPlus),
            false,
        )
        .unwrap_err();
        assert!(err.contains("ms simulator"));
        assert!(AmphiQueueSchedKind::Edf.outbound_uses_edf());
        assert!(AmphiQueueSchedKind::EdfPlus.outbound_uses_edf());
        assert!(!AmphiQueueSchedKind::Fcfs.outbound_uses_edf());
        assert!(AmphiQueueSchedKind::EdfPlus.intent_queue_uses_edf());
        assert!(!AmphiQueueSchedKind::Edf.intent_queue_uses_edf());
    }

    #[test]
    fn validate_centralized_sched_requires_centralized_for_edf() {
        assert!(
            validate_centralized_sched(
                LoadBalancePolicyKind::Centralized,
                CentralizedSchedKind::Fcfs,
            )
            .is_ok()
        );
        assert!(
            validate_centralized_sched(
                LoadBalancePolicyKind::Centralized,
                CentralizedSchedKind::Edf,
            )
            .is_ok()
        );
        assert!(
            validate_centralized_sched(
                LoadBalancePolicyKind::PowerOfTwo,
                CentralizedSchedKind::Fcfs,
            )
            .is_ok()
        );
        let err = validate_centralized_sched(
            LoadBalancePolicyKind::PowerOfTwo,
            CentralizedSchedKind::Edf,
        )
        .unwrap_err();
        assert!(err.contains("only valid with --lb-policy centralized or jbsq"));
        assert!(CentralizedSchedKind::Edf.uses_edf());
        assert!(!CentralizedSchedKind::Fcfs.uses_edf());
    }
}
