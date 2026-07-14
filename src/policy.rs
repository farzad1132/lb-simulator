use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::rng;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum LoadBalancePolicyKind {
    Random,
    PowerOfTwo,
    RoundRobin,
    LeastRequest,
    Centralized,
    #[value(name = "cl")]
    Cl,
    #[value(name = "cl-lr")]
    ClLr,
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
            Self::Centralized => Box::new(CentralizedPolicy),
            Self::Cl | Self::ClLr | Self::Corr => Box::new(PowerOfTwoPolicy),
        }
    }

    pub fn is_centralized(self) -> bool {
        matches!(self, Self::Centralized)
    }

    pub fn is_cl(self) -> bool {
        matches!(self, Self::Cl)
    }

    pub fn is_corr(self) -> bool {
        matches!(self, Self::Corr)
    }

    pub fn is_ms_only(self) -> bool {
        matches!(self, Self::Cl | Self::ClLr | Self::Corr)
    }

    pub fn uses_shared_downstream(self) -> bool {
        matches!(
            self,
            Self::Cl | Self::ClLr | Self::Centralized | Self::Corr
        )
    }

    pub fn ingress_policy(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::Cl | Self::ClLr | Self::Centralized | Self::Corr => {
                Box::new(PowerOfTwoPolicy)
            }
            other => other.build(),
        }
    }

    pub fn downstream_push_policy(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::ClLr => Box::new(LeastRequestPolicy),
            Self::Cl => Box::new(PowerOfTwoPolicy),
            _ => Box::new(PowerOfTwoPolicy),
        }
    }
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
        assert!(LoadBalancePolicyKind::Centralized.uses_shared_downstream());
        assert!(LoadBalancePolicyKind::Corr.uses_shared_downstream());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.uses_shared_downstream());
    }

    #[test]
    fn is_ms_only_for_cl_cl_lr_and_corr() {
        assert!(LoadBalancePolicyKind::Cl.is_ms_only());
        assert!(LoadBalancePolicyKind::ClLr.is_ms_only());
        assert!(LoadBalancePolicyKind::Corr.is_ms_only());
        assert!(!LoadBalancePolicyKind::Centralized.is_ms_only());
        assert!(!LoadBalancePolicyKind::PowerOfTwo.is_ms_only());
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
    fn cl_lr_downstream_is_least_request() {
        let mut policy = LoadBalancePolicyKind::ClLr.downstream_push_policy();
        let loads = [5u32, 1, 3];
        assert_eq!(policy.select(&loads), 1);
    }
}
