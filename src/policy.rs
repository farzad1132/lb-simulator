use clap::ValueEnum;

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
        if loads[i] <= loads[j] {
            i
        } else {
            j
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LoadBalancePolicyKind {
    Random,
    PowerOfTwo,
    RoundRobin,
    LeastRequest,
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
        }
    }
}
