use clap::ValueEnum;
use rand::Rng;

pub trait LoadBalancePolicy: Send {
    fn select(&mut self, n_servers: usize) -> usize;
}

pub struct RandomPolicy;

impl LoadBalancePolicy for RandomPolicy {
    fn select(&mut self, n_servers: usize) -> usize {
        rand::rng().random_range(0..n_servers)
    }
}

#[derive(Clone, Copy, Debug, ValueEnum, Default)]
pub enum LoadBalancePolicyKind {
    #[default]
    Random,
}

impl LoadBalancePolicyKind {
    pub fn build(self) -> Box<dyn LoadBalancePolicy> {
        match self {
            Self::Random => Box::new(RandomPolicy),
        }
    }
}
