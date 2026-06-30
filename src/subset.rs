use clap::ValueEnum;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

use crate::rng;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum SubsetPolicyKind {
    #[default]
    Deterministic,
    Random,
}

fn subset_size_k(n: usize, subset_size: u32) -> usize {
    if subset_size == 0 {
        n
    } else {
        (subset_size as usize).min(n).max(1)
    }
}

pub fn assign_subset(
    policy: SubsetPolicyKind,
    n: usize,
    client_id: usize,
    subset_size: u32,
) -> Vec<usize> {
    if n == 0 {
        return Vec::new();
    }

    let k = subset_size_k(n, subset_size);
    match policy {
        SubsetPolicyKind::Random => random_subset(n, k),
        SubsetPolicyKind::Deterministic => deterministic_subset(n, client_id, k),
    }
}

fn random_subset(n: usize, k: usize) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..n).collect();
    rng::shuffle(&mut indices);
    indices.truncate(k);
    indices
}

fn deterministic_subset(n: usize, client_id: usize, k: usize) -> Vec<usize> {
    if k >= n {
        return (0..n).collect();
    }

    let subset_count = n / k;
    let round = client_id / subset_count;
    let subset_id = client_id % subset_count;

    let mut indices: Vec<usize> = (0..n).collect();
    let mut rng = StdRng::seed_from_u64(round as u64);
    indices.shuffle(&mut rng);

    let start = subset_id * k;
    indices[start..start + k].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_is_reproducible() {
        let a = assign_subset(SubsetPolicyKind::Deterministic, 20, 3, 3);
        let b = assign_subset(SubsetPolicyKind::Deterministic, 20, 3, 3);
        assert_eq!(a, b);
    }

    #[test]
    fn deterministic_round_zero_clients_are_disjoint() {
        let k = 3;
        let n = 20;
        let subset_count = n / k;
        let mut seen = std::collections::HashSet::new();
        for client_id in 0..subset_count {
            let subset = assign_subset(SubsetPolicyKind::Deterministic, n, client_id, k as u32);
            assert_eq!(subset.len(), k);
            for idx in subset {
                assert!(seen.insert(idx), "duplicate backend {idx} for client {client_id}");
            }
        }
        assert_eq!(seen.len(), subset_count * k);
    }

    #[test]
    fn deterministic_clients_in_same_round_share_shuffle() {
        let k = 3;
        let n = 20;
        let subset_count = n / k;
        let round_one_first = subset_count;
        let round_one_second = subset_count + 1;
        let first = assign_subset(
            SubsetPolicyKind::Deterministic,
            n,
            round_one_first,
            k as u32,
        );
        let second = assign_subset(
            SubsetPolicyKind::Deterministic,
            n,
            round_one_second,
            k as u32,
        );
        assert_ne!(first, second);
        assert!(first.iter().all(|idx| !second.contains(idx)));
    }

    #[test]
    fn deterministic_reference_fixture() {
        let mut expected = Vec::new();
        for client_id in 0..10 {
            expected.push(assign_subset(SubsetPolicyKind::Deterministic, 20, client_id, 3));
        }
        for (client_id, want) in expected.iter().enumerate() {
            let got = assign_subset(SubsetPolicyKind::Deterministic, 20, client_id, 3);
            assert_eq!(&got, want, "client {client_id}");
        }
    }

    #[test]
    fn subset_size_zero_returns_all() {
        assert_eq!(
            assign_subset(SubsetPolicyKind::Deterministic, 10, 0, 0),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn k_ge_n_returns_all() {
        assert_eq!(
            assign_subset(SubsetPolicyKind::Deterministic, 5, 7, 10),
            (0..5).collect::<Vec<_>>()
        );
    }

    #[test]
    fn leftover_backends_excluded() {
        let k = 3;
        let n = 20;
        let subset_count = n / k;
        let mut seen = std::collections::HashSet::new();
        for client_id in 0..subset_count {
            for idx in assign_subset(SubsetPolicyKind::Deterministic, n, client_id, k as u32) {
                seen.insert(idx);
            }
        }
        assert_eq!(seen.len(), subset_count * k);
        assert_eq!(seen.len(), 18);
    }

    #[test]
    fn random_subset_has_expected_length() {
        crate::rng::enter_run(Some(42));
        let subset = assign_subset(SubsetPolicyKind::Random, 20, 0, 3);
        crate::rng::exit_run();
        assert_eq!(subset.len(), 3);
        assert!(subset.iter().all(|&i| i < 20));
        assert_eq!(subset.iter().collect::<std::collections::HashSet<_>>().len(), 3);
    }
}
