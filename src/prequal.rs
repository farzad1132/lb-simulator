use serde::{Deserialize, Serialize};

use crate::rng;

/// Probes issued per request (async, off the critical path).
pub const R_PROBE: usize = 2;
/// Max times a candidate may be selected before removal from the pool.
pub const B_REUSE: u32 = 1;
/// Fractional worst-candidate removal rate per request.
pub const R_REMOVE: f64 = 0.3;
/// Pool capacity as a fraction of the server count.
pub const POOL_FRAC: f64 = 0.25;

pub fn pool_cap(n_servers: usize) -> usize {
    ((POOL_FRAC * n_servers as f64).ceil() as usize).max(1)
}

/// Probe request from a load balancer to a server.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Probe {
    pub sender_id: usize,
}

/// Probe reply carrying server-local RIF (`queue.len + in_flight`).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProbeReply {
    pub server_idx: usize,
    pub rif: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub server_idx: usize,
    pub rif: u32,
    pub uses: u32,
}

/// Per-LB candidate pool ordered oldest → newest.
#[derive(Clone, Debug, Default)]
pub struct CandidatePool {
    entries: Vec<Candidate>,
    cap: usize,
}

impl CandidatePool {
    pub fn new(cap: usize) -> Self {
        Self {
            entries: Vec::new(),
            cap: cap.max(1),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains(&self, server_idx: usize) -> bool {
        self.entries.iter().any(|e| e.server_idx == server_idx)
    }

    pub fn server_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.entries.iter().map(|e| e.server_idx)
    }

    /// Remove the highest-RIF candidate; ties break to oldest.
    pub fn remove_worst(&mut self) -> Option<Candidate> {
        if self.entries.is_empty() {
            return None;
        }
        let mut worst_i = 0;
        for i in 1..self.entries.len() {
            if self.entries[i].rif > self.entries[worst_i].rif {
                worst_i = i;
            }
        }
        Some(self.entries.remove(worst_i))
    }

    /// Select least-RIF candidate; ties break to oldest. Does not mutate.
    pub fn select_best(&self) -> Option<usize> {
        let first = self.entries.first()?;
        let mut best_i = 0;
        let mut best_rif = first.rif;
        for (i, e) in self.entries.iter().enumerate().skip(1) {
            if e.rif < best_rif {
                best_i = i;
                best_rif = e.rif;
            }
        }
        Some(self.entries[best_i].server_idx)
    }

    /// After dispatching to `server_idx` if it is in the pool: optimistic RIF++,
    /// increment uses, and remove when `uses >= b_reuse`.
    pub fn after_dispatch(&mut self, server_idx: usize, b_reuse: u32) {
        let Some(pos) = self.entries.iter().position(|e| e.server_idx == server_idx) else {
            return;
        };
        self.entries[pos].rif = self.entries[pos].rif.saturating_add(1);
        self.entries[pos].uses = self.entries[pos].uses.saturating_add(1);
        if self.entries[pos].uses >= b_reuse {
            self.entries.remove(pos);
        }
    }

    /// Insert or refresh a probe reply. Refresh resets uses and moves to newest.
    /// On insert at capacity, evict oldest first.
    pub fn ingest_reply(&mut self, server_idx: usize, rif: u32) {
        if let Some(pos) = self.entries.iter().position(|e| e.server_idx == server_idx) {
            self.entries.remove(pos);
            self.entries.push(Candidate {
                server_idx,
                rif,
                uses: 0,
            });
            return;
        }
        if self.entries.len() >= self.cap {
            self.entries.remove(0);
        }
        self.entries.push(Candidate {
            server_idx,
            rif,
            uses: 0,
        });
    }

    #[cfg(test)]
    pub fn entries(&self) -> &[Candidate] {
        &self.entries
    }
}

/// Apply fractional `r_remove`: accumulate and remove worst while debt >= 1.
pub fn apply_r_remove(pool: &mut CandidatePool, accum: &mut f64, r_remove: f64) {
    *accum += r_remove;
    while *accum >= 1.0 && !pool.is_empty() {
        pool.remove_worst();
        *accum -= 1.0;
    }
}

/// Sample up to `r_probe` servers uniformly without replacement from those not in the pool.
pub fn sample_probe_targets(n_servers: usize, pool: &CandidatePool, r_probe: usize) -> Vec<usize> {
    if n_servers == 0 || r_probe == 0 {
        return Vec::new();
    }
    let mut candidates: Vec<usize> = (0..n_servers).filter(|&i| !pool.contains(i)).collect();
    if candidates.is_empty() {
        return Vec::new();
    }
    rng::shuffle(&mut candidates);
    let take = r_probe.min(candidates.len());
    candidates.truncate(take);
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_cap_ceil_fraction() {
        assert_eq!(pool_cap(1), 1);
        assert_eq!(pool_cap(3), 1); // ceil(0.75) = 1
        assert_eq!(pool_cap(4), 1); // ceil(1.0) = 1
        assert_eq!(pool_cap(5), 2); // ceil(1.25) = 2
        assert_eq!(pool_cap(100), 25);
    }

    #[test]
    fn remove_worst_prefers_highest_rif_then_oldest() {
        let mut pool = CandidatePool::new(4);
        pool.ingest_reply(0, 5);
        pool.ingest_reply(1, 9);
        pool.ingest_reply(2, 9);
        let removed = pool.remove_worst().unwrap();
        // oldest among rif=9 is server 1
        assert_eq!(removed.server_idx, 1);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn select_best_least_rif_oldest_tie() {
        let mut pool = CandidatePool::new(4);
        pool.ingest_reply(3, 2);
        pool.ingest_reply(1, 2);
        pool.ingest_reply(0, 5);
        assert_eq!(pool.select_best(), Some(3));
    }

    #[test]
    fn after_dispatch_increments_and_evicts_on_reuse() {
        let mut pool = CandidatePool::new(4);
        pool.ingest_reply(2, 1);
        pool.after_dispatch(2, 2);
        assert_eq!(pool.entries()[0].rif, 2);
        assert_eq!(pool.entries()[0].uses, 1);
        pool.after_dispatch(2, 2);
        assert!(pool.is_empty());
    }

    #[test]
    fn ingest_at_cap_evicts_oldest() {
        let mut pool = CandidatePool::new(2);
        pool.ingest_reply(0, 1);
        pool.ingest_reply(1, 2);
        pool.ingest_reply(2, 3);
        assert_eq!(pool.len(), 2);
        assert!(!pool.contains(0));
        assert!(pool.contains(1));
        assert!(pool.contains(2));
    }

    #[test]
    fn ingest_duplicate_refreshes_and_marks_newest() {
        let mut pool = CandidatePool::new(2);
        pool.ingest_reply(0, 1);
        pool.ingest_reply(1, 2);
        pool.after_dispatch(0, 10);
        pool.ingest_reply(0, 7);
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.entries()[1].server_idx, 0);
        assert_eq!(pool.entries()[1].rif, 7);
        assert_eq!(pool.entries()[1].uses, 0);
        // oldest is server 1; capacity insert evicts it
        pool.ingest_reply(2, 0);
        assert!(!pool.contains(1));
        assert!(pool.contains(0));
        assert!(pool.contains(2));
    }

    #[test]
    fn apply_r_remove_fractional() {
        let mut pool = CandidatePool::new(4);
        pool.ingest_reply(0, 1);
        pool.ingest_reply(1, 9);
        let mut accum = 0.0;
        apply_r_remove(&mut pool, &mut accum, 0.3);
        assert_eq!(pool.len(), 2);
        assert!((accum - 0.3).abs() < 1e-9);
        apply_r_remove(&mut pool, &mut accum, 0.3);
        apply_r_remove(&mut pool, &mut accum, 0.3);
        apply_r_remove(&mut pool, &mut accum, 0.3);
        // 1.2 >= 1 → one removal
        assert_eq!(pool.len(), 1);
        assert!(!pool.contains(1));
        assert!((accum - 0.2).abs() < 1e-9);
    }

    #[test]
    fn sample_probe_targets_excludes_pool() {
        crate::rng::enter_run(Some(1));
        let mut pool = CandidatePool::new(4);
        pool.ingest_reply(0, 1);
        pool.ingest_reply(2, 1);
        let targets = sample_probe_targets(4, &pool, 2);
        assert_eq!(targets.len(), 2);
        assert!(!targets.contains(&0));
        assert!(!targets.contains(&2));
        crate::rng::exit_run();
    }
}
