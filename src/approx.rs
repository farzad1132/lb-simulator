use serde::{Deserialize, Serialize};

/// Pull intent sent from a client balancer to a server.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PullIntent {
    /// Originating balancer id (`lb_id` in `lb`, `rb_id` in `ms`).
    pub sender_id: usize,
    /// Bound request id for the queued item this intent will pull.
    pub request_id: u64,
}

/// Pull request sent from a server back to a client balancer.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PullRequest {
    pub server_idx: usize,
    /// Bound request id for approx pulls; `None` for centralized warm-start pulls.
    pub request_id: Option<u64>,
}

pub fn fatal_pull_abort(simulator: &str, details: impl std::fmt::Display) -> ! {
    eprintln!("FATAL approx pull abort ({simulator}): {details}");
    panic!("approx pull abort ({simulator}): {details}");
}
