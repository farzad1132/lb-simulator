use clap::ValueEnum;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum SchedulingPolicyKind {
    #[default]
    Fifo,
    Edf,
}

/// Returns the index at which a new upstream hop should be inserted in a mixed queue.
/// `is_upstream[i]` is true when queue position `i` holds an upstream item; paired
/// deadlines are only read for upstream positions.
pub fn edf_insert_index_in_mixed_queue(
    items: impl Iterator<Item = (bool, MonotonicTime)>,
    new_deadline: MonotonicTime,
) -> usize {
    let mut index = 0usize;
    for (is_upstream, deadline) in items {
        if is_upstream && deadline > new_deadline {
            return index;
        }
        index += 1;
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn t(ms: u64) -> MonotonicTime {
        MonotonicTime::EPOCH + Duration::from_millis(ms)
    }

    #[test]
    fn edf_inserts_before_first_strictly_later_upstream() {
        let items = [
            (true, t(120)),
            (true, t(200)),
            (false, t(0)),
        ];
        let idx = edf_insert_index_in_mixed_queue(items.into_iter(), t(150));
        assert_eq!(idx, 1);
    }

    #[test]
    fn edf_appends_when_no_later_upstream() {
        let items = [(true, t(100)), (true, t(120))];
        let idx = edf_insert_index_in_mixed_queue(items.into_iter(), t(200));
        assert_eq!(idx, 2);
    }

    #[test]
    fn edf_skips_returns_when_scanning() {
        let items = [
            (true, t(100)),
            (false, t(0)),
            (true, t(180)),
        ];
        let idx = edf_insert_index_in_mixed_queue(items.into_iter(), t(150));
        assert_eq!(idx, 2);
    }

    #[test]
    fn edf_equal_deadline_inserts_after_existing() {
        let items = [(true, t(120)), (true, t(200))];
        let idx = edf_insert_index_in_mixed_queue(items.into_iter(), t(120));
        assert_eq!(idx, 1);
    }
}
