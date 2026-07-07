use clap::ValueEnum;
use nexosim::time::MonotonicTime;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum SchedulingPolicyKind {
    #[default]
    Fifo,
    Edf,
}

/// Returns the index at which a new queue item should be inserted by deadline.
/// Scans front to back; inserts before the first item with a strictly later deadline.
/// Equal deadlines insert after existing ties (FIFO among ties).
pub fn edf_insert_index(
    deadlines: impl Iterator<Item = MonotonicTime>,
    new_deadline: MonotonicTime,
) -> usize {
    let mut index = 0usize;
    for deadline in deadlines {
        if deadline > new_deadline {
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
    fn edf_inserts_before_first_strictly_later_deadline() {
        let items = [t(120), t(200), t(0)];
        let idx = edf_insert_index(items.into_iter(), t(150));
        assert_eq!(idx, 1);
    }

    #[test]
    fn edf_appends_when_no_later_deadline() {
        let items = [t(100), t(120)];
        let idx = edf_insert_index(items.into_iter(), t(200));
        assert_eq!(idx, 2);
    }

    #[test]
    fn edf_return_inserts_before_later_deadline() {
        let items = [t(120), t(180)];
        let idx = edf_insert_index(items.into_iter(), t(150));
        assert_eq!(idx, 1);
    }

    #[test]
    fn edf_mixed_queue_orders_by_deadline() {
        let items = [t(100), t(160), t(180)];
        let idx = edf_insert_index(items.into_iter(), t(150));
        assert_eq!(idx, 1);
    }

    #[test]
    fn edf_equal_deadline_inserts_after_existing() {
        let items = [t(120), t(200)];
        let idx = edf_insert_index(items.into_iter(), t(120));
        assert_eq!(idx, 1);
    }
}
