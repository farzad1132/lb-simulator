use nexosim::time::MonotonicTime;

#[derive(Clone, Debug, Default)]
pub struct OccupancyAccumulator {
    last_sample_time: Option<MonotonicTime>,
    last_level: u32,
    integral: f64,
}

impl OccupancyAccumulator {
    pub fn record(&mut self, now: MonotonicTime, level: u32) {
        if let Some(t0) = self.last_sample_time {
            let dt = now.duration_since(t0).as_secs_f64();
            self.integral += f64::from(self.last_level) * dt;
        }
        self.last_sample_time = Some(now);
        self.last_level = level;
    }

    pub fn finalize(&mut self, end: MonotonicTime, sim_start: MonotonicTime) -> f64 {
        let start = self.last_sample_time.unwrap_or(sim_start);
        let level = if self.last_sample_time.is_some() {
            self.last_level
        } else {
            0
        };
        let dt = end.duration_since(start).as_secs_f64();
        self.integral += f64::from(level) * dt;

        let obs = end.duration_since(sim_start);
        let obs_secs = obs.as_secs_f64();
        if obs_secs <= 0.0 {
            return 0.0;
        }
        self.integral / obs_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn t(secs: f64) -> MonotonicTime {
        MonotonicTime::EPOCH + Duration::from_secs_f64(secs)
    }

    #[test]
    fn constant_level_averages_to_level() {
        let mut acc = OccupancyAccumulator::default();
        acc.record(t(0.0), 2);
        acc.record(t(10.0), 2);
        let avg = acc.finalize(t(10.0), t(0.0));
        assert!((avg - 2.0).abs() < 1e-9);
    }

    #[test]
    fn level_change_mid_run() {
        let mut acc = OccupancyAccumulator::default();
        acc.record(t(0.0), 0);
        acc.record(t(5.0), 4);
        acc.record(t(10.0), 4);
        let avg = acc.finalize(t(10.0), t(0.0));
        // 5s at 0 + 5s at 4 = 20 / 10 = 2
        assert!((avg - 2.0).abs() < 1e-9);
    }

    #[test]
    fn idle_before_first_sample_counts_as_zero() {
        let mut acc = OccupancyAccumulator::default();
        acc.record(t(5.0), 3);
        let avg = acc.finalize(t(10.0), t(0.0));
        // 5s at 0 + 5s at 3 = 15 / 10 = 1.5
        assert!((avg - 1.5).abs() < 1e-9);
    }
}
