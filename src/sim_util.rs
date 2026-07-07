use nexosim::simulation::{EventId, SchedulingError, Simulation};
use std::time::Duration;

const MIN_DURATION_SECS: f32 = 1e-9;

pub fn schedule_initial_pulls(
    sim: &Simulation,
    pull_inputs: &[EventId<()>],
    concurrency: u32,
) -> Result<(), SchedulingError> {
    let scheduler = sim.scheduler();
    let delay = Duration::from_secs_f32(MIN_DURATION_SECS);
    for pull_input in pull_inputs {
        for _ in 0..concurrency {
            scheduler.schedule_event(delay, pull_input, ())?;
        }
    }
    Ok(())
}
