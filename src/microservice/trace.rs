use nexosim::time::MonotonicTime;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct MsTracer {
    limit: u32,
    next_id: AtomicU64,
    write_lock: Mutex<()>,
}

impl MsTracer {
    pub fn new(limit: u32) -> Arc<Self> {
        Arc::new(Self {
            limit,
            next_id: AtomicU64::new(0),
            write_lock: Mutex::new(()),
        })
    }

    pub fn next_request_id(&self) -> (u64, bool) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        (id, id <= u64::from(self.limit))
    }

    pub fn log(&self, should_log: bool, t: MonotonicTime, req_id: u64, msg: &str) {
        if !should_log {
            return;
        }
        let _guard = self.write_lock.lock().unwrap();
        let mut stderr = io::stderr().lock();
        let _ = writeln!(
            stderr,
            "[t={:.6}s] req={} {}",
            t.duration_since(MonotonicTime::EPOCH).as_secs_f64(),
            req_id,
            msg
        );
    }
}
