use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone, Default)]
pub struct LoadRegistry(Arc<Vec<AtomicU32>>);

impl LoadRegistry {
    pub fn new(n: usize) -> Self {
        Self(Arc::new((0..n).map(|_| AtomicU32::new(0)).collect()))
    }

    pub fn get(&self, idx: usize) -> u32 {
        self.0
            .get(idx)
            .map(|load| load.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn set(&self, idx: usize, load: u32) {
        if let Some(slot) = self.0.get(idx) {
            slot.store(load, Ordering::Relaxed);
        }
    }
}
