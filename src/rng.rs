use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::cell::RefCell;

thread_local! {
    static RUN_RNG: RefCell<Option<StdRng>> = const { RefCell::new(None) };
}

pub fn enter_run(seed: Option<u64>) {
    RUN_RNG.with(|slot| {
        *slot.borrow_mut() = Some(match seed {
            Some(s) => StdRng::seed_from_u64(s),
            None => StdRng::from_rng(&mut rand::rng()),
        });
    });
}

pub fn exit_run() {
    RUN_RNG.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

pub fn with_rng<R>(f: impl FnOnce(&mut StdRng) -> R) -> R {
    if RUN_RNG.with(|slot| slot.borrow().is_some()) {
        RUN_RNG.with(|slot| f(slot.borrow_mut().as_mut().expect("run rng missing")))
    } else {
        let mut tmp = StdRng::from_rng(&mut rand::rng());
        f(&mut tmp)
    }
}

pub fn random_usize_range(range: std::ops::Range<usize>) -> usize {
    with_rng(|rng| rng.random_range(range))
}

pub fn shuffle<T>(slice: &mut [T]) {
    with_rng(|rng| slice.shuffle(rng));
}
