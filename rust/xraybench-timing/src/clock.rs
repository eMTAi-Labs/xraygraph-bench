use std::sync::atomic::{fence, Ordering};
use xraybench_types::Result;

// ── macOS implementation ──────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
mod platform {
    use mach2::mach_time::{mach_absolute_time, mach_timebase_info};
    use std::sync::OnceLock;

    static TIMEBASE: OnceLock<(u32, u32)> = OnceLock::new();

    fn timebase() -> (u32, u32) {
        *TIMEBASE.get_or_init(|| {
            let mut info = mach_timebase_info { numer: 0, denom: 0 };
            unsafe { mach_timebase_info(&mut info) };
            (info.numer, info.denom)
        })
    }

    pub fn now_ns() -> super::Result<u64> {
        let ticks = unsafe { mach_absolute_time() };
        let (numer, denom) = timebase();
        // Use u128 intermediate to avoid overflow
        let ns = (ticks as u128 * numer as u128) / denom as u128;
        Ok(ns as u64)
    }
}

// ── Linux implementation ──────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
mod platform {
    use libc::{clock_gettime, timespec, CLOCK_MONOTONIC_RAW};

    pub fn now_ns() -> super::Result<u64> {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let ret = unsafe { clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts) };
        if ret != 0 {
            return Err(super::BenchError::ClockUnavailable(
                "clock_gettime(CLOCK_MONOTONIC_RAW) failed".to_string(),
            ));
        }
        let ns = ts.tv_sec as u64 * 1_000_000_000u64 + ts.tv_nsec as u64;
        Ok(ns)
    }
}

// ── Fallback implementation ───────────────────────────────────────────────────

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod platform {
    use std::sync::OnceLock;
    use std::time::Instant;

    static EPOCH: OnceLock<Instant> = OnceLock::new();

    pub fn now_ns() -> super::Result<u64> {
        let epoch = EPOCH.get_or_init(Instant::now);
        let elapsed = epoch.elapsed();
        Ok(elapsed.as_nanos() as u64)
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Returns the current monotonic time in nanoseconds.
pub fn monotonic_ns() -> Result<u64> {
    platform::now_ns()
}

/// A timing fence: prevents compiler/CPU reordering around measurement points.
#[inline]
pub fn timing_fence() {
    fence(Ordering::SeqCst);
    std::hint::black_box(());
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_returns_nonzero() {
        let t = monotonic_ns().expect("clock should work");
        assert!(t > 0, "clock returned zero");
    }

    #[test]
    fn clock_is_monotonic() {
        let t1 = monotonic_ns().expect("clock t1");
        let t2 = monotonic_ns().expect("clock t2");
        assert!(t2 >= t1, "clock went backwards: t1={t1}, t2={t2}");
    }

    #[test]
    fn clock_advances() {
        let t1 = monotonic_ns().expect("clock t1");
        // Spin for 10K iterations to guarantee some time passes
        let mut _sum: u64 = 0;
        for i in 0..10_000u64 {
            _sum = _sum.wrapping_add(i);
        }
        std::hint::black_box(_sum);
        let t2 = monotonic_ns().expect("clock t2");
        assert!(t2 > t1, "clock did not advance: t1={t1}, t2={t2}");
    }

    #[test]
    fn fence_does_not_panic() {
        timing_fence();
        timing_fence();
        timing_fence();
    }

    #[test]
    fn clock_reasonable_magnitude() {
        let t = monotonic_ns().expect("clock");
        // Should be > 1 billion ns (1 second since system epoch / boot)
        assert!(
            t > 1_000_000_000,
            "clock magnitude unreasonably small: {t}"
        );
    }
}
