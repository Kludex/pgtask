//! Property tests for retry backoff.
//!
//! `delay_for` decides how long a failed task waits before it is claimed again.
//! It is pure apart from the jitter, and the jitter is exactly what makes
//! example-based tests weak here: a single call proves nothing about the bound.
//!
//! Case count follows `PROPTEST_CASES`.

use std::time::Duration;

use pgtask_core::RetryPolicy;
use proptest::prelude::*;

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(512)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: cases(),
        ..ProptestConfig::default()
    }
}

/// Exponential policies the database would accept: a positive factor and a
/// ceiling at least as large as the base, matching the CHECK constraint on
/// `pgtask.handler_policies`.
fn storable_exponential() -> impl Strategy<Value = RetryPolicy> {
    (0_u64..3_600_000, 1_u32..16, 0_u64..86_400_000).prop_map(|(base, factor, extra)| RetryPolicy::Exponential {
        base_delay: Duration::from_millis(base),
        factor,
        max_delay: Duration::from_millis(base + extra),
    })
}

proptest! {
    #![proptest_config(config())]

    /// The ceiling is the promise the policy makes. Jitter may shorten a delay,
    /// never lengthen it past `max_delay`.
    #[test]
    fn an_exponential_delay_never_exceeds_its_ceiling(
        policy in storable_exponential(),
        attempt in 0_u16..2_000,
    ) {
        let RetryPolicy::Exponential { max_delay, .. } = policy else {
            unreachable!("the strategy only builds exponential policies")
        };
        let delay = policy.delay_for(attempt).expect("exponential always yields a delay");
        prop_assert!(
            delay <= max_delay,
            "attempt {attempt} waited {delay:?}, past the {max_delay:?} ceiling"
        );
    }

    /// Growth is bounded by the policy's own curve, not just by the ceiling, so
    /// an early attempt cannot wait as long as a late one.
    #[test]
    fn an_exponential_delay_stays_under_the_curve(
        base_millis in 1_u64..10_000,
        factor in 2_u32..8,
        attempt in 1_u16..8,
    ) {
        let policy = RetryPolicy::Exponential {
            base_delay: Duration::from_millis(base_millis),
            factor,
            // No effective ceiling, so this measures the curve rather than the clamp.
            max_delay: Duration::MAX,
        };
        // Derived here rather than read from the implementation.
        let expected_ceiling = Duration::from_millis(
            base_millis * u64::from(factor).pow(u32::from(attempt) - 1),
        );
        let delay = policy.delay_for(attempt).unwrap();
        prop_assert!(
            delay <= expected_ceiling,
            "attempt {attempt} waited {delay:?}, past the {expected_ceiling:?} the curve allows"
        );
    }

    /// A fixed policy is exactly that: the same wait every time, with no jitter.
    #[test]
    fn a_fixed_delay_is_constant(millis in 0_u64..86_400_000, attempt in 0_u16..2_000) {
        let delay = Duration::from_millis(millis);
        prop_assert_eq!(RetryPolicy::Fixed { delay }.delay_for(attempt), Some(delay));
    }

    /// `Never` means never, at any attempt.
    #[test]
    fn never_never_retries(attempt in 0_u16..u16::MAX) {
        prop_assert_eq!(RetryPolicy::Never.delay_for(attempt), None);
    }

    /// Attempt numbers come from the database, so the whole `u16` range has to
    /// be survivable — including the saturating edges.
    #[test]
    fn delays_are_computable_for_any_attempt(
        policy in storable_exponential(),
        attempt in any::<u16>(),
    ) {
        let _ = policy.delay_for(attempt);
    }
}

/// The two edges the range strategies above will essentially never generate.
#[test]
fn the_extreme_attempt_numbers_are_survivable() {
    let policy = RetryPolicy::default();
    assert!(policy.delay_for(0).is_some(), "attempt 0 must not panic");
    assert!(policy.delay_for(u16::MAX).is_some(), "the last attempt must not panic");

    let saturating = RetryPolicy::Exponential {
        base_delay: Duration::from_secs(u64::MAX / 2),
        factor: u32::MAX,
        max_delay: Duration::MAX,
    };
    assert!(
        saturating.delay_for(u16::MAX).is_some(),
        "a policy that overflows every multiplication must saturate, not panic"
    );
}
