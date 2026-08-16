use std::time::Duration;

use rand::Rng as _;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RetryPolicy {
    Never,
    Fixed {
        delay: Duration,
    },
    Exponential {
        base_delay: Duration,
        factor: u32,
        max_delay: Duration,
    },
}

impl RetryPolicy {
    pub fn delay_for(self, attempt: u16) -> Option<Duration> {
        match self {
            Self::Never => None,
            Self::Fixed { delay } => Some(delay),
            Self::Exponential {
                base_delay,
                factor,
                max_delay,
            } => {
                let multiplier = factor.saturating_pow(u32::from(attempt.saturating_sub(1)));
                let ceiling = base_delay.saturating_mul(multiplier).min(max_delay);
                let ceiling_nanos = u64::try_from(ceiling.as_nanos()).unwrap_or(u64::MAX);
                Some(Duration::from_nanos(rand::rng().random_range(0..=ceiling_nanos)))
            }
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::Exponential {
            base_delay: Duration::from_secs(1),
            factor: 2,
            max_delay: Duration::from_mins(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RetryPolicy;

    #[test]
    fn policies_calculate_bounded_delays() {
        assert_eq!(RetryPolicy::Never.delay_for(1), None);
        assert_eq!(
            RetryPolicy::Fixed {
                delay: Duration::from_secs(3)
            }
            .delay_for(10),
            Some(Duration::from_secs(3))
        );
        let exponential = RetryPolicy::Exponential {
            base_delay: Duration::from_secs(2),
            factor: 3,
            max_delay: Duration::from_secs(20),
        };
        for _ in 0..100 {
            assert!(exponential.delay_for(1).unwrap() <= Duration::from_secs(2));
            assert!(exponential.delay_for(2).unwrap() <= Duration::from_secs(6));
            assert!(exponential.delay_for(10).unwrap() <= Duration::from_secs(20));
        }
    }
}
