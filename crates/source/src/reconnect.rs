//! When to try the source again, and how long to wait first.
//!
//! Pure decisions only, so the rules can be tested without a database. The
//! caller owns the counting and the sleeping.

use std::time::Duration;

/// Backoff is capped so a long outage settles into a steady retry rhythm
/// instead of drifting into hours.
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy)]
pub struct ReconnectPolicy {
    /// Consecutive failures tolerated before giving up. Zero disables
    /// reconnecting, which is what the tool did before it could reconnect.
    pub max_attempts: u32,
    pub base_backoff_ms: u64,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base_backoff_ms: 1000,
        }
    }
}

impl ReconnectPolicy {
    /// Whether a failure this far into a streak is worth another try.
    pub fn should_retry(&self, consecutive_failures: u32) -> bool {
        consecutive_failures < self.max_attempts
    }

    /// How long to wait before the attempt that follows `consecutive_failures`.
    ///
    /// Doubles per failure, mirroring the sink's retry policy so the two parts
    /// of the pipeline behave the same way under pressure.
    pub fn delay_for(&self, consecutive_failures: u32) -> Duration {
        let doubled = self
            .base_backoff_ms
            .saturating_mul(2u64.saturating_pow(consecutive_failures.min(16)));
        Duration::from_millis(doubled).min(MAX_BACKOFF)
    }

    /// Whether an attempt that lasted this long counts as having recovered.
    ///
    /// Without this a connection that works for hours and then drops would
    /// accumulate toward the limit exactly like a crash loop does. Outliving
    /// the backoff cap is the evidence that the source was genuinely healthy.
    pub fn attempt_recovered(&self, streamed_for: Duration) -> bool {
        streamed_for >= MAX_BACKOFF
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_then_stops_growing() {
        let policy = ReconnectPolicy {
            max_attempts: 10,
            base_backoff_ms: 1000,
        };
        assert_eq!(policy.delay_for(0), Duration::from_secs(1));
        assert_eq!(policy.delay_for(1), Duration::from_secs(2));
        assert_eq!(policy.delay_for(2), Duration::from_secs(4));
        assert_eq!(policy.delay_for(20), MAX_BACKOFF, "capped, never unbounded");
    }

    #[test]
    fn a_huge_base_cannot_overflow_into_a_short_delay() {
        let policy = ReconnectPolicy {
            max_attempts: 10,
            base_backoff_ms: u64::MAX,
        };
        assert_eq!(policy.delay_for(5), MAX_BACKOFF);
    }

    #[test]
    fn the_limit_is_the_number_of_failures_tolerated() {
        let policy = ReconnectPolicy {
            max_attempts: 3,
            base_backoff_ms: 10,
        };
        assert!(policy.should_retry(0));
        assert!(policy.should_retry(2), "the third try is still allowed");
        assert!(!policy.should_retry(3), "the fourth is not");
    }

    #[test]
    fn zero_attempts_means_exit_on_the_first_failure() {
        let policy = ReconnectPolicy {
            max_attempts: 0,
            base_backoff_ms: 1000,
        };
        assert!(!policy.should_retry(0));
    }

    #[test]
    fn only_a_long_lived_attempt_clears_the_streak() {
        let policy = ReconnectPolicy::default();
        assert!(!policy.attempt_recovered(Duration::from_secs(2)));
        assert!(policy.attempt_recovered(Duration::from_secs(60)));
    }
}
