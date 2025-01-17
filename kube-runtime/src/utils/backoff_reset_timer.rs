use std::time::{Duration, Instant};

pub trait Backoff: Iterator<Item = Duration> + Send + Sync + Unpin {
    /// Resets the internal state to the initial value.
    fn reset(&mut self);
}

impl<B: Backoff + ?Sized> Backoff for Box<B> {
    fn reset(&mut self) {
        let this: &mut B = self;
        this.reset()
    }
}

/// A [`Backoff`] wrapper that resets after a fixed duration has elapsed.
pub struct ResetTimerBackoff<B: Backoff> {
    backoff: B,
    last_backoff: Option<Instant>,
    reset_duration: Duration,
}

impl<B: Backoff> ResetTimerBackoff<B> {
    pub fn new(backoff: B, reset_duration: Duration) -> Self {
        Self {
            backoff,
            last_backoff: None,
            reset_duration,
        }
    }
}

impl<B: Backoff> Iterator for ResetTimerBackoff<B> {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        if let Some(last_backoff) = self.last_backoff {
            if tokio::time::Instant::now().into_std() > last_backoff + self.reset_duration {
                tracing::debug!(
                    ?last_backoff,
                    reset_duration = ?self.reset_duration,
                    "Resetting backoff, since reset duration has expired"
                );
                self.backoff.reset();
            }
        }
        self.last_backoff = Some(tokio::time::Instant::now().into_std());
        self.backoff.next()
    }
}

impl<B: Backoff> Backoff for ResetTimerBackoff<B> {
    fn reset(&mut self) {
        self.backoff.reset();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::advance;

    use super::ResetTimerBackoff;
    use crate::utils::stream_backoff::tests::LinearBackoff;
    use std::time::Duration;

    #[tokio::test]
    async fn should_reset_when_timer_expires() {
        tokio::time::pause();
        let mut backoff = ResetTimerBackoff::new(
            LinearBackoff::new(Duration::from_secs(2)),
            Duration::from_secs(60),
        );
        assert_eq!(backoff.next(), Some(Duration::from_secs(2)));
        advance(Duration::from_secs(40)).await;
        assert_eq!(backoff.next(), Some(Duration::from_secs(4)));
        advance(Duration::from_secs(40)).await;
        assert_eq!(backoff.next(), Some(Duration::from_secs(6)));
        advance(Duration::from_secs(80)).await;
        assert_eq!(backoff.next(), Some(Duration::from_secs(2)));
        advance(Duration::from_secs(80)).await;
        assert_eq!(backoff.next(), Some(Duration::from_secs(2)));
    }
}
