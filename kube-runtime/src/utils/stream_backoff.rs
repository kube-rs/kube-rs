use std::{future::Future, pin::Pin, task::Poll};

use futures::{Stream, TryStream};
use pin_project::pin_project;
use tokio::time::{sleep, Instant, Sleep};

use crate::utils::Backoff;

/// Applies a [`Backoff`] policy to a [`Stream`]
///
/// After any [`Err`] is emitted, the stream is paused for [`Backoff::next_backoff`]. The
/// [`Backoff`] is [`reset`](`Backoff::reset`) on any [`Ok`] value.
///
/// If [`Backoff::next_backoff`] returns [`None`] then the backing stream is given up on, and closed.
#[pin_project]
pub struct StreamBackoff<S, B> {
    #[pin]
    stream: S,
    backoff: B,
    #[pin]
    state: State,
}

#[pin_project(project = StreamBackoffStateProj)]
// It's expected to have relatively few but long-lived `StreamBackoff`s in a project, so we would rather have
// cheaper sleeps than a smaller `StreamBackoff`.
#[allow(clippy::large_enum_variant)]
enum State {
    BackingOff(#[pin] Sleep),
    GivenUp,
    Awake,
}

impl<S: TryStream, B: Backoff> StreamBackoff<S, B> {
    pub fn new(stream: S, backoff: B) -> Self {
        Self {
            stream,
            backoff,
            state: State::Awake,
        }
    }
}

impl<S: TryStream, B: Backoff> Stream for StreamBackoff<S, B> {
    type Item = Result<S::Ok, S::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.state.as_mut().project() {
            StreamBackoffStateProj::BackingOff(mut backoff_sleep) => match backoff_sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    tracing::debug!(deadline = ?backoff_sleep.deadline(), "Backoff complete, waking up");
                    this.state.set(State::Awake)
                }
                Poll::Pending => {
                    let deadline = backoff_sleep.deadline();
                    tracing::trace!(
                        ?deadline,
                        remaining_duration = ?deadline.saturating_duration_since(Instant::now()),
                        "Still waiting for backoff sleep to complete"
                    );
                    return Poll::Pending;
                }
            },
            StreamBackoffStateProj::GivenUp => {
                tracing::debug!("Backoff has given up, stream is closed");
                return Poll::Ready(None);
            }
            StreamBackoffStateProj::Awake => {}
        }

        let next_item = this.stream.try_poll_next(cx);
        match &next_item {
            Poll::Ready(Some(Err(_))) => {
                if let Some(backoff_duration) = this.backoff.next() {
                    let backoff_sleep = sleep(backoff_duration);
                    tracing::debug!(
                        deadline = ?backoff_sleep.deadline(),
                        duration = ?backoff_duration,
                        "Error received, backing off"
                    );
                    this.state.set(State::BackingOff(backoff_sleep));
                } else {
                    tracing::debug!("Error received, giving up");
                    this.state.set(State::GivenUp);
                }
            }
            Poll::Ready(_) => {
                tracing::trace!("Non-error received, resetting backoff");
                this.backoff.reset();
            }
            Poll::Pending => {}
        }
        next_item
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{pin::pin, task::Poll, time::Duration};

    use crate::utils::Backoff;

    use super::StreamBackoff;
    use backon::BackoffBuilder;
    use futures::{channel::mpsc, poll, stream, StreamExt};

    pub struct ConstantBackoff {
        inner: backon::ConstantBackoff,
        delay: Duration,
        max_times: usize,
    }

    impl ConstantBackoff {
        pub fn new(delay: Duration, max_times: usize) -> Self {
            Self {
                inner: backon::ConstantBuilder::default()
                    .with_delay(delay)
                    .with_max_times(max_times)
                    .build(),
                delay,
                max_times,
            }
        }
    }

    impl Iterator for ConstantBackoff {
        type Item = Duration;

        fn next(&mut self) -> Option<Duration> {
            self.inner.next()
        }
    }

    impl Backoff for ConstantBackoff {
        fn reset(&mut self) {
            self.inner = backon::ConstantBuilder::default()
                .with_delay(self.delay)
                .with_max_times(self.max_times)
                .build();
        }
    }

    #[tokio::test]
    async fn stream_should_back_off() {
        tokio::time::pause();
        let tick = Duration::from_secs(1);
        let rx = stream::iter([Ok(0), Ok(1), Err(2), Ok(3), Ok(4)]);
        let mut rx = pin!(StreamBackoff::new(rx, ConstantBackoff::new(tick, 10)));
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(0))));
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(1))));
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Err(2))));
        assert_eq!(poll!(rx.next()), Poll::Pending);
        tokio::time::advance(tick * 2).await;
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(3))));
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(4))));
        assert_eq!(poll!(rx.next()), Poll::Ready(None));
    }

    #[tokio::test]
    async fn backoff_time_should_update() {
        tokio::time::pause();
        let (tx, rx) = mpsc::unbounded();
        // let rx = stream::iter([Ok(0), Ok(1), Err(2), Ok(3)]);
        let mut rx = pin!(StreamBackoff::new(rx, LinearBackoff::new(Duration::from_secs(2))));
        tx.unbounded_send(Ok(0)).unwrap();
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(0))));
        tx.unbounded_send(Ok(1)).unwrap();
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(1))));
        tx.unbounded_send(Err(2)).unwrap();
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Err(2))));
        assert_eq!(poll!(rx.next()), Poll::Pending);
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(poll!(rx.next()), Poll::Pending);
        tx.unbounded_send(Err(3)).unwrap();
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Err(3))));
        tx.unbounded_send(Ok(4)).unwrap();
        assert_eq!(poll!(rx.next()), Poll::Pending);
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(poll!(rx.next()), Poll::Pending);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(poll!(rx.next()), Poll::Ready(Some(Ok(4))));
        assert_eq!(poll!(rx.next()), Poll::Pending);
        drop(tx);
        assert_eq!(poll!(rx.next()), Poll::Ready(None));
    }

    #[tokio::test]
    async fn backoff_should_close_when_requested() {
        assert_eq!(
            StreamBackoff::new(stream::iter([Ok(0), Ok(1), Err(2), Ok(3)]), StoppedBackoff {})
                .collect::<Vec<_>>()
                .await,
            vec![Ok(0), Ok(1), Err(2)]
        );
    }

    struct StoppedBackoff;

    impl Backoff for StoppedBackoff {
        fn reset(&mut self) {}
    }

    impl Iterator for StoppedBackoff {
        type Item = Duration;

        fn next(&mut self) -> Option<Duration> {
            None
        }
    }

    /// Dynamic backoff policy that is still deterministic and testable
    pub struct LinearBackoff {
        interval: Duration,
        current_duration: Duration,
    }

    impl LinearBackoff {
        pub fn new(interval: Duration) -> Self {
            Self {
                interval,
                current_duration: Duration::ZERO,
            }
        }
    }

    impl Iterator for LinearBackoff {
        type Item = Duration;

        fn next(&mut self) -> Option<Duration> {
            self.current_duration += self.interval;
            Some(self.current_duration)
        }
    }

    impl Backoff for LinearBackoff {
        fn reset(&mut self) {
            self.current_duration = Duration::ZERO
        }
    }
}
