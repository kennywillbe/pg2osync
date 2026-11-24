//! The lease a running pipeline keeps in the target, and how a command that
//! must not run beside one reads it.
//!
//! A pipeline with nothing to stream leaves no trace anywhere else: MySQL does
//! not list it, a PostgreSQL slot is inactive between reconnects, and the
//! checkpoint only moves when something is written. So `run` refreshes a lease
//! per stream, and `reindex` and `switch-alias` watch whether it changes. Only
//! the change is evidence — the recorded time is never compared with a clock,
//! so two hosts disagreeing about the time cannot make a dead pipeline look
//! alive, and a killed pipeline's lease, which never changes again, never
//! blocks anything.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use pg2osync_core::checkpoint::{Lease, StreamId};
use pg2osync_core::sink::Sink;

/// How often a running pipeline refreshes its lease.
pub(crate) const REFRESH: Duration = Duration::from_secs(5);

/// Shortens [`REFRESH`] for both the pipeline and the reader, so the e2e
/// suites do not spend minutes waiting. Not documented as configuration: a
/// longer interval only makes every check slower, and a reader still waits
/// for the interval the holder recorded.
const INTERVAL_ENV: &str = "PG2OSYNC_LEASE_INTERVAL_MS";

/// The longest a reader waits, whatever a lease records: a document holding a
/// nonsense interval must not hang a command for hours.
const LONGEST_WAIT: Duration = Duration::from_secs(60);

/// A clean shutdown waits at most this long to clear the lease. A target that
/// does not answer must not hold the process up; the lease it leaves behind
/// stops moving, which is all a reader asks of a stopped pipeline.
const CLEAR_TIMEOUT: Duration = Duration::from_secs(5);

/// Failures are logged once and then at most this often, so a target that is
/// down for an hour does not write seven hundred identical warnings.
const WARN_EVERY: Duration = Duration::from_secs(60);

/// The refresh interval this process uses.
pub(crate) fn interval() -> Duration {
    std::env::var(INTERVAL_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map_or(REFRESH, Duration::from_millis)
}

/// How long a reader waits between its two reads for a holder refreshing
/// every `interval`: two refreshes and a margin, so one refresh that is late
/// or fails is not taken for a stopped pipeline. Twelve seconds at the
/// default.
pub(crate) fn wait_for(interval: Duration) -> Duration {
    (interval * 12 / 5).min(LONGEST_WAIT)
}

/// The wait after reading `first`: the interval its holder recorded, or this
/// process's own when there is no lease to say.
pub(crate) fn wait_after(first: Option<&Lease>) -> Duration {
    let interval = first
        .map(|l| Duration::from_millis(l.interval_ms))
        .filter(|i| !i.is_zero())
        .unwrap_or_else(interval);
    wait_for(interval)
}

/// A refreshing lease, held for as long as one stream's pipeline runs.
pub(crate) struct Holder {
    sink: Arc<dyn Sink>,
    stream: StreamId,
    task: tokio::task::JoinHandle<()>,
}

/// Start refreshing `stream`'s lease every `every`, the first time at once.
///
/// `sink` should be one of its own: a sink shared with the replication path
/// can queue the refresh behind a batch — the PostgreSQL target holds one
/// connection behind a mutex — and a refresh stuck behind a long batch would
/// make a busy pipeline look stopped.
pub(crate) fn hold(sink: Arc<dyn Sink>, stream: StreamId, every: Duration) -> Holder {
    let lease = Lease {
        stream: stream.clone(),
        run: run_token(),
        counter: 0,
        interval_ms: u64::try_from(every.as_millis()).unwrap_or(u64::MAX),
        holder: holder_name(),
        refreshed_at_epoch: 0,
    };
    let task = tokio::spawn(refresh(sink.clone(), lease, every));
    Holder { sink, stream, task }
}

impl Holder {
    /// Stop refreshing, then clear the lease, so a stopped pipeline costs a
    /// reader nothing but the wait.
    ///
    /// The order matters: a refresh landing after the clear would leave a
    /// lease behind. One already in flight when the task stops can still do
    /// that, and it is harmless — the lease it leaves never moves again.
    pub(crate) async fn release(mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
        match tokio::time::timeout(CLEAR_TIMEOUT, self.sink.clear_lease(&self.stream)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(target: "pg2osync::lease",
                "the lease was not cleared ({e}); it stops moving, so it blocks nothing"),
            Err(_) => tracing::warn!(target: "pg2osync::lease",
                "the target did not answer the lease clear in {}s; it stops moving, so it \
                 blocks nothing", CLEAR_TIMEOUT.as_secs()),
        }
    }
}

/// A dropped `JoinHandle` detaches its task rather than stopping it, so a
/// pipeline that unwinds past [`Holder::release`] — a panic in its source
/// task, which the supervisor survives to keep the other sources running —
/// would leave the refresher raising the counter of a stream nothing runs,
/// and every reindex of it refused until the whole process restarts. The
/// lease itself stays behind, still: clearing it needs the target, and a
/// lease that stops moving is all a reader asks of a halted pipeline.
impl Drop for Holder {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn refresh(sink: Arc<dyn Sink>, mut lease: Lease, every: Duration) {
    let mut ticker = tokio::time::interval(every);
    // a refresh that overran is followed by one interval, not a burst
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Each attempt is cut off well inside the interval, so a target that
    // hangs costs one refresh rather than all the ones a reader waits for.
    let attempt = every * 4 / 5;
    let mut last_warned: Option<Instant> = None;
    loop {
        ticker.tick().await;
        lease.counter += 1;
        lease.refreshed_at_epoch = epoch_seconds();
        let outcome = match tokio::time::timeout(attempt, sink.write_lease(&lease)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(format!("no answer in {}ms", attempt.as_millis())),
        };
        match outcome {
            Ok(()) => {
                if last_warned.take().is_some() {
                    tracing::info!(target: "pg2osync::lease", "the lease is refreshing again");
                }
            }
            Err(why) => {
                if last_warned.is_none_or(|at| at.elapsed() >= WARN_EVERY) {
                    tracing::warn!(target: "pg2osync::lease",
                        "cannot refresh the lease for {}: {why}. Replication is unaffected; \
                         until it refreshes, reindex and switch-alias cannot see this pipeline \
                         running", describe(&lease.stream));
                    last_warned = Some(Instant::now());
                }
            }
        }
    }
}

/// Refuse when the lease moved between `earlier` and `later`.
pub(crate) fn refusal(
    stream: &StreamId,
    earlier: Option<&Lease>,
    later: Option<&Lease>,
    during: &str,
) -> Option<String> {
    if !Lease::moved(earlier, later) {
        return None;
    }
    let lease = later?;
    Some(format!(
        "the lease of {} was refreshed {during} (by {}, refresh {}), so a pipeline is still \
         running this stream. Stop the pipeline and run this again; a pipeline that was killed \
         stops refreshing and does not hold this up",
        describe(stream),
        if lease.holder.is_empty() {
            "an instance that did not name itself"
        } else {
            lease.holder.as_str()
        },
        lease.counter
    ))
}

/// Read the lease, wait for its holder to refresh twice, read it again, and
/// refuse if it moved.
pub(crate) async fn refuse_a_running_pipeline(sink: &dyn Sink, stream: &StreamId) -> Result<()> {
    let first = sink.read_lease(stream).await?;
    tokio::time::sleep(wait_after(first.as_ref())).await;
    let second = sink.read_lease(stream).await?;
    if let Some(refusal) = refusal(
        stream,
        first.as_ref(),
        second.as_ref(),
        "while this was checking",
    ) {
        bail!(refusal);
    }
    Ok(())
}

/// The stream as an operator would recognise it in their config.
pub(crate) fn describe(stream: &StreamId) -> String {
    match stream.source.as_str() {
        pg2osync_core::checkpoint::SOURCE_MYSQL => {
            format!("the MySQL stream with server_id {}", stream.stream)
        }
        _ => format!("the PostgreSQL stream on slot {}", stream.stream),
    }
}

/// What `status` prints for a stream's lease.
pub(crate) fn status_line(lease: Option<&Lease>) -> String {
    match lease {
        Some(l) => format!(
            "lease: held by {}, refresh {} every {}ms, last at epoch {} by its own clock \
             (a pipeline that stopped without clearing it leaves it here unchanged)",
            if l.holder.is_empty() {
                "an unnamed instance"
            } else {
                l.holder.as_str()
            },
            l.counter,
            l.interval_ms,
            l.refreshed_at_epoch
        ),
        None => "lease: none (no pipeline has refreshed it since the last clean stop)".into(),
    }
}

/// Unique to this process and to this pipeline in it, so a restart — which
/// counts from one again — never reads as the same lease.
fn run_token() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{:x}-{nanos:x}-{seq}", std::process::id())
}

/// Who the lease names: the instance id the deployment sets for the
/// checkpoint, else the host, and the process either way.
fn holder_name() -> String {
    let name = ["PG2OSYNC_INSTANCE_ID", "HOSTNAME"]
        .iter()
        .find_map(|k| std::env::var(k).ok().filter(|v| !v.trim().is_empty()));
    match name {
        Some(name) => format!("{name} pid {}", std::process::id()),
        None => format!("pid {}", std::process::id()),
    }
}

fn epoch_seconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use pg2osync_core::checkpoint::Checkpoint;
    use pg2osync_core::error::CoreError;
    use pg2osync_core::sink::{Health, IndexSpec, LsnOp, SinkAck};
    use serde_json::Value;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    /// Keeps the lease and nothing else; can be told to fail or to hang.
    #[derive(Default)]
    pub(crate) struct LeaseOnly {
        pub(crate) lease: Mutex<Option<Lease>>,
        failures_left: AtomicU32,
        hang: AtomicBool,
        writes: AtomicU32,
    }

    #[async_trait::async_trait]
    impl Sink for LeaseOnly {
        async fn ensure_ready(&self, _: &[IndexSpec]) -> Result<(), CoreError> {
            Ok(())
        }
        async fn get_documents(
            &self,
            _: &str,
            _: &[(String, Option<String>)],
        ) -> Result<Vec<Option<Value>>, CoreError> {
            Ok(Vec::new())
        }
        async fn write(&self, _: Vec<LsnOp>) -> Result<SinkAck, CoreError> {
            Ok(SinkAck::written(None))
        }
        async fn truncate_index(
            &self,
            _: &str,
            _: Option<u64>,
            _: Option<(&str, &str)>,
        ) -> Result<(), CoreError> {
            Ok(())
        }
        async fn refresh(&self, _: &[String]) -> Result<(), CoreError> {
            Ok(())
        }
        async fn write_checkpoint(&self, _: &Checkpoint) -> Result<(), CoreError> {
            Ok(())
        }
        async fn read_checkpoint(&self, _: &StreamId) -> Result<Option<Checkpoint>, CoreError> {
            Ok(None)
        }
        async fn write_lease(&self, lease: &Lease) -> Result<(), CoreError> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            if self.hang.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            let mut left = self.failures_left.load(Ordering::SeqCst);
            while left > 0 {
                match self.failures_left.compare_exchange(
                    left,
                    left - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => return Err(CoreError::Sink("target unavailable".into())),
                    Err(now) => left = now,
                }
            }
            *self.lease.lock().unwrap() = Some(lease.clone());
            Ok(())
        }
        async fn read_lease(&self, _: &StreamId) -> Result<Option<Lease>, CoreError> {
            Ok(self.lease.lock().unwrap().clone())
        }
        async fn clear_lease(&self, _: &StreamId) -> Result<(), CoreError> {
            if self.hang.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            *self.lease.lock().unwrap() = None;
            Ok(())
        }
        async fn health(&self) -> Result<Health, CoreError> {
            Ok(Health::Up)
        }
    }

    fn stream() -> StreamId {
        StreamId {
            source: pg2osync_core::checkpoint::SOURCE_MYSQL.into(),
            stream: "990001".into(),
            publication: String::new(),
        }
    }

    const TICK: Duration = Duration::from_millis(50);

    /// Waits for `holds` rather than for a fixed time, so a loaded machine
    /// makes the test slower and never wrong.
    async fn eventually(what: &str, holds: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !holds() {
            assert!(Instant::now() < deadline, "never happened: {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn counter(sink: &LeaseOnly) -> Option<u64> {
        sink.lease.lock().unwrap().as_ref().map(|l| l.counter)
    }

    #[tokio::test]
    async fn the_counter_rises_from_the_first_refresh_on() {
        let sink = Arc::new(LeaseOnly::default());
        let holder = hold(sink.clone(), stream(), Duration::from_secs(3600));
        // an hour's interval, so only the refresh at once can explain it
        eventually("the first refresh, at once", || counter(&sink) == Some(1)).await;
        holder.release().await;

        let holder = hold(sink.clone(), stream(), TICK);
        eventually("the counter rising", || {
            counter(&sink).is_some_and(|c| c >= 3)
        })
        .await;
        let lease = sink.lease.lock().unwrap().clone().unwrap();
        assert_eq!(lease.interval_ms, 50);
        assert_eq!(lease.stream, stream());
        holder.release().await;
    }

    #[tokio::test]
    async fn a_failed_refresh_is_retried_rather_than_ending_the_lease() {
        let sink = Arc::new(LeaseOnly::default());
        sink.failures_left.store(3, Ordering::SeqCst);
        let holder = hold(sink.clone(), stream(), TICK);
        // the first three were refused, so the fourth attempt is the first
        // that lands, and it carries the counter the failures advanced
        eventually("a refresh after the failures", || counter(&sink).is_some()).await;
        assert!(
            counter(&sink).is_some_and(|c| c >= 4),
            "{:?}",
            counter(&sink)
        );
        holder.release().await;
    }

    #[tokio::test]
    async fn a_hanging_target_costs_one_refresh_not_all_of_them() {
        let sink = Arc::new(LeaseOnly::default());
        sink.hang.store(true, Ordering::SeqCst);
        let holder = hold(sink.clone(), stream(), TICK);
        eventually("attempts after a hung one", || {
            sink.writes.load(Ordering::SeqCst) >= 3
        })
        .await;
        let started = Instant::now();
        holder.release().await;
        assert!(
            started.elapsed() <= CLEAR_TIMEOUT + Duration::from_secs(2),
            "a hung target held the shutdown up for {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_clean_stop_clears_the_lease_and_stops_refreshing() {
        let sink = Arc::new(LeaseOnly::default());
        let holder = hold(sink.clone(), stream(), TICK);
        eventually("a refresh", || counter(&sink).is_some()).await;
        holder.release().await;
        assert_eq!(counter(&sink), None);
        let writes = sink.writes.load(Ordering::SeqCst);
        tokio::time::sleep(TICK * 3).await;
        assert_eq!(sink.writes.load(Ordering::SeqCst), writes);
        assert_eq!(counter(&sink), None);
    }

    #[tokio::test]
    async fn a_running_pipeline_is_refused_and_a_killed_one_is_not() {
        let sink = Arc::new(LeaseOnly::default());
        let holder = hold(sink.clone(), stream(), TICK);
        eventually("a refresh", || counter(&sink).is_some()).await;
        let refused = refuse_a_running_pipeline(sink.as_ref(), &stream())
            .await
            .expect_err("a refreshing lease is a running pipeline")
            .to_string();
        assert!(refused.contains("server_id 990001"), "{refused}");
        assert!(refused.contains("Stop the pipeline"), "{refused}");
        // what a SIGKILL leaves: the lease, no longer refreshed
        let mut holder = holder;
        holder.task.abort();
        let _ = (&mut holder.task).await;
        assert!(counter(&sink).is_some());
        refuse_a_running_pipeline(sink.as_ref(), &stream())
            .await
            .expect("a lease nobody refreshes is no evidence");
    }

    #[tokio::test]
    async fn a_pipeline_that_panics_stops_refreshing_its_lease() {
        let sink = Arc::new(LeaseOnly::default());
        let held = sink.clone();
        let pipeline = tokio::spawn(async move {
            let _holder = hold(held.clone(), stream(), TICK);
            eventually("a refresh", || counter(&held).is_some()).await;
            panic!("a bug in the source task");
        });
        assert!(pipeline.await.unwrap_err().is_panic());
        // a refresh in flight when the task was aborted may still land
        tokio::time::sleep(TICK * 2).await;
        let writes = sink.writes.load(Ordering::SeqCst);
        tokio::time::sleep(TICK * 4).await;
        assert_eq!(
            sink.writes.load(Ordering::SeqCst),
            writes,
            "the refresher outlived the pipeline that panicked"
        );
        assert!(counter(&sink).is_some(), "a halted pipeline's lease stays");
        refuse_a_running_pipeline(sink.as_ref(), &stream())
            .await
            .expect("the lease of a pipeline that panicked is no evidence");
    }

    #[tokio::test]
    async fn a_reader_waits_for_the_interval_the_holder_recorded() {
        let lease = Lease {
            stream: stream(),
            run: "r".into(),
            counter: 1,
            interval_ms: 1_000,
            holder: String::new(),
            refreshed_at_epoch: 0,
        };
        assert_eq!(wait_after(Some(&lease)), Duration::from_millis(2_400));
        let absurd = Lease {
            interval_ms: u64::MAX,
            ..lease
        };
        assert_eq!(wait_after(Some(&absurd)), LONGEST_WAIT);
        assert_eq!(wait_for(REFRESH), Duration::from_secs(12));
    }

    #[test]
    fn a_refusal_names_the_stream_and_who_holds_it() {
        let held = Lease {
            stream: stream(),
            run: "r".into(),
            counter: 9,
            interval_ms: 5_000,
            holder: "pod-a pid 7".into(),
            refreshed_at_epoch: 0,
        };
        let pg = StreamId {
            source: pg2osync_core::checkpoint::SOURCE_POSTGRES.into(),
            stream: "users_slot".into(),
            publication: "p".into(),
        };
        let text = refusal(&pg, None, Some(&held), "while this was checking").unwrap();
        assert!(text.contains("slot users_slot"), "{text}");
        assert!(text.contains("pod-a pid 7"), "{text}");
        let unnamed = Lease {
            holder: String::new(),
            ..held.clone()
        };
        let text = refusal(&pg, None, Some(&unnamed), "x").unwrap();
        assert!(text.contains("did not name itself"), "{text}");
        assert_eq!(refusal(&pg, Some(&held), Some(&held), "x"), None);
    }
}
