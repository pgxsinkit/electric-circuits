//! Retirement completion: a shape stream the engine has removed from its records gets deleted,
//! **eventually**, whatever storage was doing at the moment it decided to.
//!
//! ADR-0007 fixes the shape of a retirement — close the stream, then delete it — but not its
//! durability. Before this module every retirement site was `retire_stream(...)` with a `warn!` on
//! failure, and the shape record was already gone by then: a 503 on the close/delete left the public
//! stream URL open forever, serving rows Postgres no longer contains, with nothing anywhere that
//! remembered it should not exist. "The engine forgot the shape but the world still has its stream"
//! is not a state a restart repaired either — the restore only walks records that still exist.
//!
//! So retirement is written down in two halves (see [`CatalogEvent::Retired`]):
//!
//! 1. **intent** — `Dropped { id }`, written BEFORE the retirement is attempted, at every site;
//! 2. **completion** — `Retired { id }`, written only once storage has accepted the delete.
//!
//! and the gap between them is closed by the queue below: every failed retirement is enqueued and
//! retried with backoff until it lands, and every boot enqueues each `Dropped` the catalog fold
//! could not match with a `Retired`. Losing the in-memory queue at exit costs nothing — the intent
//! is durable, so the next boot picks the work up.

use super::*;

/// The retry schedule for one retirement: 500 ms, doubling, capped at 5 s. Pure, so the schedule is
/// a unit test rather than a comment. Slower off the mark than the catalog writer's: nobody is
/// waiting on a retirement, and the shape it belonged to is already gone.
pub(crate) fn retire_backoff(attempt: u32) -> std::time::Duration {
    let step = attempt.saturating_sub(1).min(4);
    std::time::Duration::from_millis(500u64.saturating_mul(1u64 << step)).min(std::time::Duration::from_secs(5))
}

/// One outstanding retirement, as the worker's queue holds it. What it completes lives in the
/// queue's shared [`Outstanding`] entry for the path, not here, so a repeat enqueue of the same path
/// can add to it without a second copy of the work.
struct Retirement {
    stream_path: String,
    attempt: u32,
}

/// What a pending retirement of one stream path owes when it lands.
#[derive(Default)]
struct Outstanding {
    /// The shape whose `Dropped` this completes, if any — a change-log segment or a rolled-back
    /// create's stream has no record to close out. A repeat enqueue that names one fills it in.
    shape_id: Option<String>,
    completions: Vec<Arc<RetirementCompletion>>,
}

pub(crate) struct RetirementCompletion {
    done: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

impl RetirementCompletion {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self { done: std::sync::atomic::AtomicBool::new(false), notify: tokio::sync::Notify::new() })
    }

    fn complete(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// The engine's background retirement queue (see the module docs). Cheap to clone: a sender plus the
/// shared set of outstanding paths.
#[derive(Clone)]
pub(crate) struct RetirementQueue {
    tx: mpsc::UnboundedSender<Retirement>,
    /// Every stream path queued or in flight, and what its completion owes. One entry per PATH: the
    /// same stream enqueued again while it is still outstanding — every boot attempt re-enqueues
    /// the catalog's unmatched `Dropped` records, and a boot that retries through a storage outage
    /// makes many attempts — joins the entry instead of queueing the work twice, so the gauge
    /// counts streams rather than attempts and `Retired` is written once. Its size IS the gauge.
    outstanding: Arc<std::sync::Mutex<HashMap<String, Outstanding>>>,
}

impl RetirementQueue {
    /// Hand a stream to the queue. Infallible by design, exactly like [`CatalogWriter::send`]: a
    /// dead queue means the process is going away, and the durable `Dropped` means the next boot
    /// will find this work again.
    pub(crate) fn enqueue(&self, stream_path: &str, shape_id: Option<&str>) {
        self.enqueue_with_completion(stream_path, shape_id, None);
    }

    pub(crate) fn enqueue_with_completion(
        &self,
        stream_path: &str,
        shape_id: Option<&str>,
        completion: Option<Arc<RetirementCompletion>>,
    ) {
        let mut outstanding = self.outstanding.lock().unwrap();
        let fresh = !outstanding.contains_key(stream_path);
        let entry = outstanding.entry(stream_path.to_string()).or_default();
        if entry.shape_id.is_none() {
            entry.shape_id = shape_id.map(str::to_string);
        }
        entry.completions.extend(completion);
        if fresh && self.tx.send(Retirement { stream_path: stream_path.to_string(), attempt: 0 }).is_err() {
            outstanding.remove(stream_path);
        }
        if !fresh {
            tracing::debug!("retirement of {stream_path} is already outstanding; joined it");
        }
        crate::metrics::metrics().retirements_pending.store(outstanding.len() as u64, Ordering::Relaxed);
    }

    /// Retirements enqueued and not yet completed, one per stream (the `retirements_pending` gauge).
    pub(crate) fn pending(&self) -> u64 {
        self.outstanding.lock().unwrap().len() as u64
    }
}

/// Spawn the single retirement worker.
///
/// One consumer, and a failed head goes to the BACK of the queue rather than blocking it: every
/// entry targets the same durable-streams server, so an outage stalls them all equally, but a single
/// stream that storage answers badly for must not starve the rest forever.
pub(crate) fn spawn_retirement_queue(
    ds: DsClient,
    catalog_tx: CatalogWriter,
    shutdown: crate::shutdown::ShutdownToken,
) -> RetirementQueue {
    let (tx, mut rx) = mpsc::unbounded_channel::<Retirement>();
    let outstanding: Arc<std::sync::Mutex<HashMap<String, Outstanding>>> = Arc::default();
    let owed = outstanding.clone();
    tokio::spawn(async move {
        let mut queue: std::collections::VecDeque<Retirement> = std::collections::VecDeque::new();
        loop {
            if queue.is_empty() {
                match rx.recv().await {
                    Some(item) => queue.push_back(item),
                    None => return, // engine gone
                }
            }
            while let Ok(item) = rx.try_recv() {
                queue.push_back(item);
            }
            // A terminating process is not a lost retirement: the `Dropped` intent is durable, so
            // the next boot enqueues whatever is left here. Nothing waits on this task, which is
            // why it registers no shutdown party.
            if shutdown.is_shutting_down() {
                tracing::info!(
                    "shutdown: leaving {} pending stream retirement(s) to the next boot (their \
                     `Dropped` records are durable)",
                    queue.len()
                );
                return;
            }
            let mut item = queue.pop_front().expect("non-empty above");
            match ds.retire_stream(&item.stream_path).await {
                Ok(()) => {
                    // Taken out of the shared set and settled under the same lock, so an enqueue of
                    // this path either joined the entry before it was taken (and is settled with it)
                    // or finds it gone and queues a retirement of its own.
                    let done = {
                        let mut outstanding = owed.lock().unwrap();
                        let done = outstanding.remove(&item.stream_path).unwrap_or_default();
                        crate::metrics::metrics()
                            .retirements_pending
                            .store(outstanding.len() as u64, Ordering::Relaxed);
                        // Only now: `Retired` means "storage accepted the delete", and the whole
                        // point of the record is that it can be trusted at the next boot.
                        if let Some(id) = &done.shape_id {
                            catalog_tx.send(CatalogEvent::Retired { id: id.clone() });
                        }
                        done
                    };
                    if item.attempt > 0 {
                        tracing::info!("retired stream {} after {} retr(ies)", item.stream_path, item.attempt);
                    }
                    for completion in done.completions {
                        completion.complete();
                    }
                }
                Err(e) => {
                    item.attempt += 1;
                    crate::metrics::metrics().retirement_retries.fetch_add(1, Ordering::Relaxed);
                    let base = retire_backoff(item.attempt);
                    // Once per entry per outage escalation, not once per attempt: the ceiling is
                    // reached in a few seconds and the loop can then run for hours.
                    if item.attempt == 1 || item.attempt.is_multiple_of(20) {
                        tracing::warn!(
                            "retiring stream {} failed (attempt {}), retrying: {e:#}",
                            item.stream_path,
                            item.attempt
                        );
                    }
                    queue.push_back(item);
                    tokio::select! {
                        _ = shutdown.wait() => {}
                        _ = tokio::time::sleep(crate::replication::jitter(base, crate::replication::clock_nanos())) => {}
                    }
                }
            }
        }
    });
    RetirementQueue { tx, outstanding }
}

impl Engine {
    /// Retire one shape's stream and write the completion (ADR-0007): close, delete, `Retired`.
    ///
    /// The single implementation behind every engine-initiated shape removal — purge, eviction,
    /// schema drift / `TRUNCATE`, the epoch reset, the catalog restore's drops. Never fails, never
    /// forgets: a storage failure goes to the background queue, which retries it to completion and
    /// writes the record then. Callers must have written `Dropped { id }` FIRST — that is the
    /// durable intent a crash in here is recovered from.
    pub(crate) async fn retire_shape_stream(&self, id: &str, stream_path: &str) {
        match self.ds.retire_stream(stream_path).await {
            Ok(()) => self.catalog_tx.send(CatalogEvent::Retired { id: id.to_string() }),
            Err(e) => {
                tracing::warn!(
                    "retiring stream {stream_path} for shape {id} failed ({e:#}); queued — the shape \
                     is gone from the engine either way, and its stream must not outlive it"
                );
                self.retirements.enqueue(stream_path, Some(id));
            }
        }
    }

    pub(crate) async fn retire_shape_stream_with_completion(
        &self,
        id: &str,
        stream_path: &str,
        completion: Arc<RetirementCompletion>,
    ) {
        match self.ds.retire_stream(stream_path).await {
            Ok(()) => {
                self.catalog_tx.send(CatalogEvent::Retired { id: id.to_string() });
                completion.complete();
            }
            Err(e) => {
                tracing::warn!("retiring stream {stream_path} for shape {id} failed ({e:#}); queued and awaited");
                self.retirements.enqueue_with_completion(stream_path, Some(id), Some(completion));
            }
        }
    }

    /// Retirements enqueued and not yet completed (introspection + the `retirements_pending` gauge).
    pub fn pending_retirements(&self) -> u64 {
        self.retirements.pending()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog::testing::FakeDs;

    #[test]
    fn the_retirement_schedule_climbs_to_five_seconds_and_stays() {
        let ms = |a: u32| retire_backoff(a).as_millis();
        assert_eq!(ms(1), 500, "the first retry is prompt");
        assert_eq!(ms(2), 1000);
        assert_eq!(ms(3), 2000);
        assert_eq!(ms(4), 4000);
        assert_eq!(ms(5), 5000, "capped");
        assert_eq!(ms(40), 5000, "and stays capped");
    }

    /// The whole point of the queue: a retirement storage refuses is retried until it lands, and the
    /// `Retired` record is written only then.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failing_retirement_is_retried_until_it_lands_and_records_only_then() {
        let server = FakeDs::start().await;
        server.fail_deletes(2);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let ds = DsClient::new(server.url());
        let catalog = spawn_catalog_writer(ds.clone(), shutdown.clone());
        let q = spawn_retirement_queue(ds, catalog.clone(), shutdown);
        q.enqueue("shape/s1", Some("s1"));
        assert_eq!(q.pending(), 1);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while q.pending() != 0 {
            assert!(std::time::Instant::now() < deadline, "the retirement never completed");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(server.deletes(), 3, "two refusals then the delete that landed");
        assert_eq!(server.closes(), server.deletes(), "ADR-0007: every attempt closes before it deletes");
        assert!(catalog.drain(std::time::Duration::from_secs(5)).await);
        assert_eq!(
            server.catalog_kinds(),
            vec!["retired".to_string()],
            "the completion is recorded once, and only after storage accepted the delete"
        );
    }

    /// Every boot attempt re-enqueues the catalog's unmatched `Dropped` records, and a boot that
    /// retries through a storage outage makes many attempts. The same stream enqueued again while
    /// it is still outstanding is ONE retirement: one gauge entry, one delete that lands, one
    /// `Retired` — and a completion attached by the repeat is still settled when it lands.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_path_already_outstanding_is_joined_not_queued_twice() {
        let server = FakeDs::start().await;
        server.fail_deletes(3);
        let shutdown = crate::shutdown::ShutdownToken::new();
        let ds = DsClient::new(server.url());
        let catalog = spawn_catalog_writer(ds.clone(), shutdown.clone());
        let q = spawn_retirement_queue(ds, catalog.clone(), shutdown);
        q.enqueue("shape/s1", None);
        let completion = RetirementCompletion::new();
        q.enqueue_with_completion("shape/s1", Some("s1"), Some(completion.clone()));
        q.enqueue("shape/s1", Some("s1"));
        assert_eq!(q.pending(), 1, "one stream outstanding, however many times it was enqueued");

        tokio::time::timeout(std::time::Duration::from_secs(20), completion.wait())
            .await
            .expect("the joined completion is settled when the retirement lands");
        assert_eq!(q.pending(), 0);
        assert_eq!(server.deletes(), 4, "three refusals, then the one delete that landed — no second copy");
        assert!(catalog.drain(std::time::Duration::from_secs(5)).await);
        assert_eq!(
            server.catalog_kinds(),
            vec!["retired".to_string()],
            "the shape id a repeat supplied is recorded, once"
        );
    }

    /// A retirement with no shape behind it (a rolled-back create's stream) still gets deleted; it
    /// simply has no `Dropped` to close out.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_anonymous_retirement_writes_no_record() {
        let server = FakeDs::start().await;
        let shutdown = crate::shutdown::ShutdownToken::new();
        let ds = DsClient::new(server.url());
        let catalog = spawn_catalog_writer(ds.clone(), shutdown.clone());
        let q = spawn_retirement_queue(ds, catalog.clone(), shutdown);
        q.enqueue("shape/s9", None);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while q.pending() != 0 {
            assert!(std::time::Instant::now() < deadline, "the retirement never completed");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(server.deletes(), 1);
        assert!(catalog.drain(std::time::Duration::from_secs(5)).await);
        assert!(server.catalog_kinds().is_empty(), "nothing to record");
    }
}
