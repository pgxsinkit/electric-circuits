//! Minimal durable-streams HTTP client: PUT-create, POST-append (JSON array), and
//! offset-resumable reads (catch-up + long-poll live). Offsets are opaque tokens; we just
//! persist and replay `Stream-Next-Offset`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, bail};

use crate::heap_size::HeapSize;
use serde::{Deserialize, Serialize};

/// A State-Protocol change event, the JSON item on every table/shape stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub type_: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// The full prior row, carried by replication on UPDATE/DELETE (`REPLICA IDENTITY FULL`). Lets
    /// the engine compute the input delta without an in-memory `table_state`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old: Option<serde_json::Value>,
    pub headers: EnvelopeHeaders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeHeaders {
    pub operation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txid: Option<String>,
    // The server stamps an `offset` onto each item; accept it on read, never send it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<String>,
    /// Postgres commit LSN of the change (set by the replication ingestor). Used to skip changes a
    /// shape/family already reflects from its backfill snapshot (`lsn <= seed_lsn`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lsn: Option<String>,
    /// Position of this change within its transaction (set by the ingestor). `(lsn, seq)` uniquely
    /// identifies a change, letting the tailer skip duplicates when the ingestor re-appends a batch
    /// after a partial failure or a crash between append and slot-advance (at-least-once delivery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// **Transaction-end marker**: `Some(true)` on the LAST envelope of a transaction, and only
    /// there (ADR-0003).
    ///
    /// It is what keeps per-transaction atomic emission true on the wire now that a commit too
    /// large for one request body is appended in several chunks. Each append is exposed atomically
    /// by durable-streams, so without the marker the sequencer would see chunk 1 as a complete
    /// `(txid, lsn)` run, fan it out and flush it to shape streams, then do the same for chunks
    /// 2..N — a subscriber would observe a fraction of a transaction. With it, the sequencer HOLDS a
    /// trailing run whose last envelope is unmarked and processes the transaction only once the
    /// marker arrives.
    ///
    /// Every producer sets it: the ingestor on the last envelope of the last chunk (single-chunk
    /// commits included, so the rule is uniform), and library-mode writers on every envelope
    /// (one-envelope transactions). An envelope WITHOUT it that is not followed by one is an
    /// incomplete transaction, by definition — never a transaction that opted out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last: Option<bool>,
    /// **The schema this change was decoded under** — `SchemaFingerprint::digest` as 16 lowercase hex
    /// characters (ADR-0010).
    ///
    /// Set by the replication ingestor on every data envelope it appends to the change log in
    /// Postgres mode, and on nothing else: never on a shape-stream (output) envelope — subscribers
    /// read those, and the engine's own fence is no part of their contract — and never in library
    /// mode, where there is no fingerprint and no drift to fence.
    ///
    /// The sequencer is behind the ingestor, so it can reach an envelope whose schema a drift has
    /// already replaced (ADR-0005 swaps the compiled schema and retires that table's shapes). This is
    /// what tells the two apart: a stamp that matches the schema about to decode it means a decode
    /// failure is an ENGINE BUG and stops ingest; one that names a schema the drift replaced means the
    /// envelope's shapes are already gone and consuming it without decoding is correct. A string, not
    /// a `u64`, because the log is JSON and a number above 2^53 does not survive `JSON.parse`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl crate::heap_size::HeapSize for EnvelopeHeaders {
    fn heap_bytes(&self) -> usize {
        self.operation.heap_bytes()
            + self.txid.heap_bytes()
            + self.offset.heap_bytes()
            + self.lsn.heap_bytes()
            + self.schema.heap_bytes()
    }
}

impl crate::heap_size::HeapSize for Envelope {
    fn heap_bytes(&self) -> usize {
        self.type_.heap_bytes()
            + self.key.heap_bytes()
            + self.value.heap_bytes()
            + self.old.heap_bytes()
            + self.headers.heap_bytes()
    }
}

/// What one envelope costs to hold in memory: its inline representation plus the heap it owns.
///
/// This is the quantity `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` is measured in (ADR-0003), so the knob
/// counts what is actually held rather than what the same data would serialize to. It is a lower
/// bound in the same sense as every other [`crate::heap_size::HeapSize`] estimate (allocator
/// overhead and `serde_json::Map` bucket overhead are not modelled).
pub fn envelope_memory_bytes(env: &Envelope) -> u64 {
    (std::mem::size_of::<Envelope>() + env.heap_bytes()) as u64
}

pub struct ReadResult {
    pub envelopes: Vec<Envelope>,
    pub next_offset: Option<String>,
    pub up_to_date: bool,
    /// The server reported `stream-closed`: the stream is **terminal** — it will never grow again.
    /// For a shape stream that means the engine retired the shape (close-then-delete, see
    /// [`DsClient::retire_stream`]) and readers must stop, not re-poll: a closed stream answers a
    /// long-poll instantly, so looping on "empty page, same offset" would spin on the server. For a
    /// `changes/<n>` segment it means the log ROTATED (ADR-0006) and the reader follows the batch's
    /// rotation pointer onto the next segment.
    pub closed: bool,
}

/// What a `HEAD` found: the stream's tail offset and whether it is closed. `None` from
/// [`DsClient::head`] means the stream is not there (404) or soft-deleted (410).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamHead {
    pub next_offset: Option<String>,
    pub closed: bool,
}

/// A read hit a stream that is not there: deleted (404) or soft-deleted (410).
///
/// A typed error because callers make very different decisions about it than about a transient read
/// failure. On the change log (ADR-0006) it is never expected — a segment is deleted only once
/// nothing can resume inside it — so the sequencer treats it as an error to log loudly and back off
/// from, the boot treats it as fatal for the position it is about to resume, and a dormant shape's
/// replay treats it as "this shape can never be brought up to date", which evicts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGone {
    pub path: String,
    pub status: u16,
}

impl std::fmt::Display for StreamGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream '{}' is gone ({})", self.path, self.status)
    }
}

impl std::error::Error for StreamGone {}

/// Does this error (or anything it was contextualised from) mean "the stream is not there"?
pub fn is_stream_gone(e: &anyhow::Error) -> bool {
    stream_gone(e).is_some()
}

/// The [`StreamGone`] in this error's chain, if any — for a caller that must know WHICH stream is
/// not there (a change-log segment and a shape's own stream call for different answers).
pub fn stream_gone(e: &anyhow::Error) -> Option<&StreamGone> {
    e.chain().find_map(|c| c.downcast_ref::<StreamGone>())
}

/// The durable-streams server answered, but not with an answer: a 5xx (reachable and not serving) or
/// a 429 (reachable and asking to be asked later). Typed so callers (the boot, above all) can tell "storage is
/// having a moment" from "this request is wrong", which a status embedded in a message string cannot
/// express.
#[derive(Debug)]
pub struct DsUnavailable {
    pub op: &'static str,
    pub path: String,
    pub status: u16,
}

impl std::fmt::Display for DsUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} -> {} (durable-streams unavailable)", self.op, self.path, self.status)
    }
}

impl std::error::Error for DsUnavailable {}

/// Storage answered an append to a stream with a terminal status (404, 410, or 409 +
/// `stream-closed`) while its own `HEAD` finds that stream present and open — usually a proxy or a
/// router in front of it answering for a stream it does not know about.
///
/// Its own type rather than [`DsUnavailable`]: it is just as worth waiting out (the boot retries it,
/// `pg::boot_disposition`), but "durable-streams is unreachable" would send an operator looking at
/// the wrong thing when the server is answering perfectly well — inconsistently.
#[derive(Debug)]
pub struct DsInconsistent {
    pub path: String,
    pub status: u16,
}

impl std::fmt::Display for DsInconsistent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "POST {} -> {}, but HEAD finds the stream present and open (storage answers this stream inconsistently)",
            self.path, self.status
        )
    }
}

impl std::error::Error for DsInconsistent {}

/// Does this error (or anything it was contextualised from) carry [`DsInconsistent`]?
pub fn is_inconsistent(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.downcast_ref::<DsInconsistent>().is_some())
}

/// Is this a durable-streams failure that may clear on its own — the server not up yet, a refused
/// connection, a timeout, a connection dropped mid-response, a 5xx, or a 429?
///
/// This is the read the **boot** takes: a storage server that comes up after its engine is the
/// normal case in a compose/Kubernetes start, so it must back off rather than exit `EX_CONFIG`.
/// Deliberately narrow — it forgives the TRANSPORT and nothing else:
///
/// * `reqwest::Error::is_builder` (an unusable `ELECTRIC_CIRCUITS_DS_URL`) is **not** forgiven: no
///   amount of waiting reshapes a URL;
/// * `is_decode` (a body that is not what it claims) is **not** forgiven — that is a malformed
///   catalog, which is fatal by design;
/// * a [`StreamGone`] (404/410) is **not** forgiven: a stream that is not there is an answer, and
///   every caller has its own, very different response to it;
/// * a typed catalog strictness refusal carries no `reqwest::Error` at all, so it stays fatal.
pub fn is_unavailable(e: &anyhow::Error) -> bool {
    if e.chain().any(|c| c.downcast_ref::<StreamGone>().is_some()) {
        return false;
    }
    if e.chain().any(|c| c.downcast_ref::<DsUnavailable>().is_some()) {
        return true;
    }
    e.chain().filter_map(|c| c.downcast_ref::<reqwest::Error>()).any(|r| {
        !r.is_builder() && !r.is_decode() && (r.is_connect() || r.is_timeout() || r.is_request() || r.is_body())
    })
}

/// Build the error for a non-2xx durable-streams response: typed when the server said it is
/// unavailable, an ordinary message otherwise.
///
/// A 429 is unavailability, not a refusal: "too many requests" is the server saying the request is
/// fine and the timing is not. Read as a refusal it would exit the process on a catalog append
/// (`EXIT_CATALOG_REFUSED`) and refuse a boot whose stream checks merely arrived in a burst.
fn status_error(op: &'static str, path: &str, status: u16, body: &str) -> anyhow::Error {
    if (500..600).contains(&status) || status == 429 {
        return anyhow::Error::new(DsUnavailable { op, path: path.to_string(), status });
    }
    if body.is_empty() {
        anyhow::anyhow!("{op} {path} -> {status}")
    } else {
        anyhow::anyhow!("{op} {path} -> {status}: {body}")
    }
}

/// The outcome of an append that treats retirement as an answer rather than an error (see
/// [`DsClient::append_checked`]).
pub enum Appended {
    /// Landed; `next_offset` is the stream's tail afterwards (`stream-next-offset`).
    Ok { next_offset: Option<String> },
    /// The stream is retired — deleted (404), soft-deleted (410) or closed (409 + `stream-closed`).
    Retired(u16),
}

/// Why an append failed: the stream is retired — deleted (404), soft-deleted (410) or closed
/// (409 + `stream-closed`), all terminal, discard — or a transient/other error (retry or surface).
/// `Gone` carries the status so the log/error names which of the three it was.
enum AppendError {
    Gone(u16),
    Other(anyhow::Error),
}

/// What the engine decides about a shape stream whose append came back **terminal** (404, 410, or
/// 409 + `stream-closed`) — see [`DsClient::set_gone_reconciler`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoneVerdict {
    /// The engine does not hold this stream any more (retired, evicted, purged, or never a shape
    /// stream at all): discarding the batch is correct and complete.
    Discard,
    /// The shape is still registered AND storage still has its stream: the terminal answer was
    /// FALSE — a proxy, a router or a failover said 404 about a stream that is right there. Retry
    /// the append; the batch belongs to a live shape and dropping it is permanent divergence.
    Retry,
}

/// Reconcile a terminal-looking append answer against engine state. Installed once by the engine
/// (see `Engine::install_gone_reconciler`); absent in the tests/tools that use a bare `DsClient`,
/// where a terminal answer is taken at face value exactly as before.
pub type GoneReconciler = std::sync::Arc<
    dyn Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = GoneVerdict> + Send>> + Send + Sync,
>;

/// One response from the durable-stream protocol. Private and status-shaped on purpose: it lets
/// [`DsClient`] keep its exact error/retry behavior while the HTTP mechanics sit below the port. A
/// closed outcome vocabulary would replace it if a second store implementation ever needs one.
struct StoreResponse {
    status: u16,
    /// The response body is deliberately retained as an outcome rather than normalized to text.
    /// Successful stream reads must fail if their body cannot be acquired: accepting the advertised
    /// next offset with an empty page could checkpoint past envelopes that never reached Circuits.
    /// Write/control operations keep best-effort body handling.
    body: Option<std::result::Result<String, anyhow::Error>>,
    next_offset: Option<String>,
    up_to_date: bool,
    closed: bool,
}

impl StoreResponse {
    /// Best-effort response-body handling for write/control operations.
    fn body_or_default(self) -> String {
        self.body.and_then(std::result::Result::ok).unwrap_or_default()
    }

    /// A successful stream read is not a successful page until its body has been acquired.
    fn required_body(self) -> Result<String> {
        self.body.unwrap_or_else(|| Err(anyhow::anyhow!("provider omitted a successful stream response body")))
    }
}

#[derive(Clone, Copy)]
enum BodyRead {
    Never,
    OnFailure,
    OnData,
    Always,
}

type StoreFuture<'a> = Pin<Box<dyn Future<Output = Result<StoreResponse>> + Send + 'a>>;

/// The engine-owned, provider-neutral single-attempt Durable Streams port.
///
/// The contract intentionally accepts and returns opaque offset strings.  It owns no retry,
/// reconciliation, envelope codec, retirement, or byte-accounting policy: those are Circuits
/// invariants and remain on [`DsClient`].  It is private because no external crate is entitled to
/// rely on this status-shaped outcome representation.
trait DurableStreamStore: Send + Sync {
    fn ensure<'a>(&'a self, path: &'a str, content_type: &'a str) -> StoreFuture<'a>;
    fn append<'a>(
        &'a self,
        path: &'a str,
        content_type: &'a str,
        body: Vec<u8>,
        response_body: BodyRead,
    ) -> StoreFuture<'a>;
    fn read<'a>(&'a self, path: &'a str, offset: &'a str, live: bool) -> StoreFuture<'a>;
    fn head<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
    fn close<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
    fn delete<'a>(&'a self, path: &'a str) -> StoreFuture<'a>;
}

/// The currently pinned pgxsinkit/durable-streams-rust wire adapter.  It performs exactly one
/// HTTP request per port call; `DsClient` owns the interpretation and retry policy above it.
struct HttpDurableStreamsStore {
    base: String,
    http: reqwest::Client,
}

impl HttpDurableStreamsStore {
    fn new(base: String) -> Self {
        Self { base, http: reqwest::Client::new() }
    }

    fn stream_url(&self, path: &str) -> String {
        format!("{}/{}", self.base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    async fn response(res: reqwest::Response, body_read: BodyRead) -> StoreResponse {
        let status = res.status().as_u16();
        let next_offset = header(&res, "stream-next-offset");
        let up_to_date = res.headers().get("stream-up-to-date").is_some();
        let closed = header(&res, "stream-closed").is_some_and(|v| v.eq_ignore_ascii_case("true"));
        let should_read = match body_read {
            BodyRead::Never => false,
            BodyRead::OnFailure => !(200..300).contains(&status),
            // Existing read paths return before acquiring a 204 body.
            BodyRead::OnData => (200..300).contains(&status) && status != 204,
            BodyRead::Always => true,
        };
        // Keep acquisition fallible for the facade to interpret per operation.  In particular,
        // `read` and `read_json` used `res.text().await?` after a successful GET; turning an
        // interrupted body into `""` would manufacture an empty page at a real next offset.
        // Otherwise the selected mode keeps each operation's best-effort behavior.
        let body = if should_read { Some(res.text().await.map_err(anyhow::Error::new)) } else { None };
        StoreResponse { status, body, next_offset, up_to_date, closed }
    }
}

impl DurableStreamStore for HttpDurableStreamsStore {
    fn ensure<'a>(&'a self, path: &'a str, content_type: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .put(self.stream_url(path))
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .send()
                .await
                .with_context(|| format!("PUT {path}"))?;
            Ok(Self::response(res, BodyRead::Always).await)
        })
    }

    fn append<'a>(
        &'a self,
        path: &'a str,
        content_type: &'a str,
        body: Vec<u8>,
        response_body: BodyRead,
    ) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .post(self.stream_url(path))
                .header(reqwest::header::CONTENT_TYPE, content_type)
                .body(body)
                .send()
                .await
                .with_context(|| format!("POST {path}"))?;
            Ok(Self::response(res, response_body).await)
        })
    }

    fn read<'a>(&'a self, path: &'a str, offset: &'a str, live: bool) -> StoreFuture<'a> {
        Box::pin(async move {
            let mut url = format!("{}?offset={}", self.stream_url(path), offset);
            if live {
                url.push_str("&live=long-poll");
            }
            let res = self.http.get(url).send().await.with_context(|| format!("GET {path}"))?;
            Ok(Self::response(res, BodyRead::OnData).await)
        })
    }

    fn head<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self.http.head(self.stream_url(path)).send().await.with_context(|| format!("HEAD {path}"))?;
            Ok(Self::response(res, BodyRead::Never).await)
        })
    }

    fn close<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self
                .http
                .post(self.stream_url(path))
                .header("stream-closed", "true")
                .send()
                .await
                .with_context(|| format!("POST {path} (close)"))?;
            Ok(Self::response(res, BodyRead::Always).await)
        })
    }

    fn delete<'a>(&'a self, path: &'a str) -> StoreFuture<'a> {
        Box::pin(async move {
            let res = self.http.delete(self.stream_url(path)).send().await.with_context(|| format!("DELETE {path}"))?;
            Ok(Self::response(res, BodyRead::Always).await)
        })
    }
}

#[derive(Clone)]
pub struct DsClient {
    base: String,
    store: Arc<dyn DurableStreamStore>,
    /// Shared across clones (installed after the engine exists, seen by every copy of the client
    /// from then on). See [`Self::set_gone_reconciler`].
    reconcile: std::sync::Arc<std::sync::OnceLock<GoneReconciler>>,
    /// Bytes appended per stream path since this process started (serialized request bodies).
    /// The durable-streams server exposes no per-stream sizes, so this engine-side accounting is
    /// what the retention disk-budget layer works from. It undercounts streams that already
    /// existed before the process started (restart persistence is the catalog work, GH #8).
    appended: std::sync::Arc<std::sync::Mutex<HashMap<String, u64>>>,
}

impl DsClient {
    pub fn new(base: impl Into<String>) -> Self {
        let base = base.into();
        Self::with_store(base.clone(), Arc::new(HttpDurableStreamsStore::new(base)))
    }

    /// Construct the semantic facade over a supplied single-attempt store.  This is restricted to
    /// the engine crate so application callers cannot acquire a dependency on a provider or HTTP
    /// status behavior; deterministic stores belong in `ds.rs` unit tests.
    fn with_store(base: String, store: Arc<dyn DurableStreamStore>) -> Self {
        DsClient {
            base,
            store,
            reconcile: std::sync::Arc::new(std::sync::OnceLock::new()),
            appended: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Install the reconciler [`Self::append_reliable`] consults before believing a terminal
    /// append answer (see [`GoneVerdict`]). Shared by every clone of this client, including the ones
    /// already handed to the sequencer, the emission lanes and the subquery registry — which is why
    /// it can be installed after construction. Idempotent: a second install is ignored.
    pub fn set_gone_reconciler(&self, reconciler: GoneReconciler) {
        let _ = self.reconcile.set(reconciler);
    }

    /// Tracked bytes appended to `path` since process start (0 if never appended).
    pub fn appended_bytes(&self, path: &str) -> u64 {
        self.appended.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// Snapshot of tracked appended bytes for every stream path with the given prefix
    /// (e.g. `"shape/"` for the retention disk budget).
    pub fn appended_bytes_with_prefix(&self, prefix: &str) -> HashMap<String, u64> {
        self.appended
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p.starts_with(prefix))
            .map(|(p, b)| (p.clone(), *b))
            .collect()
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    pub fn stream_url(&self, path: &str) -> String {
        format!("{}/{}", self.base.trim_end_matches('/'), path.trim_start_matches('/'))
    }

    /// Idempotently create a JSON stream (PUT). Existing stream with same config -> 200.
    pub async fn ensure_stream(&self, path: &str) -> Result<()> {
        let res = self.store.ensure(path, "application/json").await?;
        if (200..300).contains(&res.status) {
            Ok(())
        } else {
            let status = res.status;
            Err(status_error("PUT", path, status, &res.body_or_default()))
        }
    }

    /// Append envelopes as a JSON array (the server flattens one array level into N messages).
    /// Append raw JSON events (non-envelope streams, e.g. the shape catalog).
    pub async fn append_json(&self, path: &str, events: &[serde_json::Value]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let body = serde_json::to_vec(events).with_context(|| format!("serializing POST {path}"))?;
        let res = self.store.append(path, "application/json", body, BodyRead::OnFailure).await?;
        if !(200..300).contains(&res.status) {
            let status = res.status;
            return Err(status_error("POST", path, status, &res.body_or_default()));
        }
        Ok(())
    }

    /// Read raw JSON events (non-envelope streams). Returns `(events, next_offset, up_to_date)`.
    pub async fn read_json(&self, path: &str, offset: &str) -> Result<(Vec<serde_json::Value>, Option<String>, bool)> {
        let res = self.store.read(path, offset, false).await?;
        if res.status == 204 || res.status == 404 {
            return Ok((Vec::new(), res.next_offset, true));
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("GET", path, res.status, ""));
        }
        let next_offset = res.next_offset.clone();
        let up_to_date = res.up_to_date;
        let body = res.required_body()?;
        let events: Vec<serde_json::Value> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok((events, next_offset, up_to_date))
    }

    /// Append envelopes. A retired stream (404/410/closed) is an error here; the live path uses
    /// [`Self::append_reliable`], which discards instead, and the change log uses
    /// [`Self::append_checked`], which routes around it.
    pub async fn append(&self, path: &str, envelopes: &[Envelope]) -> Result<()> {
        match self.append_once(path, envelopes).await {
            Ok(_) => Ok(()),
            Err(AppendError::Gone(status)) => bail!("POST {path} -> {status} (stream retired)"),
            Err(AppendError::Other(e)) => Err(e),
        }
    }

    /// How long [`Self::append_retrying`] keeps trying a transient failure before giving up. Long
    /// enough to ride out a storage restart or a failover, short enough that a boot does not hang
    /// on a dependency that is not coming back.
    pub const RESTORE_APPEND_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

    /// Append on a path where **failing costs the shape**: activation, the catalog restore's
    /// re-seed, a dormant shape's replay. Transient storage failures (`ds::is_unavailable`:
    /// transport, timeout, 5xx) are retried with capped backoff until `budget` runs out or the
    /// shutdown token fires.
    ///
    /// The plain [`Self::append`] propagates the first error, and an error on these paths is costly
    /// — a dormant shape's failed replay evicts it, and a restore that fails costs the whole boot
    /// attempt (ADR-0009); one 503 during a boot once permanently deleted an acknowledged
    /// subscription and its stream. Permanent removal is for records that are genuinely
    /// unrecoverable: a definite refusal (a 4xx, an unserialisable event), a stream storage confirms
    /// is gone (`HEAD` → 404/410/closed), or an exhausted budget. A service that is merely
    /// unavailable is backpressure, not loss.
    ///
    /// A **terminal** answer (404/410/`stream-closed`) gets the same reconciliation
    /// [`Self::append_reliable`] gives it, and for the same reason: it is what a proxy, a storage
    /// router or a failover says just as readily as a real deletion, and believing one here retires
    /// an acknowledged shape. `HEAD` decides — the stream is there and open ⇒ the status was false,
    /// keep retrying within the budget; storage agrees it is gone (or a `HEAD` that itself fails
    /// cannot say) ⇒ only the first of those is terminal.
    pub async fn append_retrying(
        &self,
        path: &str,
        envelopes: &[Envelope],
        budget: std::time::Duration,
        shutdown: &crate::shutdown::ShutdownToken,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + budget;
        let mut attempt = 0u32;
        loop {
            let e = match self.append_checked(path, envelopes).await {
                Ok(Appended::Ok { .. }) => return Ok(()),
                Ok(Appended::Retired(status)) => match self.head(path).await {
                    // There and appendable: the terminal status did not come from storage. Typed as
                    // its own condition, so a budget that runs out on it is retried at boot and NAMED
                    // for what it is — something between the engine and storage answering one stream
                    // two ways — rather than as an unreachable server or an unexplained refusal.
                    Ok(Some(head)) if !head.closed => {
                        anyhow::Error::new(DsInconsistent { path: path.to_string(), status })
                    }
                    // Storage agrees: this one really is terminal, and the caller retires the record.
                    // Typed, so the caller can tell a vanished stream from a failure.
                    Ok(_) => return Err(anyhow::Error::new(StreamGone { path: path.to_string(), status })),
                    // Cannot tell. Retrying costs a stale append at worst; retiring on a guess costs
                    // an acknowledged subscription. The HEAD's own error is kept, typed.
                    Err(he) => he.context(format!("POST {path} -> {status}; HEAD could not confirm it")),
                },
                Err(e) => {
                    if !is_unavailable(&e) {
                        return Err(e);
                    }
                    // Storage answering "no such stream" to a HEAD is the one transient-looking case
                    // that is really terminal: stop waiting and let the caller retire the record.
                    // Typed like the terminal answer above, so the caller can tell the two apart from a
                    // failure the same way; the transient error that led here is kept as context.
                    // (`head` does not say whether it was 404 or 410; both mean "not there".)
                    if let Ok(None) = self.head(path).await {
                        return Err(anyhow::Error::new(StreamGone { path: path.to_string(), status: 404 })
                            .context(format!("POST {path} failed ({e:#}) and HEAD finds no stream")));
                    }
                    e
                }
            };
            attempt += 1;
            if std::time::Instant::now() >= deadline {
                return Err(e.context(format!("appending to {path} kept failing for {budget:?} ({attempt} attempts)")));
            }
            let backoff = std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
            tracing::warn!("append to {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown.wait() => {
                    return Err(e.context(format!("appending to {path} abandoned: shutting down")));
                }
            }
        }
    }

    /// Append, reporting a retired stream as an outcome instead of an error, and handing back the
    /// stream's tail offset on success.
    ///
    /// The change log's writer needs both (ADR-0006): the tail offset IS the segment's size (the
    /// rotation decision), and a closed segment is a routing signal — walk forward to the successor
    /// — not a loss. Transient failures still surface as `Err` so the ingestor can tear its
    /// connection down unacknowledged rather than lose the commit.
    pub async fn append_checked(&self, path: &str, envelopes: &[Envelope]) -> Result<Appended> {
        match self.append_once(path, envelopes).await {
            Ok(next_offset) => Ok(Appended::Ok { next_offset }),
            Err(AppendError::Gone(status)) => Ok(Appended::Retired(status)),
            Err(AppendError::Other(e)) => Err(e),
        }
    }

    async fn append_once(
        &self,
        path: &str,
        envelopes: &[Envelope],
    ) -> std::result::Result<Option<String>, AppendError> {
        if envelopes.is_empty() {
            return Ok(None);
        }
        // Serialize once ourselves (instead of `.json(...)`) so the successful append's byte size
        // can be recorded for the retention disk-budget accounting.
        let payload = serde_json::to_vec(envelopes)
            .map_err(|e| AppendError::Other(anyhow::Error::new(e).context(format!("serializing POST {path}"))))?;
        let payload_len = payload.len() as u64;
        let res =
            self.store.append(path, "application/json", payload, BodyRead::Always).await.map_err(AppendError::Other)?;
        // A retired stream answers 404 (deleted), 410 (soft-deleted) or 409 + `stream-closed: true`
        // (closed, which retirement does before deleting).  The provider parses the header before
        // draining its response; the body text ("stream is closed") is not the contract.
        if (200..300).contains(&res.status) {
            *self.appended.lock().unwrap().entry(path.to_string()).or_insert(0) += payload_len;
            Ok(res.next_offset)
        } else if res.status == 404 || res.status == 410 || (res.status == 409 && res.closed) {
            Err(AppendError::Gone(res.status))
        } else {
            let status = res.status;
            Err(AppendError::Other(status_error("POST", path, status, &res.body_or_default())))
        }
    }

    /// Append with **no silent loss**: retry transient failures with capped backoff until the append
    /// lands. A dropped shape-stream append is a permanent divergence for every subscriber of that
    /// shape, so the only sound behaviors are (a) retry until success — the storage server being down
    /// simply backpressures the tailer, matching the ingestor's read-then-commit stance — or (b) stop
    /// because the stream was retired (the shape was dropped/evicted mid-flush), which is a clean
    /// no-op. Envelopes are absolute per-pk (`upsert`/`delete` by key), so an at-least-once retry
    /// that double-appends after an ambiguous network failure is idempotent for readers.
    /// Returns `false` iff the stream is retired (404, 410, or closed).
    ///
    /// Treating a **closed** stream as terminal is sound only for shape streams: their envelopes are
    /// absolute per-pk and the stream is about to be deleted, so the discarded batch has no reader
    /// left to diverge. The change log must keep using [`Self::append`] (which propagates): a closed
    /// `changes/*` segment is a routing signal, and silently dropping ingest there would lose data.
    ///
    /// **A terminal answer is reconciled, never taken on trust.** "404" is what a proxy, a storage
    /// router or a failover says just as readily as a real deletion, and this method's `false` makes
    /// the caller advance past the batch — leaving a still-registered shape permanently missing a
    /// committed Postgres change, with nothing anywhere that remembers it. So when a reconciler is
    /// installed ([`Self::set_gone_reconciler`]) the engine gets to answer: [`GoneVerdict::Retry`]
    /// (the shape is registered and its stream is right there — the 404 was false) keeps retrying,
    /// and [`GoneVerdict::Discard`] means the engine has confirmed the stream is gone and has retired
    /// the shape, so the batch has no reader left. Either way the shape's batch is never silently
    /// abandoned while the shape stays registered and stale.
    pub async fn append_reliable(&self, path: &str, envelopes: &[Envelope]) -> bool {
        let mut attempt = 0u32;
        let mut false_gone = 0u32;
        loop {
            match self.append_once(path, envelopes).await {
                Ok(_) => return true,
                Err(AppendError::Gone(status)) => {
                    let verdict = match self.reconcile.get() {
                        Some(reconcile) => reconcile(path.to_string()).await,
                        None => GoneVerdict::Discard,
                    };
                    if verdict == GoneVerdict::Discard {
                        tracing::debug!(
                            "append to {path}: stream retired ({status}); discarding {} envelopes",
                            envelopes.len()
                        );
                        return false;
                    }
                    false_gone += 1;
                    if false_gone == 1 {
                        tracing::warn!(
                            "append to {path} answered {status}, but the shape is still registered and its \
                             stream is still there: treating the terminal status as transient and retrying"
                        );
                    }
                    attempt += 1;
                    let backoff =
                        std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                    tokio::time::sleep(backoff).await;
                }
                Err(AppendError::Other(e)) => {
                    attempt += 1;
                    let backoff =
                        std::time::Duration::from_millis(100u64.saturating_mul(1 << attempt.min(5)).min(2000));
                    if attempt.is_multiple_of(10) {
                        tracing::error!("append to {path} still failing after {attempt} attempts: {e:#}");
                    } else {
                        tracing::warn!("append to {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
                    }
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Close a stream: `POST` with `Stream-Closed: true` and an empty body. Closing is terminal —
    /// appends are refused with `409` + `stream-closed` afterwards — and it releases every waiting
    /// long-poll reader immediately with `stream-closed: true` instead of leaving it blocked until
    /// the read times out. Idempotent (`204` again for an already-closed stream); an absent (`404`)
    /// or soft-deleted (`410`) stream is a success, there is nothing left to close.
    pub async fn close_stream(&self, path: &str) -> Result<()> {
        let res = self.store.close(path).await?;
        if (200..300).contains(&res.status) || res.status == 404 || res.status == 410 {
            Ok(())
        } else {
            let status = res.status;
            bail!("POST {path} (close) -> {status}: {}", res.body_or_default())
        }
    }

    /// Retire a stream: close it, THEN delete it (see `docs/adr/0007-retirement-closes-before-delete.md`).
    /// Every engine-initiated removal of a shape stream goes through here — eviction, purge,
    /// drop-at-restore, the degraded subquery reap, and the future schema-drift / epoch-reset paths —
    /// so a tailing client is released at once with `stream-closed` and "closed" unambiguously means
    /// "the engine retired this shape; re-subscribe". Closing is terminal, so the paths that are NOT
    /// retirement must not use it: deactivation parks a dormant shape whose stream stays appendable
    /// for reactivation, and creation rollback removes a stream no subscriber ever saw.
    /// A failed close is logged and does not block the delete: removing the stream is the must-have,
    /// the close is the courtesy signal. Returns the delete's result.
    pub async fn retire_stream(&self, path: &str) -> Result<()> {
        if let Err(e) = self.close_stream(path).await {
            tracing::warn!("retiring stream {path}: close failed ({e:#}); deleting anyway");
        }
        self.delete_stream(path).await
    }

    /// Delete a stream (DELETE). An already-gone stream — absent (404) or soft-deleted (410) — is a
    /// success: deletion is idempotent, and a retry loop (the degraded reap) must not spin forever
    /// on a stream storage has already retired.
    pub async fn delete_stream(&self, path: &str) -> Result<()> {
        let res = self.store.delete(path).await?;
        if (200..300).contains(&res.status) || res.status == 404 || res.status == 410 {
            self.appended.lock().unwrap().remove(path);
            Ok(())
        } else {
            let status = res.status;
            bail!("DELETE {path} -> {status}: {}", res.body_or_default())
        }
    }

    /// [`Self::head`], retrying a transient failure ([`is_unavailable`]) in place: up to `attempts`
    /// tries in all, `100 ms × attempt` apart. A definitive answer — there, closed, not there — and a
    /// non-transient error return at once; only the transport and storage's own unavailability are
    /// worth asking again.
    ///
    /// The callers turn an unanswered `HEAD` into something costly — a refused join, a failed boot
    /// attempt — so one dropped response must not be enough to do it.
    pub async fn head_retrying(&self, path: &str, attempts: u32) -> Result<Option<StreamHead>> {
        let mut attempt = 0u32;
        loop {
            match self.head(path).await {
                Ok(head) => return Ok(head),
                Err(e) => {
                    attempt += 1;
                    if attempt >= attempts.max(1) || !is_unavailable(&e) {
                        return Err(e);
                    }
                    let backoff = std::time::Duration::from_millis(100 * u64::from(attempt));
                    tracing::warn!("HEAD {path} failed (attempt {attempt}), retrying in {backoff:?}: {e:#}");
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// `HEAD` a stream: its tail offset and whether it is closed, without reading a byte of it (and
    /// without resetting its TTL). `Ok(None)` = not there (404) or soft-deleted (410).
    ///
    /// The change log's boot walk uses this to step over segments a crashed predecessor closed
    /// (ADR-0006): a closed segment can be a gigabyte, and durable-streams offers no bounded tail
    /// read, so the successor is derived and *verified* rather than read back.
    pub async fn head(&self, path: &str) -> Result<Option<StreamHead>> {
        let res = self.store.head(path).await?;
        if res.status == 404 || res.status == 410 {
            return Ok(None);
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("HEAD", path, res.status, ""));
        }
        Ok(Some(StreamHead { next_offset: res.next_offset, closed: res.closed }))
    }

    /// Read from `offset` (use "-1" for the beginning). `live` enables long-poll tailing.
    pub async fn read(&self, path: &str, offset: &str, live: bool) -> Result<ReadResult> {
        let res = self.store.read(path, offset, live).await?;

        // 204 = long-poll timeout / no new data / a close that woke this long-poll.
        if res.status == 204 {
            return Ok(ReadResult {
                envelopes: Vec::new(),
                next_offset: res.next_offset,
                up_to_date: res.up_to_date,
                closed: res.closed,
            });
        }
        // A stream that is not there is a TYPED error (see `StreamGone`), never a generic read
        // failure: on the change log every caller has its own, very different answer to it.
        if res.status == 404 || res.status == 410 {
            return Err(anyhow::Error::new(StreamGone { path: path.to_string(), status: res.status }));
        }
        if !(200..300).contains(&res.status) {
            return Err(status_error("GET", path, res.status, ""));
        }
        let next_offset = res.next_offset.clone();
        let up_to_date = res.up_to_date;
        let closed = res.closed;
        let body = res.required_body()?;
        let envelopes: Vec<Envelope> = if body.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(&body).with_context(|| format!("parsing stream body: {body}"))?
        };
        Ok(ReadResult { envelopes, next_offset, up_to_date, closed })
    }
}

fn header(res: &reqwest::Response, name: &str) -> Option<String> {
    res.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct ScriptedStore {
        appended: std::sync::Mutex<Vec<(String, String, Vec<u8>)>>,
        fail_read_body: bool,
    }

    fn response(status: u16) -> StoreResponse {
        StoreResponse {
            status,
            body: Some(Ok(String::new())),
            next_offset: Some("opaque-provider-token".to_string()),
            up_to_date: true,
            closed: false,
        }
    }

    impl DurableStreamStore for ScriptedStore {
        fn ensure<'a>(&'a self, _path: &'a str, _content_type: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(201)) })
        }

        fn append<'a>(
            &'a self,
            path: &'a str,
            content_type: &'a str,
            body: Vec<u8>,
            _response_body: BodyRead,
        ) -> StoreFuture<'a> {
            Box::pin(async move {
                self.appended.lock().unwrap().push((path.to_string(), content_type.to_string(), body));
                Ok(response(204))
            })
        }

        fn read<'a>(&'a self, _path: &'a str, _offset: &'a str, _live: bool) -> StoreFuture<'a> {
            Box::pin(async move {
                let mut res = response(200);
                res.next_offset = Some("tempting-next-offset".to_string());
                if self.fail_read_body {
                    res.body = Some(Err(anyhow::anyhow!("scripted stream body failure")));
                }
                Ok(res)
            })
        }

        fn head<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(200)) })
        }

        fn close<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(204)) })
        }

        fn delete<'a>(&'a self, _path: &'a str) -> StoreFuture<'a> {
            Box::pin(async { Ok(response(204)) })
        }
    }

    #[tokio::test]
    async fn facade_keeps_envelope_codec_and_byte_accounting_above_the_store_port() {
        let store = Arc::new(ScriptedStore::default());
        let client = DsClient::with_store("scripted://provider".to_string(), store.clone());
        let envelope = Envelope {
            type_: "public.items".to_string(),
            key: "item-1".to_string(),
            value: Some(serde_json::json!({ "id": "item-1" })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "upsert".to_string(),
                txid: None,
                offset: None,
                lsn: None,
                seq: Some(7),
                last: Some(true),
                schema: None,
            },
        };
        let expected = serde_json::to_vec(&[envelope.clone()]).unwrap();

        let appended = client.append_checked("shape/s1", &[envelope]).await.unwrap();

        assert!(matches!(appended, Appended::Ok { next_offset: Some(ref token) } if token == "opaque-provider-token"));
        assert_eq!(client.appended_bytes("shape/s1"), expected.len() as u64);
        assert_eq!(client.stream_url("shape/s1"), "scripted://provider/shape/s1");
        assert_eq!(
            *store.appended.lock().unwrap(),
            vec![("shape/s1".to_string(), "application/json".to_string(), expected)]
        );
    }

    #[tokio::test]
    async fn successful_read_body_failure_never_accepts_the_advertised_next_offset() {
        let store = Arc::new(ScriptedStore { fail_read_body: true, ..Default::default() });
        let client = DsClient::with_store("scripted://provider".to_string(), store);

        let envelope_err = match client.read("changes/0", "prior-offset", false).await {
            Ok(_) => panic!("a successful GET with an unreadable body must not produce a page"),
            Err(err) => err,
        };
        assert!(
            format!("{envelope_err:#}").contains("scripted stream body failure"),
            "the source body failure must survive the facade boundary"
        );

        let json_err = client
            .read_json("meta/catalog", "prior-offset")
            .await
            .expect_err("a successful GET with an unreadable body must not produce JSON events");
        assert!(
            format!("{json_err:#}").contains("scripted stream body failure"),
            "the source body failure must survive the facade boundary"
        );
    }

    /// The boot classification of a durable-streams failure. Getting this wrong is expensive in
    /// both directions: forgiving too much hides a malformed catalog behind an infinite retry,
    /// forgiving too little exits `EX_CONFIG` for a storage pod that is merely slower to start than
    /// the engine — which, in a compose or Kubernetes start, is the normal ordering.
    #[tokio::test]
    async fn transport_failures_are_retryable_and_answers_are_not() {
        // A REAL connect refusal (nothing listens on port 1), not a fabricated one: `reqwest::Error`
        // has no public constructor, and a mock would only prove the mock.
        let refused = match DsClient::new("http://127.0.0.1:1").read("changes/0", "-1", false).await {
            Err(e) => e,
            Ok(_) => panic!("nothing listens on port 1"),
        };
        assert!(is_unavailable(&refused), "a refused connection must retry: {refused:#}");

        // A 5xx: the server is there and not serving.
        let five_oh_three = anyhow::Error::new(DsUnavailable { op: "GET", path: "meta/catalog".into(), status: 503 })
            .context("folding the durable catalog");
        assert!(is_unavailable(&five_oh_three));
        assert_eq!(
            crate::pg::boot_disposition(&five_oh_three),
            crate::pg::BootFailure::Retryable,
            "storage that is not serving yet must not exit EX_CONFIG"
        );
        assert_eq!(crate::pg::boot_failure_name(&five_oh_three), "durable-streams is unreachable");

        // ...but an ANSWER is not a transport failure. A stream that is gone, a malformed catalog
        // and a strictness refusal all stay fatal.
        let gone = anyhow::Error::new(StreamGone { path: "meta/catalog".into(), status: 404 });
        assert!(!is_unavailable(&gone));
        assert_eq!(crate::pg::boot_disposition(&gone), crate::pg::BootFailure::Fatal);

        let malformed = anyhow::Error::new(serde_json::from_str::<serde_json::Value>("{oh no").unwrap_err())
            .context("parsing stream body");
        assert!(!is_unavailable(&malformed));
        assert_eq!(crate::pg::boot_disposition(&malformed), crate::pg::BootFailure::Fatal);

        // A typed catalog strictness refusal carries no `reqwest::Error` at all, so it stays fatal
        // — the same path every non-transport boot failure takes.
        let strictness = anyhow::anyhow!("catalog predates ADR-0006 segmentation");
        assert!(!is_unavailable(&strictness));
        assert_eq!(crate::pg::boot_disposition(&strictness), crate::pg::BootFailure::Fatal);
        assert_eq!(crate::pg::boot_failure_name(&strictness), "not a transient Postgres condition");
    }

    /// A 4xx carries its body (the server's own words); a 5xx — and a 429, which asks to be asked
    /// later — becomes the typed, retryable error.
    #[test]
    fn status_errors_are_typed_only_for_5xx_and_429() {
        let four = status_error("PUT", "shape/1", reqwest::StatusCode::BAD_REQUEST.as_u16(), "bad config");
        assert!(!is_unavailable(&four));
        assert!(format!("{four:#}").contains("bad config"));
        let five = status_error("PUT", "shape/1", reqwest::StatusCode::BAD_GATEWAY.as_u16(), "");
        assert!(is_unavailable(&five));
        assert!(format!("{five:#}").contains("502"));
        let slow_down = status_error("HEAD", "shape/1", reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16(), "");
        assert!(is_unavailable(&slow_down), "a 429 is a timing answer, not a refusal");
        assert_eq!(crate::pg::boot_disposition(&slow_down), crate::pg::BootFailure::Retryable);
    }
}
