//! Per-transaction buffering with **spill to disk** and **chunked appends** (ADR-0003) — the
//! receiver side of "ingest stays on pgoutput protocol v1".
//!
//! The ingestor cannot append a transaction before its `Commit` frame arrives (the commit LSN is
//! unknown until then, and the transaction may still abort), so every change between `Begin` and
//! `Commit` has to be held somewhere. Holding it all in RAM is what a 1M-row `UPDATE` under
//! `REPLICA IDENTITY FULL` turns into a multi-gigabyte spike; protocol v2 streaming would not change
//! that (see the ADR). So the buffer is bounded instead:
//!
//! - while the transaction is small it is held as ordinary [`Envelope`] structs — **nothing is
//!   serialized on the way in**, so an ordinary commit costs exactly what it did before this ADR
//!   (one serialization, in `DsClient`, at the append). The cap is measured on
//!   [`crate::ds::envelope_memory_bytes`] — inline size plus owned heap — so its units are the
//!   memory actually held, not the size the same data would serialize to;
//! - once that reaches `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` the buffer is serialized out to one
//!   temporary file as newline-delimited JSON, memory is released, and every further envelope of
//!   that transaction is serialized **straight to the file**;
//! - at `Commit` the envelopes are streamed back in order (stamped in place from memory, parsed
//!   back from the file), and handed out as chunks whose serialized POST body stays under
//!   `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` (which must itself stay under the durable-streams
//!   body cap, [`DS_MAX_BODY_BYTES`]).
//!
//! Peak **ingestor** memory is therefore the cap plus one chunk (held parsed, plus its serialized
//! body while the POST is in flight), for a transaction of any size. That is a bound on the
//! ingestor, not on the engine: the sequencer's own read page, its held run and `txn_pending` are
//! bounded by the transaction's size, not by this knob. Nothing is ever invalidated, refused or
//! purged because a transaction was large — the ADR is explicit that transaction size must never
//! cost a shape its stream.
//!
//! **`seq` and the transaction-end marker are assigned by the drain**, not by the buffer: `seq` is
//! the running index over the whole transaction, so it stays contiguous `0..n` *across* chunk
//! boundaries, and `headers.last = Some(true)` is set on the final envelope of the final chunk (and
//! on the only envelope of a one-chunk commit, so the rule is uniform). The sequencer needs both:
//! `(lsn, seq)` is its de-duplication key, and `last` is what tells it a `(txid, lsn)` run is
//! complete — without it, chunk 1 of a large commit would look like a whole transaction and be
//! flushed to shape streams on its own. See `engine/sequencer.rs`.
//!
//! **Chunk packing needs a serialized size**, which the in-memory path does not otherwise have. It
//! is obtained at drain with an allocation-free counting writer (one `serde_json::to_writer` pass
//! into a byte counter, no `Vec`, no parse). That is the one CPU cost this ADR adds to a small
//! commit; the alternative — packing on the heap estimate — would make "no chunk body exceeds the
//! budget" an approximation instead of a fact.
//!
//! **The spill file is process-local scratch, never state**: it is created `O_EXCL` with mode 0600
//! inside a 0700 directory, and removed when the buffer is dropped — at commit, when a `Begin`
//! replaces the buffer, and when the replication connection is torn down. A leftover from a process
//! that died mid-transaction is removed by [`sweep_spill_dir`] at the next boot. That sweep
//! identifies leftovers by the pid in the file name, which is only meaningful **within one pid
//! namespace**: two engines in different containers sharing a spill directory (a `hostPath` temp
//! mount, say) can see each other's pids as live or dead at random, so sharing a spill directory
//! between engines is unsupported — give each its own.

use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use anyhow::{Context, Result, bail};

use crate::ds::{Envelope, EnvelopeHeaders, envelope_memory_bytes};

/// The durable-streams server's cap on a single request body (1 GiB). A configured append budget
/// above this could not land, so it is refused at boot rather than discovered as a failing commit.
pub const DS_MAX_BODY_BYTES: u64 = 1024 * 1024 * 1024;

/// Upper bound on the bytes stamping adds to one serialized envelope, used when packing chunks so
/// the POST body that `DsClient` builds from a chunk is still within the budget:
/// `,"txid":"4294967295"` (20) + `,"lsn":"FFFFFFFF/FFFFFFFF"` (26) + `,"seq":18446744073709551615`
/// (27) + `,"last":true` (12) = 85, rounded up for headroom. Over-counting only makes chunks
/// slightly smaller.
const STAMP_BYTES: u64 = 96;

/// Prefix of a spill file's name. The `<xid>-<pid>` suffix makes it unique: protocol v1 delivers
/// transactions serially, so one process has at most one buffered transaction at a time.
const SPILL_PREFIX: &str = "circuits-txn-";
const SPILL_SUFFIX: &str = ".ndjson";

/// The default spill directory: a PRIVATE (0700) per-uid subdirectory of the system temp dir, not
/// the temp dir itself. Spill files carry row data, and the system temp dir is world-readable.
fn default_spill_dir() -> PathBuf {
    // SAFETY: getuid is a pure read of the process's own credentials; it cannot fail.
    let uid = unsafe { libc::getuid() };
    std::env::temp_dir().join(format!("circuits-txn-spill-{uid}"))
}

/// Large-transaction tuning, resolved from the environment at boot (ADR-0003).
///
/// | Env var | Default | Meaning |
/// |---|---|---|
/// | `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` | `134217728` (128 MiB) | In-memory bytes of ONE transaction before it spills to disk. `0` = never spill. |
/// | `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` | `67108864` (64 MiB) | Largest POST body one commit's append may build. Must be > 0 and ≤ [`DS_MAX_BODY_BYTES`]. |
/// | `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` | `<temp dir>/circuits-txn-spill-<uid>` | Where spill files are written. Needs room for the largest transaction. |
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnBufferConfig {
    /// Buffered in-memory bytes ([`envelope_memory_bytes`]) before the transaction spills. `0`
    /// disables spilling entirely (the pre-ADR-0003 behaviour: buffer the whole transaction in
    /// memory).
    pub memory_bytes: u64,
    /// Byte budget for one append (one POST body).
    pub append_bytes: u64,
    /// Directory spill files are written to.
    pub spill_dir: PathBuf,
}

impl Default for TxnBufferConfig {
    fn default() -> Self {
        TxnBufferConfig {
            memory_bytes: 128 * 1024 * 1024,
            append_bytes: 64 * 1024 * 1024,
            spill_dir: default_spill_dir(),
        }
    }
}

impl TxnBufferConfig {
    /// Resolve from an env getter. Pure (no process-env access) so it is unit-testable, and
    /// **fallible**: an unparseable or out-of-range setting is boot-fatal rather than silently
    /// replaced by the default — same reasoning as `ELECTRIC_CIRCUITS_PG_TABLES` (see `config.rs`),
    /// a quietly half-configured memory cap is worse than a refused boot.
    pub fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<TxnBufferConfig> {
        let d = TxnBufferConfig::default();
        let memory_bytes = bytes_var(&get, "ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES", d.memory_bytes)?;
        let append_bytes = bytes_var(&get, "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES", d.append_bytes)?;
        if append_bytes == 0 {
            bail!(
                "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES must be a positive byte count (it bounds one \
                 append's request body); 0 would make every commit unappendable"
            );
        }
        if append_bytes > DS_MAX_BODY_BYTES {
            bail!(
                "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES is {append_bytes}, above the durable-streams \
                 request-body cap of {DS_MAX_BODY_BYTES} bytes; an append that large could never land"
            );
        }
        let spill_dir = get("ELECTRIC_CIRCUITS_TXN_SPILL_DIR")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or(d.spill_dir);
        Ok(TxnBufferConfig { memory_bytes, append_bytes, spill_dir })
    }

    /// Resolve from the process environment.
    pub fn from_env() -> Result<TxnBufferConfig> {
        TxnBufferConfig::resolve(|k| std::env::var(k).ok())
    }

    /// Boot probe: the spill directory must exist, be private, and be writable — **now**, not at the
    /// first large transaction. A spill that fails mid-commit tears the replication connection down
    /// and is retried forever against the same broken directory, so an unwritable one is a boot
    /// failure with a named message rather than an ingest loop nobody reads the logs of.
    ///
    /// Skipped when spilling is disabled (`memory_bytes == 0`): nothing will ever be written there.
    pub fn probe(&self) -> Result<()> {
        if self.memory_bytes == 0 {
            return Ok(());
        }
        ensure_spill_dir(&self.spill_dir)?;
        let probe = self.spill_dir.join(format!("{SPILL_PREFIX}probe-{}{SPILL_SUFFIX}", std::process::id()));
        let _ = std::fs::remove_file(&probe);
        let mut f = spill_open(&probe).with_context(|| {
            format!(
                "ELECTRIC_CIRCUITS_TXN_SPILL_DIR={} is not writable; large transactions spill there \
                 (ADR-0003), so the engine refuses to start rather than fail every big commit",
                self.spill_dir.display()
            )
        })?;
        let wrote = f.write_all(b"{}\n").and_then(|()| f.flush());
        drop(f);
        let _ = std::fs::remove_file(&probe);
        wrote.with_context(|| format!("writing to the transaction spill dir {} failed", self.spill_dir.display()))?;
        Ok(())
    }
}

/// Create the spill directory if absent, private to this user (0700 — spill files carry row data).
fn ensure_spill_dir(dir: &Path) -> Result<()> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("creating the transaction spill dir {}", dir.display()))
}

/// Create one spill file: `O_EXCL` (never adopt a file we did not just make) and mode 0600.
fn spill_open(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)
}

fn bytes_var(get: impl Fn(&str) -> Option<String>, name: &str, default: u64) -> Result<u64> {
    let Some(raw) = get(name) else { return Ok(default) };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(default);
    }
    raw.parse::<u64>().with_context(|| format!("{name}='{raw}' is not a byte count (a plain non-negative integer)"))
}

/// The transaction-level values every envelope of a commit is stamped with. `seq` and the
/// transaction-end marker are NOT here: the drain assigns `seq` as the running index over the whole
/// transaction and sets `last` on the final envelope, so both stay right across chunk boundaries.
#[derive(Clone, Debug)]
pub struct Stamp {
    /// The transaction's COMMIT LSN (not the per-change record LSN).
    pub lsn: String,
    /// The transaction's Postgres xid, as the wire spells it.
    pub txid: String,
}

/// The open spill file of a transaction that outgrew its memory cap.
struct Spill {
    path: PathBuf,
    /// `None` once the drain has flushed and closed it.
    writer: Option<BufWriter<std::fs::File>>,
    /// Bytes written to the file (NDJSON, so payload + one newline per envelope).
    bytes: u64,
}

/// Buffer for the spill writer. Spilling is a sequential bulk write of up to the memory cap; 1 MiB
/// keeps it to a handful of syscalls per chunk of that.
const SPILL_WRITE_BUFFER: usize = 1024 * 1024;

/// One transaction, buffered between `Begin` and `Commit`.
///
/// Envelopes go in with [`TxnBuffer::push`] and come out at commit through [`TxnBuffer::drain`] as
/// stamped chunks. Dropping the buffer removes its spill file, so an aborted transaction, a replaced
/// `Begin` and a torn-down connection all clean up on the ordinary path.
pub struct TxnBuffer {
    xid: u32,
    cfg: TxnBufferConfig,
    /// Envelopes still in memory, in transaction order, UNSERIALIZED. Emptied when the buffer
    /// spills.
    mem: Vec<Envelope>,
    /// In-memory bytes currently held in `mem` ([`envelope_memory_bytes`]).
    mem_bytes: u64,
    spill: Option<Spill>,
    /// Envelopes pushed so far.
    count: u64,
    /// In-memory bytes of everything pushed so far (memory + what has gone to the file) — what the
    /// cap is measured against.
    buffered_bytes: u64,
    /// Raw pgoutput payload bytes of the tracked changes (StatsD only).
    raw_bytes: u64,
    /// `__el_sync` counter carried by this transaction (drain barrier).
    sync: Option<i64>,
}

impl TxnBuffer {
    pub fn new(xid: u32, cfg: TxnBufferConfig) -> TxnBuffer {
        TxnBuffer {
            xid,
            cfg,
            mem: Vec::new(),
            mem_bytes: 0,
            spill: None,
            count: 0,
            buffered_bytes: 0,
            raw_bytes: 0,
            sync: None,
        }
    }

    pub fn xid(&self) -> u32 {
        self.xid
    }

    /// Envelopes buffered so far.
    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Raw pgoutput payload bytes of the tracked changes (StatsD).
    pub fn raw_bytes(&self) -> u64 {
        self.raw_bytes
    }

    /// In-memory bytes of everything buffered — the quantity the memory cap is measured on.
    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes
    }

    /// Has this transaction spilled to disk?
    pub fn spilled(&self) -> bool {
        self.spill.is_some()
    }

    /// The spill file, once there is one (tests and diagnostics).
    pub fn spill_path(&self) -> Option<&Path> {
        self.spill.as_ref().map(|s| s.path.as_path())
    }

    pub fn sync(&self) -> Option<i64> {
        self.sync
    }

    pub fn set_sync(&mut self, n: i64) {
        self.sync = Some(n);
    }

    /// Buffer one decoded change. `raw_len` is the pgoutput payload's size (StatsD accounting only).
    ///
    /// Nothing is serialized here while the transaction fits in memory — an ordinary commit costs
    /// exactly what it did before ADR-0003. An I/O failure on the spill file propagates: the
    /// ingestor tears the connection down unacknowledged and Postgres re-delivers the whole
    /// transaction.
    pub fn push(&mut self, env: Envelope, raw_len: u64) -> Result<()> {
        let bytes = envelope_memory_bytes(&env);
        self.count += 1;
        self.buffered_bytes += bytes;
        self.raw_bytes += raw_len;
        if self.spill.is_some() {
            let line = serde_json::to_vec(&env).context("serializing a change for the spill file")?;
            return self.write_spilled(&line);
        }
        self.mem_bytes += bytes;
        self.mem.push(env);
        // `0` = never spill (the pre-ADR-0003 behaviour, kept as an escape hatch).
        if self.cfg.memory_bytes > 0 && self.mem_bytes >= self.cfg.memory_bytes {
            self.begin_spill()?;
        }
        Ok(())
    }

    /// Cross the cap: create the spill file, serialize everything buffered so far into it, and
    /// release the memory. Happens exactly once per transaction — every later push is serialized
    /// straight to the file.
    ///
    /// The bulk write runs **on the caller's task**, not on `spawn_blocking`: it is a sequential
    /// write of at most the memory cap through a 1 MiB buffer to a local file, the ingestor task has
    /// nothing else to do until it returns (it cannot decode the next message before this change is
    /// buffered), and moving the buffer to a blocking pool and back would mean moving the whole
    /// transaction's envelopes across threads. It does occupy one tokio worker for the duration —
    /// stated here so it is a decision rather than an oversight.
    fn begin_spill(&mut self) -> Result<()> {
        ensure_spill_dir(&self.cfg.spill_dir)?;
        let path = self.cfg.spill_dir.join(format!("{SPILL_PREFIX}{}-{}{SPILL_SUFFIX}", self.xid, std::process::id()));
        // A file already there is a leftover the boot sweep could not attribute (a recycled pid);
        // it cannot be a live buffer of ours, since one process buffers one transaction at a time.
        let file = match spill_open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                tracing::warn!("transaction spill file {} already existed; replacing it", path.display());
                std::fs::remove_file(&path)
                    .with_context(|| format!("removing the stale spill file {}", path.display()))?;
                spill_open(&path).with_context(|| format!("creating the transaction spill file {}", path.display()))?
            }
            Err(e) => {
                return Err(anyhow::Error::new(e))
                    .with_context(|| format!("creating the transaction spill file {}", path.display()));
            }
        };
        self.spill = Some(Spill { path, writer: Some(BufWriter::with_capacity(SPILL_WRITE_BUFFER, file)), bytes: 0 });
        let buffered = std::mem::take(&mut self.mem);
        self.mem_bytes = 0;
        for env in &buffered {
            let line = serde_json::to_vec(env).context("serializing a change for the spill file")?;
            self.write_spilled(&line)?;
        }
        drop(buffered);
        crate::metrics::metrics().txn_spills.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            xid = self.xid,
            bytes = self.buffered_bytes,
            changes = self.count,
            "large transaction: buffering it in memory would exceed {} bytes; spilling to {} (ADR-0003)",
            self.cfg.memory_bytes,
            self.spill_path().map(|p| p.display().to_string()).unwrap_or_default(),
        );
        Ok(())
    }

    fn write_spilled(&mut self, line: &[u8]) -> Result<()> {
        let spill = self.spill.as_mut().expect("write_spilled with no spill file");
        let w = spill.writer.as_mut().expect("write_spilled after the drain closed the file");
        w.write_all(line).and_then(|()| w.write_all(b"\n")).with_context(|| {
            format!("writing a buffered change to the transaction spill file {}", spill.path.display())
        })?;
        spill.bytes += line.len() as u64 + 1;
        Ok(())
    }

    /// Start streaming the transaction out for its commit: envelopes in order, each stamped with
    /// `stamp` plus its running `seq`, the last of them additionally marked `last: true`, packed
    /// into chunks whose request body stays inside `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`.
    ///
    /// The caller appends each chunk to the CURRENT segment **in order** and acknowledges the slot
    /// only after the last one lands.
    pub fn drain(&mut self, stamp: Stamp) -> Result<TxnDrain<'_>> {
        let reader = match self.spill.as_mut() {
            None => None,
            Some(spill) => {
                if let Some(mut w) = spill.writer.take() {
                    w.flush()
                        .with_context(|| format!("flushing the transaction spill file {}", spill.path.display()))?;
                }
                let f = std::fs::File::open(&spill.path)
                    .with_context(|| format!("reopening the transaction spill file {}", spill.path.display()))?;
                Some(BufReader::with_capacity(SPILL_WRITE_BUFFER, f))
            }
        };
        let budget = self.cfg.append_bytes;
        Ok(TxnDrain { buf: self, idx: 0, reader, stamp, seq: 0, budget, pending: None, exhausted: false })
    }
}

impl Drop for TxnBuffer {
    fn drop(&mut self) {
        let Some(spill) = self.spill.take() else { return };
        crate::metrics::metrics().txn_spill_bytes.fetch_add(spill.bytes, Ordering::Relaxed);
        drop(spill.writer);
        if let Err(e) = std::fs::remove_file(&spill.path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!("could not remove the transaction spill file {}: {e}", spill.path.display());
        }
    }
}

/// A `std::io::Write` that keeps only a byte count — lets the drain learn an in-memory envelope's
/// serialized length with one allocation-free `serde_json::to_writer` pass.
struct CountingWriter(u64);

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The serialized JSON length of `v`, without allocating it. Shared with the streamed backfill
/// (`pg::BackfillReader`), which packs chunks against the same measure.
pub(crate) fn serialized_json_len<T: serde::Serialize + ?Sized>(v: &T) -> Result<u64> {
    let mut c = CountingWriter(0);
    serde_json::to_writer(&mut c, v).context("measuring a value's serialized size")?;
    Ok(c.0)
}

/// The serialized length of `env`, without allocating it.
fn serialized_len(env: &Envelope) -> Result<u64> {
    serialized_json_len(env)
}

/// The commit-time stream out of a [`TxnBuffer`]: envelopes in transaction order, stamped, packed
/// into append-sized chunks.
///
/// Also an [`Iterator`] of `Result<Vec<Envelope>>`, but the ingestor drives it with
/// [`TxnDrain::next_chunk`] so a read failure is a plain `?` in the middle of an `async` loop.
pub struct TxnDrain<'a> {
    buf: &'a mut TxnBuffer,
    /// Next index into `buf.mem` (unused once the transaction spilled — `mem` is empty then).
    idx: usize,
    /// Reader over the spill file, when there is one.
    reader: Option<BufReader<std::fs::File>>,
    stamp: Stamp,
    /// The running position within the transaction — the `seq` the sequencer de-duplicates on.
    seq: u64,
    budget: u64,
    /// The envelope that did not fit the chunk just closed, carried into the next one.
    pending: Option<(Envelope, u64)>,
    /// The source ran out: the chunk being built is the transaction's LAST, so its final envelope
    /// carries the transaction-end marker.
    exhausted: bool,
}

impl TxnDrain<'_> {
    /// The next chunk, or `None` when the transaction is fully drained.
    ///
    /// A chunk is as large as the budget allows, and never larger — except for a single envelope
    /// that is itself over budget, which becomes a chunk of its own: an envelope is the atom here,
    /// splitting one would produce two invalid JSON items.
    ///
    /// The last envelope of the last chunk is marked `last: true`. The loop always tries to read one
    /// more envelope than it needs, so "the chunk filled exactly and the transaction then ended" is
    /// still recognised as the end — a chunk is only returned unmarked when there genuinely is
    /// another one coming.
    pub fn next_chunk(&mut self) -> Result<Option<Vec<Envelope>>> {
        let mut chunk: Vec<Envelope> = Vec::new();
        // `DsClient` posts the chunk as a JSON array: two brackets, plus a comma between items.
        let mut body = 2u64;
        loop {
            let next = match self.pending.take() {
                Some(pending) => Some(pending),
                None => self.next_stamped()?,
            };
            let Some((env, len)) = next else {
                self.exhausted = true;
                break;
            };
            let cost = len + STAMP_BYTES + u64::from(!chunk.is_empty());
            if !chunk.is_empty() && body + cost > self.budget {
                self.pending = Some((env, len));
                break;
            }
            // A single envelope that cannot fit ANY body the storage server will accept is named
            // loudly before the append that is about to be refused. Behaviour is unchanged — the
            // append fails, the connection goes down unacknowledged, Postgres re-delivers — but the
            // operator gets the row instead of a bare 413.
            if chunk.is_empty() && len + STAMP_BYTES + 2 > DS_MAX_BODY_BYTES {
                tracing::error!(
                    "row too large for the storage server ({} > {DS_MAX_BODY_BYTES}): {} key {}",
                    len + STAMP_BYTES + 2,
                    env.type_,
                    env.key
                );
            }
            body += cost;
            chunk.push(env);
        }
        if self.exhausted
            && let Some(last) = chunk.last_mut()
        {
            last.headers.last = Some(true);
        }
        Ok(if chunk.is_empty() { None } else { Some(chunk) })
    }

    /// The next buffered envelope — taken from memory, or parsed back from the spill file — stamped,
    /// with its serialized size.
    fn next_stamped(&mut self) -> Result<Option<(Envelope, u64)>> {
        let (mut env, len) = match self.next_raw()? {
            None => return Ok(None),
            // A spilled envelope's serialized size is the line we just read; no second pass.
            Some(Raw::Line(line)) => {
                let len = line.len() as u64;
                let env: Envelope = serde_json::from_slice(&line)
                    .with_context(|| format!("parsing a buffered change of transaction {} back", self.buf.xid))?;
                (env, len)
            }
            // An in-memory envelope was never serialized; measure it without allocating.
            Some(Raw::Env(env)) => {
                let len = serialized_len(&env)?;
                (env, len)
            }
        };
        env.headers.lsn = Some(self.stamp.lsn.clone());
        env.headers.txid = Some(self.stamp.txid.clone());
        env.headers.seq = Some(self.seq);
        self.seq += 1;
        Ok(Some((env, len)))
    }

    /// The next buffered envelope in its stored form: from memory while the transaction never
    /// spilled, from the file once it did (spilling empties memory, so the two never interleave).
    fn next_raw(&mut self) -> Result<Option<Raw>> {
        if let Some(r) = self.reader.as_mut() {
            let mut line = Vec::new();
            let n = r.read_until(b'\n', &mut line).context("reading the transaction spill file")?;
            if n == 0 {
                return Ok(None);
            }
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            return Ok(Some(Raw::Line(line)));
        }
        if self.idx >= self.buf.mem.len() {
            return Ok(None);
        }
        // Take the envelope out as we go, so a drained transaction's memory is released chunk by
        // chunk rather than held until the whole commit has landed.
        let env = std::mem::replace(&mut self.buf.mem[self.idx], taken());
        self.idx += 1;
        Ok(Some(Raw::Env(env)))
    }
}

/// A cheap stand-in left in the memory vector as each envelope is taken out of it, so a drained
/// transaction releases its rows chunk by chunk instead of at the end of the commit.
fn taken() -> Envelope {
    Envelope {
        type_: String::new(),
        key: String::new(),
        value: None,
        old: None,
        headers: EnvelopeHeaders {
            operation: String::new(),
            txid: None,
            offset: None,
            lsn: None,
            seq: None,
            last: None,
            schema: None,
        },
    }
}

/// A buffered change as the buffer stored it. One variant is an inline `Envelope` and the other a
/// heap `Vec`, so the enum is `Envelope`-sized; that is deliberate — it exists for exactly one
/// move out of the buffer, never in a collection.
#[allow(clippy::large_enum_variant)]
enum Raw {
    /// Still a struct (never spilled).
    Env(Envelope),
    /// A line of the spill file.
    Line(Vec<u8>),
}

impl Iterator for TxnDrain<'_> {
    type Item = Result<Vec<Envelope>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}

/// Remove spill files a previous process left behind (it died between `Begin` and `Commit`).
/// Returns how many were removed.
///
/// Only files this engine names (`circuits-txn-<xid>-<pid>.ndjson`) are considered, and only ones
/// whose pid is not a live process — several engines could share a spill dir, and one must never
/// delete another's live buffer. `kill -0` is the liveness test (same as the dbsp state-dir sweep in
/// `subq_circuit.rs`), with `EPERM` read as ALIVE: a process owned by another uid exists.
///
/// **This is only sound within one pid namespace.** Two containers sharing a spill directory see
/// unrelated pid spaces, so each would read the other's files as dead (or, worse, as live) at
/// random; a spill directory must belong to exactly one engine. The default
/// (`<temp dir>/circuits-txn-spill-<uid>`) is per-user, not per-engine, so operators running several
/// engines as one user must set `ELECTRIC_CIRCUITS_TXN_SPILL_DIR` per engine.
///
/// Our own pid is NOT special-cased: a successor that happens to be given its predecessor's pid must
/// still be able to sweep the file that predecessor left. `live` lets the caller name the spill files
/// this process genuinely owns right now (the ingestor holds at most one, and never during boot), so
/// "the pid is ours" alone is not taken as evidence the file is in use.
pub fn sweep_spill_dir(dir: &Path) -> usize {
    sweep_spill_dir_except(dir, &[])
}

/// [`sweep_spill_dir`], keeping the named files (paths this process is actively writing).
pub fn sweep_spill_dir_except(dir: &Path, live: &[PathBuf]) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    let mut removed = 0usize;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(pid) = spill_file_pid(&name) else { continue };
        let path = entry.path();
        if live.contains(&path) {
            continue;
        }
        // Our own pid with no live buffer for it means a predecessor that had this pid — stale.
        if pid != std::process::id() && process_alive(pid) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                removed += 1;
                tracing::info!("removed the transaction spill file {} left by pid {pid}", path.display());
            }
            Err(e) => tracing::warn!("could not remove the stale spill file {}: {e}", path.display()),
        }
    }
    removed
}

/// The pid encoded in a spill file's name, if the name is one of ours.
fn spill_file_pid(name: &str) -> Option<u32> {
    let body = name.strip_prefix(SPILL_PREFIX)?.strip_suffix(SPILL_SUFFIX)?;
    // `<xid>-<pid>`: split off the END, an xid is never itself hyphenated but this is the safe read.
    let (xid, pid) = body.rsplit_once('-')?;
    xid.parse::<u32>().ok()?;
    pid.parse::<u32>().ok()
}

/// Is `pid` a live process? (`kill -0` semantics.)
///
/// `EPERM` counts as ALIVE: the process exists, it just belongs to another user. Reading it as dead
/// would let one user's engine delete another's live spill file.
fn process_alive(pid: u32) -> bool {
    // SAFETY: kill with signal 0 performs only the existence/permission check.
    if unsafe { libc::kill(pid as i32, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;

    fn env_of(key: &str, payload: usize) -> Envelope {
        Envelope {
            type_: "public.items".to_string(),
            key: key.to_string(),
            value: Some(serde_json::json!({ "id": key, "blob": "x".repeat(payload) })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".to_string(),
                txid: None,
                offset: None,
                lsn: None,
                seq: None,
                last: None,
                schema: None,
            },
        }
    }

    fn cfg(dir: &Path, memory_bytes: u64, append_bytes: u64) -> TxnBufferConfig {
        TxnBufferConfig { memory_bytes, append_bytes, spill_dir: dir.to_path_buf() }
    }

    /// A unique scratch dir under the system temp dir, removed by [`Scratch`]'s drop.
    struct Scratch(PathBuf);
    impl Scratch {
        fn new(tag: &str) -> Scratch {
            let p = std::env::temp_dir().join(format!(
                "circuits-txn-test-{}-{}-{tag}",
                std::process::id(),
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            ));
            std::fs::create_dir_all(&p).unwrap();
            Scratch(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn drain_all(buf: &mut TxnBuffer) -> Vec<Vec<Envelope>> {
        let stamp = Stamp { lsn: "0/1A2B3C4".to_string(), txid: "4242".to_string() };
        let mut out = Vec::new();
        let mut drain = buf.drain(stamp).unwrap();
        while let Some(chunk) = drain.next_chunk().unwrap() {
            out.push(chunk);
        }
        out
    }

    /// The cap crossing happens once, and everything after it goes to the file rather than to RAM.
    #[test]
    fn crossing_the_cap_spills_once_and_later_pushes_go_to_the_file() {
        let dir = Scratch::new("spill-once");
        let mut buf = TxnBuffer::new(7, cfg(dir.path(), 512, 1 << 20));
        let mut spilled_after = None;
        for i in 0..40 {
            buf.push(env_of(&format!("k{i}"), 32), 10).unwrap();
            if buf.spilled() && spilled_after.is_none() {
                spilled_after = Some(i);
            }
        }
        let at = spilled_after.expect("40 envelopes of ~100 bytes must cross a 512-byte cap");
        assert!(at < 39, "the cap was crossed well before the last push");
        // The one file, named for this transaction and process.
        let path = buf.spill_path().expect("spilled").to_path_buf();
        assert_eq!(
            path.file_name().unwrap().to_string_lossy(),
            format!("circuits-txn-7-{}.ndjson", std::process::id())
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "exactly one spill file");
        // Memory is released at the crossing and never grows again.
        assert!(buf.mem.is_empty(), "the buffered envelopes moved to the file");
        assert_eq!(buf.mem_bytes, 0);
        // Every envelope is on disk, one per line, once the drain has flushed the writer.
        drop(buf.drain(Stamp { lsn: "0/1".into(), txid: "7".into() }).unwrap());
        let lines = std::fs::read_to_string(&path).unwrap().lines().count();
        assert_eq!(lines, 40);
    }

    /// Order survives the memory→file boundary: the envelopes buffered before the crossing come out
    /// before the ones written straight to the file, and `seq` is contiguous 0..n across chunks.
    #[test]
    fn order_and_seq_survive_the_spill_and_the_chunking() {
        let dir = Scratch::new("order");
        // 512-byte cap and a 900-byte append budget: several chunks, and a spill part-way.
        let mut buf = TxnBuffer::new(9, cfg(dir.path(), 512, 900));
        for i in 0..50 {
            buf.push(env_of(&format!("k{i:03}"), 40), 10).unwrap();
        }
        assert!(buf.spilled());
        let chunks = drain_all(&mut buf);
        assert!(chunks.len() > 1, "the append budget forced chunking (got {} chunk(s))", chunks.len());
        let flat: Vec<Envelope> = chunks.into_iter().flatten().collect();
        assert_eq!(flat.len(), 50);
        for (i, env) in flat.iter().enumerate() {
            assert_eq!(env.key, format!("k{i:03}"), "transaction order is preserved");
            assert_eq!(env.headers.seq, Some(i as u64), "seq is the running index over the whole transaction");
            assert_eq!(env.headers.lsn.as_deref(), Some("0/1A2B3C4"));
            assert_eq!(env.headers.txid.as_deref(), Some("4242"));
        }
    }

    /// The same, without a spill: chunking alone must not disturb order or `seq`.
    #[test]
    fn an_unspilled_transaction_chunks_with_the_same_stamps() {
        let dir = Scratch::new("nospill");
        let mut buf = TxnBuffer::new(11, cfg(dir.path(), 0, 900));
        for i in 0..30 {
            buf.push(env_of(&format!("k{i:03}"), 40), 10).unwrap();
        }
        assert!(!buf.spilled(), "TXN_MEMORY_BYTES=0 never spills");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0, "and writes nothing to disk");
        let chunks = drain_all(&mut buf);
        assert!(chunks.len() > 1);
        let flat: Vec<Envelope> = chunks.into_iter().flatten().collect();
        assert_eq!(flat.len(), 30);
        for (i, env) in flat.iter().enumerate() {
            assert_eq!(env.key, format!("k{i:03}"));
            assert_eq!(env.headers.seq, Some(i as u64));
        }
    }

    /// The POST body a chunk becomes never exceeds the budget — measured on the REAL serialization
    /// (`DsClient` posts `serde_json::to_vec(chunk)`), stamps included.
    #[test]
    fn no_chunk_body_exceeds_the_append_budget() {
        let dir = Scratch::new("budget");
        const BUDGET: u64 = 1500;
        let mut buf = TxnBuffer::new(13, cfg(dir.path(), 400, BUDGET));
        for i in 0..60 {
            buf.push(env_of(&format!("k{i:03}"), 60), 10).unwrap();
        }
        let chunks = drain_all(&mut buf);
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            let body = serde_json::to_vec(chunk).unwrap().len() as u64;
            assert!(body <= BUDGET, "a chunk of {} envelopes serialized to {body} > {BUDGET}", chunk.len());
        }
    }

    /// An envelope larger than the whole budget is its own chunk — never split, never dropped.
    #[test]
    fn an_oversized_envelope_becomes_its_own_chunk() {
        let dir = Scratch::new("oversized");
        let mut buf = TxnBuffer::new(15, cfg(dir.path(), 0, 200));
        buf.push(env_of("small-a", 4), 10).unwrap();
        buf.push(env_of("huge", 4000), 10).unwrap();
        buf.push(env_of("small-b", 4), 10).unwrap();
        let chunks = drain_all(&mut buf);
        assert_eq!(chunks.len(), 3, "each envelope is over the 200-byte budget on its own");
        assert_eq!(chunks[1].len(), 1);
        assert_eq!(chunks[1][0].key, "huge");
        assert!(serde_json::to_vec(&chunks[1]).unwrap().len() > 200, "and it really is over budget");
        let keys: Vec<&str> = chunks.iter().flatten().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["small-a", "huge", "small-b"]);
    }

    /// The transaction-end marker lands on the LAST envelope of the LAST chunk, and nowhere else —
    /// the sequencer holds a run back until it sees one, so a misplaced (or missing) marker either
    /// splits a transaction or stalls the log.
    #[test]
    fn only_the_final_envelope_of_the_final_chunk_is_marked() {
        let dir = Scratch::new("marker");
        let mut buf = TxnBuffer::new(23, cfg(dir.path(), 400, 900));
        for i in 0..40 {
            buf.push(env_of(&format!("k{i:03}"), 40), 10).unwrap();
        }
        let chunks = drain_all(&mut buf);
        assert!(chunks.len() > 1, "several chunks");
        let flat: Vec<&Envelope> = chunks.iter().flatten().collect();
        for (i, env) in flat.iter().enumerate() {
            let want = i + 1 == flat.len();
            assert_eq!(env.headers.last, want.then_some(true), "envelope {i} of {}", flat.len());
        }
        // Stated as the sequencer reads it: every chunk but the last ends UNMARKED (so the run is
        // held), and the last ends marked (so it is processed).
        for (c, chunk) in chunks.iter().enumerate() {
            let ends_marked = chunk.last().unwrap().headers.last == Some(true);
            assert_eq!(ends_marked, c + 1 == chunks.len(), "chunk {c} of {}", chunks.len());
        }
    }

    /// A one-chunk commit is marked too, so the rule is uniform: "no marker" always means
    /// "incomplete", never "a small transaction that opted out".
    #[test]
    fn a_single_chunk_commit_is_marked_as_well() {
        let dir = Scratch::new("marker-one");
        let mut buf = TxnBuffer::new(25, cfg(dir.path(), 0, 1 << 20));
        buf.push(env_of("only", 8), 10).unwrap();
        let chunks = drain_all(&mut buf);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0][0].headers.last, Some(true));
    }

    /// The boundary case the one-ahead read exists for: a chunk that fills EXACTLY to its budget
    /// and is then followed by nothing must still carry the marker, or the transaction would never
    /// complete and the sequencer would hold it forever.
    #[test]
    fn a_chunk_that_fills_exactly_at_the_end_still_carries_the_marker() {
        let dir = Scratch::new("marker-exact");
        // The budget is computed to fit exactly two of these envelopes and not a byte more, so the
        // second chunk-read is what discovers the end — the case a "break when full" loop would get
        // wrong. (`2` brackets + per-envelope `len + STAMP_BYTES`, + 1 comma between them.)
        let one = serialized_len(&env_of("a", 8)).unwrap();
        let budget = 2 + 2 * (one + STAMP_BYTES) + 1;
        let mut buf = TxnBuffer::new(27, cfg(dir.path(), 0, budget));
        buf.push(env_of("a", 8), 10).unwrap();
        buf.push(env_of("b", 8), 10).unwrap();
        let mut drain = buf.drain(Stamp { lsn: "0/1".into(), txid: "27".into() }).unwrap();
        let first = drain.next_chunk().unwrap().expect("a chunk");
        assert_eq!(first.len(), 2, "both fit — exactly");
        assert!(serde_json::to_vec(&first).unwrap().len() as u64 <= budget);
        assert_eq!(first[1].headers.last, Some(true), "the source was exhausted while filling this chunk");
        assert!(drain.next_chunk().unwrap().is_none());

        // One byte less and they do NOT both fit: the split is real, and only the second chunk is
        // marked.
        let mut buf = TxnBuffer::new(28, cfg(dir.path(), 0, budget - 1));
        buf.push(env_of("a", 8), 10).unwrap();
        buf.push(env_of("b", 8), 10).unwrap();
        let chunks = drain_all(&mut buf);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0][0].headers.last, None);
        assert_eq!(chunks[1][0].headers.last, Some(true));
    }

    /// An empty transaction drains to nothing (the ingestor then appends nothing at all).
    #[test]
    fn an_empty_transaction_yields_no_chunks() {
        let dir = Scratch::new("empty");
        let mut buf = TxnBuffer::new(17, cfg(dir.path(), 512, 900));
        assert!(buf.is_empty());
        assert!(drain_all(&mut buf).is_empty());
    }

    /// The spill file is scratch, not state: dropping the buffer removes it — which is what happens
    /// at commit, at the next `Begin`, and when the replication connection is torn down.
    #[test]
    fn dropping_the_buffer_removes_the_spill_file() {
        let dir = Scratch::new("drop");
        let path = {
            let mut buf = TxnBuffer::new(19, cfg(dir.path(), 128, 1 << 20));
            for i in 0..10 {
                buf.push(env_of(&format!("k{i}"), 32), 10).unwrap();
            }
            let path = buf.spill_path().expect("spilled").to_path_buf();
            assert!(path.exists());
            path
        };
        assert!(!path.exists(), "the spill file went with the buffer");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }

    /// The boot sweep removes what a dead process left and touches nothing else: not a live
    /// process's file, not a stranger's file, not a malformed name.
    ///
    /// A file bearing OUR pid is swept too — a successor can be handed its predecessor's pid, and
    /// at boot we hold no buffer, so such a file is by definition stale. Files this process is
    /// actively writing are named explicitly instead.
    #[test]
    fn the_boot_sweep_removes_only_a_dead_predecessors_spill_files() {
        let dir = Scratch::new("sweep");
        // pid_max on Linux is at most 2^22, so this pid can never exist.
        let dead = dir.path().join("circuits-txn-42-999999999.ndjson");
        let recycled_pid = dir.path().join(format!("circuits-txn-42-{}.ndjson", std::process::id()));
        let alive = dir.path().join(format!("circuits-txn-7-{}.ndjson", parent_pid_or_self()));
        let stranger = dir.path().join("some-other-tool.ndjson");
        let malformed = dir.path().join("circuits-txn-nope.ndjson");
        for p in [&dead, &recycled_pid, &alive, &stranger, &malformed] {
            std::fs::write(p, b"{}\n").unwrap();
        }
        assert_eq!(sweep_spill_dir(dir.path()), 2);
        assert!(!dead.exists(), "the dead process's file is gone");
        assert!(!recycled_pid.exists(), "a file bearing our pid that we do not hold is stale too");
        assert!(alive.exists(), "another live engine's buffer is untouched");
        assert!(stranger.exists(), "a file that is not ours is never considered");
        assert!(malformed.exists());
    }

    /// A spill file this process is actively writing survives a sweep, even though it carries our
    /// pid — the ingestor sweeps once at boot, but the guard has to be explicit, not incidental.
    #[test]
    fn the_sweep_keeps_a_spill_file_this_process_is_writing() {
        let dir = Scratch::new("sweep-live");
        let mut buf = TxnBuffer::new(88, cfg(dir.path(), 128, 1 << 20));
        for i in 0..8 {
            buf.push(env_of(&format!("k{i}"), 32), 10).unwrap();
        }
        let live = buf.spill_path().expect("spilled").to_path_buf();
        assert_eq!(sweep_spill_dir_except(dir.path(), std::slice::from_ref(&live)), 0);
        assert!(live.exists());
        // Without the guard the same file would go (our pid, no evidence it is in use).
        assert_eq!(sweep_spill_dir(dir.path()), 1);
        assert!(!live.exists());
    }

    /// A pid that is certainly alive and is not ours where possible (the test's parent), else ours.
    fn parent_pid_or_self() -> u32 {
        let ppid = unsafe { libc::getppid() } as u32;
        if ppid > 1 && ppid != std::process::id() && process_alive(ppid) { ppid } else { std::process::id() }
    }

    /// `EPERM` from `kill -0` means the process EXISTS but belongs to another user. Reading it as
    /// dead would let one user's engine delete another's live spill file. pid 1 is the one process
    /// guaranteed to exist and (outside a container running as root) not to be ours.
    #[test]
    fn a_process_owned_by_another_user_counts_as_alive() {
        assert!(process_alive(1), "pid 1 always exists, whether or not we may signal it");
    }

    /// The spill directory is created 0700 and files inside it 0600: they carry row data, and the
    /// system temp dir is world-readable.
    #[test]
    fn spill_files_are_private() {
        use std::os::unix::fs::PermissionsExt;
        let base = Scratch::new("perms");
        let dir = base.path().join("nested");
        let mut buf = TxnBuffer::new(21, cfg(&dir, 128, 1 << 20));
        for i in 0..8 {
            buf.push(env_of(&format!("k{i}"), 32), 10).unwrap();
        }
        let path = buf.spill_path().expect("spilled").to_path_buf();
        assert_eq!(std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
    }

    /// The boot probe is what turns an unwritable spill dir into a refused boot rather than an
    /// ingest loop that fails every large commit.
    #[test]
    fn the_boot_probe_accepts_a_writable_dir_and_refuses_an_unusable_one() {
        let base = Scratch::new("probe");
        let good = cfg(&base.path().join("fresh"), 1024, 4096);
        good.probe().expect("a creatable, writable dir");
        assert!(base.path().join("fresh").exists(), "the probe creates the dir");

        // A path whose parent is a FILE can be neither created nor written.
        let blocker = base.path().join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let bad = cfg(&blocker.join("under-a-file"), 1024, 4096);
        let err = bad.probe().expect_err("cannot create a dir under a file");
        assert!(format!("{err:#}").contains("spill"), "{err:#}");

        // Spilling disabled: the dir is never used, so it is never probed.
        cfg(&blocker.join("under-a-file"), 0, 4096).probe().expect("no spilling, no probe");
    }

    #[test]
    fn spill_file_names_are_recognised_by_pid() {
        assert_eq!(spill_file_pid("circuits-txn-42-1234.ndjson"), Some(1234));
        assert_eq!(spill_file_pid("circuits-txn-42-1234.txt"), None);
        assert_eq!(spill_file_pid("circuits-txn-1234.ndjson"), None); // no xid part
        assert_eq!(spill_file_pid("other-42-1234.ndjson"), None);
    }

    #[test]
    fn config_defaults_and_overrides() {
        let d = TxnBufferConfig::resolve(|_| None).unwrap();
        assert_eq!(d.memory_bytes, 128 * 1024 * 1024);
        assert_eq!(d.append_bytes, 64 * 1024 * 1024);
        assert_eq!(d.spill_dir, default_spill_dir(), "a private per-uid subdir, not the shared temp dir");
        assert!(d.spill_dir.starts_with(std::env::temp_dir()));

        let c = TxnBufferConfig::resolve(|k| match k {
            "ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES" => Some("4096".to_string()),
            "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES" => Some("16384".to_string()),
            "ELECTRIC_CIRCUITS_TXN_SPILL_DIR" => Some("/var/tmp/circuits".to_string()),
            _ => None,
        })
        .unwrap();
        assert_eq!(c.memory_bytes, 4096);
        assert_eq!(c.append_bytes, 16384);
        assert_eq!(c.spill_dir, PathBuf::from("/var/tmp/circuits"));

        // 0 = never spill, an accepted setting (unlike a 0 append budget).
        assert_eq!(
            TxnBufferConfig::resolve(|k| (k == "ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES").then(|| "0".to_string()))
                .unwrap()
                .memory_bytes,
            0
        );
    }

    /// A misconfigured append budget is boot-fatal, never silently replaced by the default.
    #[test]
    fn an_unusable_append_budget_is_refused() {
        let too_big = (DS_MAX_BODY_BYTES + 1).to_string();
        let err =
            TxnBufferConfig::resolve(|k| (k == "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES").then(|| too_big.clone()))
                .expect_err("above the durable-streams body cap");
        assert!(format!("{err:#}").contains("request-body cap"), "{err:#}");

        let err =
            TxnBufferConfig::resolve(|k| (k == "ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES").then(|| "0".to_string()))
                .expect_err("a zero budget cannot append anything");
        assert!(format!("{err:#}").contains("positive byte count"), "{err:#}");

        let err =
            TxnBufferConfig::resolve(|k| (k == "ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES").then(|| "128MiB".to_string()))
                .expect_err("not a byte count");
        assert!(format!("{err:#}").contains("ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES"), "{err:#}");
    }
}
