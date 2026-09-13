# electric-circuits — architecture

The as-built system architecture. Companion documents:

- **[ivm-engine-internals.md](ivm-engine-internals.md)** — the engine's execution strategies and the
  analytical cost model (what grows with shapes/users/rows).
- **[live-queries-guide.md](live-queries-guide.md)** — the user/integrator guide.
- **[deployment-postgres.md](deployment-postgres.md)** — running against your Postgres.

---

## 0. System in one diagram

```
  app ──ordinary SQL writes──▶ POSTGRES (system of record; wal_level=logical)
                                  │ logical replication (streaming pgoutput slot, REPLICA IDENTITY FULL)
                                  ▼
                               INGESTOR (replication.rs)
                                  │ decode commits → envelopes stamped (commit LSN, xid, seq)
                                  │ append, then acknowledge (append-then-acknowledge)
                                  ▼
                               DURABLE STREAMS  changes            (ONE ordered change log, commit order)
                                  │ tail (single LSN-ordered sequencer; global (lsn,seq) de-dup)
                                  ▼
                               ENGINE (engine/)
                                  │ Z-set delta → key routing ⊕ stateless filters
                                  │              ⊕ subquery registry ⊕ aggregations
                                  │ reliable append (retry-until-landed)
                                  ▼
                               DURABLE STREAMS  shape/<id>         (one feed per DISTINCT shape)
                                  │ read / long-poll
                                  ▼
                               CLIENTS
                                  ├─ @electric-circuits/client  (shapes, subset queries, aggregations)
                                  └─ ElectricSQL client     (GET /v1/shape on the engine)
```

Three ideas carry the whole design:

1. **Postgres is the system of record; the engine holds no copy of any table.** The engine keeps
   per-shape routing metadata and shared subquery inner-sets only; shape backfills and membership
   query-backs read just the matching rows from Postgres (pooled, parallel). The circuit's counts
   pipelines (§6b) are *derived*, in-memory, reseed-on-boot state — never the record of truth.
2. **Everything between layers is an append-only stream.** The write path (replication → table
   streams) and the read path (shape streams → clients) never talk directly; the engine is a
   restartable consumer in the middle.
3. **Every maintained result is de-duplicated.** Two equal shapes — same table, canonical predicate,
   projection, and kind — share one maintained stream, ref-counted. Identical subqueries share one
   inner-set node. Identical aggregations share one running fold. The engine maintains and appends
   once for all subscribers.

---

## 1. Components

- **durable-streams** — append-only, offset-addressed JSON streams with long-poll tailing. One
  segmented `changes/<n>` change log for all tables (the write log; the envelope's `type` carries the table's
  canonical `schema.name` — always qualified, see ADR-0002),
  one `shape/<id>` stream per distinct shape (the
  result feed). The decoupling boundary between write and read paths.
- **engine** (`apps/engine`, Rust) — the core: replication ingest, per-change Z-set deltas, fan-out to
  shapes/subqueries/aggregations, the control-plane HTTP API, and the Electric-compatible
  `GET /v1/shape` endpoint.
- **API** (`apps/api`, tRPC) — the extended surface used by `@electric-circuits/client`: `schema.define`,
  `ingest.write` (library mode), `shapes.create/get/delete`, `subset.query/live`, `aggregate`.
- **client** (`packages/client`) — `shape()` (a live TanStack DB collection), `subset()` (an ordered,
  windowed page + a shared live tail), `aggregate()` (a live scalar), typed writes, `awaitTxId`.
- **oracle + conformance** (`packages/oracle`, `packages/conformance`) — a Postgres/pglite reference
  implementation and the harness asserting engine ≡ oracle for the same op stream, through the real
  API + client, including live replication, fuzzing, NULLs, and concurrent writers.

---

## 2. Data model

- **`Value`** (`value.rs`) — `Int | Float | Text | Bool | Null`. NULL is first-class (three-valued
  logic). **`Row`** = positional `Vec<Value>`; the schema names the positions.
- **Z-set delta** — `Vec<Tup2<Row, ZWeight>>`, `ZWeight` a signed i64: insert = `(row,+1)`, delete =
  `(old,−1)`, update = `(old,−1),(new,+1)`. `old` comes from the replication envelope
  (`REPLICA IDENTITY FULL`), so no local table state is needed to retract a row. **Library mode
  (no Postgres) is the one exception**: the native write API sends `(table, op, pk, row)` and a
  delete/update has no prior row to carry, so the sequencer keeps the current row per key
  (`TableExec::library_rows`) and stamps it as `old` on the way in — per envelope, after the
  de-duplication highwater and ahead of the pending-shape buffers, the fan-out and the aggregate
  folds, so everything downstream sees a change indistinguishable from a replicated one. In memory
  and reported by `GET /memory`, and **exact from boot**: library mode has no catalog checkpoint to
  resume from, so a starting process replays the change log from its origin and rebuilds the view
  in full. Postgres mode allocates none of it.
- **Absolute emission** covers the one reader that is not at the log's head. A shape reactivating
  out of dormancy replays the change log from *its* resume position, where the per-key view does
  not apply, so each old-less envelope states that shape's membership outright: matches the
  predicate now ⇒ `upsert`, otherwise ⇒ `delete <key>` (`engine::output::absolute_envelope`; the
  rule the subquery registry uses, §6, for the same reason — a delta with no `-1` half cannot
  express a move-out). A delete for a key the shape never held is a deliberate no-op. On the live
  path the rule costs a visit to every shape on the table, so it is reserved for envelopes that
  genuinely have no before-image. What remains is library mode's lack of a **backfill**: a shape or
  aggregate created at time T holds only what changed after T (aggregates never go dormant, so the
  replay path never applies to one). The delta algebra
  is [`dbsp`](https://crates.io/crates/dbsp)'s — `Tup2` and `ZWeight` are dbsp's own, and
  `Value`/`Row` carry the `DBData` derive stack. Routing- and fallback-tier shapes are evaluated
  by plain Rust (key routing + stateless predicate evaluation; internals doc §1); the circuit
  tier (§6b) maintains the counts pipelines and the membership circuit's contributor relation,
  serving decomposable COUNT aggregates and the subquery registry's inner-set state (the
  per-feed delete gate lives host-side, `subq_feed.rs`). Row arrangements no longer exist —
  row data lives in Postgres.
- **Envelope** (`ds.rs`) — the unit on every stream:
  `{ type, key, value, old, headers{ operation, txid, offset, lsn, seq, last } }`. The ingestor stamps
  `lsn` (transaction **commit** LSN), `txid` (the Postgres **xid**), `seq` (the change's position
  within its transaction) and `last` (the transaction-end marker, on its final envelope only —
  ADR-0003, §3).
- **The envelope `key` is the row's primary key, and the encoding is injective.** A single-column
  key is the value's own string. A **composite** key escapes each component (`\` → `\\`, U+001F →
  `\x1f`) and joins them with U+001F (`schema.rs::key_string` / `join_key_components`, mirrored by
  the replication decoder's `key_from_obj` so the backfill and the live path spell the same key).
  Escaping is what makes the encoding a *function of the tuple*: with raw components, the legal
  Postgres tuples `('x', 'y␟z')` and `('x␟y', 'z')` both spell `x␟y␟z`, and `translate_output`
  de-duplicates positive rows by exactly this string — so one of the two rows vanished from every
  shape. Greenfield: there is no earlier encoding to accept (clients resync at cutovers).
- **An `int` value beyond 2^53 keeps its exact form by leaving the number space.** Postgres `bigint`
  reaches 2^63−1 and every JavaScript JSON parser decodes numbers as doubles, so `Value::to_json`
  emits a JSON **number** while `|v| ≤ 2^53−1` and an exact decimal **string** beyond it —
  the rule the aggregate `SUM` encoding already followed, now applied wherever a row value is
  serialised: shape-stream envelopes, `POST /query`, subset pages, and MIN/MAX over an int column.
  `Value::from_json` accepts the string form back, so the encoding round-trips. On the TypeScript
  side an `int` cell is therefore `number | string` (`packages/protocol` `Value`, the client's zod
  row schema); `String(v)` is always the exact decimal and `BigInt(v)` is the arithmetic form.
  **Consumers of the envelope JSON (pgxsinkit included) see this on the wire**, so anything that
  assumed `typeof value.<intcol> === 'number'` must widen.

---

## 3. Ingest: logical replication, exactly-once effect

`replication.rs` **streams** a `pgoutput` slot over the walsender protocol (push delivery — no
poll floor; the wire client is `pgwire-replication`, the message decoding is our `pgoutput.rs`).
Each transaction's changes are buffered between `Begin` and `Commit`, stamped with
`(commit LSN, xid, seq)`, appended to the change log's **current segment**, and only **then**
acknowledged to Postgres (`confirmed_flush_lsn`) — a failed append tears the connection down
unacknowledged, and the server resends from the confirmed position.

When there is no open transaction, the ingestor also acknowledges the WAL end carried by a server
keepalive. Postgres decides whether a transaction must be replayed from its commit record, so this
position may pass WAL written by a still-uncommitted transaction without skipping that transaction
if it commits later. A keepalive received after the decoder emits `Begin` and before it emits
`Commit` is not acknowledged — the commit's last durable chunk remains the only path that may
advance the slot past that decoded transaction. This lets Postgres recycle forced/archived WAL even
when no tracked table is changing without weakening append-before-ack durability.

**The buffer is bounded; large transactions spill and are appended in chunks** (ADR-0003). A
transaction cannot be appended before its commit frame (the commit LSN is unknown and it may still
abort), so it must be held — and a million-row `UPDATE` under `REPLICA IDENTITY FULL` carries old and
new for every row. The buffer holds `Envelope` structs (nothing is serialized on the way in) and
measures them as held memory; once that reaches `ELECTRIC_CIRCUITS_TXN_MEMORY_BYTES` (128 MiB) it is
serialized out to one NDJSON file under `ELECTRIC_CIRCUITS_TXN_SPILL_DIR`, memory is released, and
every further change of that transaction goes straight to the file. At the commit the transaction is
streamed back in order, stamped, and appended in chunks of at most
`ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES` (64 MiB, ≤ the durable-streams body cap) — **acknowledging
the slot, publishing `last_lsn` and releasing the drain barrier only after the LAST chunk lands**.
Peak **ingestor** memory is the cap plus one chunk (the sequencer's own read page, held run and
pending appends are bounded by the transaction's size, not by this knob), and transaction size never
invalidates a shape.

**Chunking is not visible as several transactions.** Durable-streams exposes each append atomically,
so the sequencer's long-poll returns chunk 1 on its own; splitting a page by `(txid, lsn)` alone would
fan that chunk out and flush it to the shape streams as a whole commit. The ingestor therefore stamps
a **transaction-end marker** on the last envelope of every transaction (`headers.last`, on
single-chunk commits too; library-mode writers stamp their one-envelope writes), and the sequencer
**holds** a trailing run that no marker terminates: held envelopes are carried into the next read and
the transaction is processed and flushed only when the marker arrives.

A re-delivery has to fold into that hold. When the ingestor fails part-way it acknowledges nothing,
so Postgres re-sends the interrupted transaction from its start — and, because acknowledgements are
flushed on an interval, it can re-send earlier **complete** commits first. So the "already held, skip
it" filter (`seq` greater than the last one held) applies to the **leading run of the page and only
while that run is the held transaction**: `seq` is the running index over one transaction and means
nothing against another, and a page-wide filter would silently drop whole acknowledged transactions
whose seqs happen to be lower. If the held transaction is not what came next at all — a reconnect
delivering complete commits first, or an epoch reset abandoning it — the fragment is discarded: it
will arrive again in full, and any commit already applied is skipped by the highwater.

Nothing is published past the page a held run began in — `processed`, the checkpoint, the
segment-deletion floor and a shape's dormant resume position all stay there — so a crash, or a park,
re-reads the whole transaction. A page that completes one held run and starts another re-pins to its
own page, so a catch-up over consecutive chunked commits keeps checkpointing. And the checkpoint
carries the `(lsn, seq)` de-duplication highwater **with** the position — written whenever either
moves, since the highwater advances while the position is pinned — so a prefix applied before a crash
is not applied twice. (The dormant-shape replay does not hold: it appends absolute per-pk rows, so a
partial commit is a prefix of the same rows.)

**The change log is segmented** (ADR-0006): `changes/0`, `changes/1`, … , never a bare `changes`
stream, because durable-streams offers whole-stream TTL but no prefix trimming and one ever-growing
log fills the disk. At a transaction boundary, after the commit is appended and acknowledged, the
ingestor rotates if the current segment is over its byte or age budget
(`ELECTRIC_CIRCUITS_CHANGES_SEGMENT_BYTES` / `_SECS`): create `changes/<n+1>`, append one **control
envelope** naming the successor to `changes/<n>`, close `changes/<n>`, record `ChangesRotated` in the
catalog, continue in `changes/<n+1>`. Nothing ever appends to a closed segment. Control envelopes are
recognised by TYPE (`__circuits.control`, a reserved schema no tracked table may use) and dropped
unconditionally by every reader before anything else looks at the batch, so they never reach a table
executor or a shape stream — never by position, since an abandoned rotation (the close failed) leaves
a pointer mid-segment. Every position in the log is a `(segment, offset)` pair; a rotated-out segment
is deleted once the **durable** checkpoint is past it and nothing resumes inside it (§5.1, and the
retention lifecycle for the evict-before-delete rule).

Delivery to the table streams is therefore **at-least-once** (a partial multi-table append failure,
or acknowledgements not yet flushed at a crash, re-deliver whole transactions). Deltas are *not*
idempotent for aggregates and subquery contributor weights, so the consumer side restores
exactly-once **effect**:

- **Sequencer de-duplication.** `(commit LSN, seq)` uniquely identifies a change and is strictly
  increasing on the single ordered log. The sequencer keeps a highwater mark and skips anything at
  or below it.
- The drain-barrier sentinel (`__el_sync`) is published only after its whole commit landed on the
  streams, so the barrier can never claim "drained" while a transaction is still due for re-append.

**Schema drift retires, it does not degrade** (ADR-0005). The compiled schema carries a *fingerprint*
— live columns in `attnum` order with `(name, type OID, typmod)`, plus `relreplident` and the primary
key — and Postgres re-sends a `Relation` message after any DDL that changes a table, so every `R` is
compared with it. Four triggers reach one **retirement**: a fingerprint difference, a replica identity
that is no longer FULL, a `TRUNCATE`, and the reconciler (`ELECTRIC_CIRCUITS_SCHEMA_RECONCILE_SECS`,
default 60 s — for DDL with no following DML, and for the primary key, which the wire cannot describe).
Every dependent of the table — shapes, aggregates, subquery shapes whose predicate references it — is
purged by closing then deleting its stream.

Drift additionally **re-introspects** the table, swaps the compiled schema in every holder (the shared
view, the engine registry, the subquery registry), drops the sequencer's executor for it, and records
`schemaChanged` in the catalog; an identity regression re-asserts `REPLICA IDENTITY FULL` first (with a
bounded lock wait, so one long reader cannot stall all ingest). `TRUNCATE` does none of that — nothing
about the schema changed. The ingestor **awaits** the handling before decoding further messages, so the
post-DDL DML is decoded against the new schema and no dependent is still being maintained when it lands.

Retire-then-swap leaves one race — a create already out at Postgres when the retirement enumerates its
victims. Two things close it: a per-table **schema generation** bumped inside that same critical
section (a create captures the generation of every table it reads and re-checks it before returning a
handle), and a per-table **resolve lock** held across the whole resolution (a create registering while
it is held is refused, since one arriving *after* the enumeration would pass its own generation check
and then be orphaned by the `ResetTable`). A drift that cannot be settled — Postgres unreachable, the
identity ALTER blocked on its lock — parks the table as **unresolved**: dependents retired, changes
dropped, creates refused with a retryable error, and a per-table retry task working on it until a
re-introspection succeeds. A publication that cannot deliver whole rows is *not* a runtime concern: a
column list is refused at boot and generated-column publishing is folded into the fingerprint, so wire
and catalog agree by construction.

Granularity is per table, never whole-engine; the one exception is a table with a counts pipeline,
which has no runtime circuit rebuild and so restarts the process after its retirements land (for
`TRUNCATE` as much as for drift). Because delivery is at-least-once, the restart is gated on the
triggering transaction's xid against the boot seed's `SnapshotGate`: a re-delivered transaction the
fresh seed already reflects does not restart again.

**The slot itself is bound to an epoch** (ADR-0004). The engine records `SlotBound { system_identifier,
timeline_id, slot }` in the durable catalog when it first creates — or adopts — its slot, and verifies
the slot against that binding before **every** connection, not only at boot: a slot that is gone, whose
`wal_status` is `lost`, that carries a different output plugin, or that lives in a cluster with another
`system_identifier` is an **epoch break**, and the changes it was supposed to carry are simply not
available anywhere. (A slot merely held by another walsender is not a break — the engine waits for it —
and a changed `timeline_id` is recorded, not acted on.) The default policy is auto-reset: every shape,
active and dormant, is retired (closed, then deleted), the slot is recreated and a new `SlotBound` is
written — that record is the new epoch, and clients re-subscribe.
`ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=false` refuses instead: ingest does not start, shape reads
degrade fail-closed with a named reason (`GET /replication/lsn` → `epoch.state`/`epoch.reason`), and
`POST /epoch/reset` is the operator's recovery. Reconnects back off exponentially with jitter
(1 s → 30 s), reset only by a connection that actually delivered — a rejected `START_REPLICATION`
climbs the schedule like any other failure — and cut short by an epoch rebind. A durable catalog that
cannot be READ at boot is fatal rather than treated as an empty one: deciding the epoch from a log the
engine could not read is how a fresh slot at the WAL head gets created beside shapes nobody dropped.

Unparseable values (e.g. `NaN` floats) still log errors when degraded to NULL.

---

## 4. Backfill and the consistency fence (SnapshotGate)

A shape's initial rows come from a single `REPEATABLE READ` snapshot with the predicate pushed into
the `SELECT`. Live and backfill must then be reconciled so every change counts exactly once.

**The snapshot is streamed, not materialised.** The rows arrive over a `query_raw` cursor (with
tokio-postgres's own backpressure) and are appended to the still-**pending** shape stream in chunks
bounded by `ELECTRIC_CIRCUITS_BACKFILL_APPEND_BYTES` (16 MiB), so engine memory per backfill is one
chunk whatever the table's size. No protocol change was needed: shape creation is already two-phase
(`BeginShape` registers a pending buffer, `ActivateShape` goes live), so nothing reads the stream
until activation and a failure part-way aborts the pending shape and rolls the creation back exactly
as before. An **aggregate** folds each chunk into an `AggSeed` and drops the rows — the same
`fold_agg_row` the live path uses, so the seed is arithmetically identical to feeding those rows
through it. A **subquery inner-set node's** seed is the one thing still collected whole, because that
set *is* the state the node will maintain; a subquery *shape's* outer backfill is chunked like any
other, keeping only the pk set the gated replay is fenced against. The `REPEATABLE READ` bracket, the
fence capture and `row_json_expr`'s casts are unchanged. The compat adapter's `/v1/shape` snapshot is
the one place a whole result is still held at once, and it has to be — the snapshot body *is* every
row; its sibling fold, the client's key set for a catch-up read, drops values as it goes and keeps
keys only.

**LSN comparison alone is not sound.** `pg_current_wal_lsn()` is a WAL *write* position, but snapshot
visibility is decided later, at `ProcArrayEndTransaction` (after the commit record is fsynced). A
transaction whose commit record is already in the WAL can be **invisible** to a snapshot taken during
that window — skipping its replicated change "because commit LSN < seed LSN" would drop the row from
both the backfill and the live stream, permanently. Conversely, a visible commit can sit exactly at
the boundary and be replayed as a duplicate.

The fence is therefore **transaction visibility** (`pg::SnapshotGate`): the backfill transaction
captures `pg_current_snapshot()` (xmin / xmax / in-progress xids) in the same statement that
establishes the snapshot, and the engine skips a replicated change **iff its xid was visible to that
snapshot** (every xid seen on the slot is committed, so visibility is `xid < xmin`, or
`xmin ≤ xid < xmax` and not in the in-progress list). Changes without a parseable xid (library mode)
fall back to the strict-`<` LSN comparison. Every seeded structure — routed shapes, standalone shapes,
aggregations, subquery nodes, subquery shapes — carries its own gate; `changes_only` feeds carry a
passthrough gate (no backfill ⇒ forward everything).

The backfill row representation is normalized to match the live path: text-mapped columns are read
with `::text` casts so a cell's value is Postgres's *text output* — the same form pgoutput's
text-mode tuples
prints — rather than `to_jsonb`'s (which would make the same timestamp compare unequal between a
backfilled row and its first live update).

*Known residual:* the **client-side subset seam** (§7) still positions by LSN watermarks; the same
visibility window theoretically applies to a subset page's snapshot vs its live tail and would need
the page query-back to also return the snapshot's xid list. Engine-maintained state is fully fenced.

---

## 5. The engine: fan-out, sharing, lifecycle

### 5.1 Sequencer model

ONE tokio task consumes the single ordered change log for all tables — Electric's
`ShapeLogCollector` pattern. Processing is serial in commit order (global ordering and state are
trivially correct), and each source transaction's shape appends are flushed **before the next
transaction is processed** — per-transaction atomic emission, across tables; the only intra-txn
parallelism is the append flush (bounded-concurrent, CAP=32). After a batch is fully fanned out
**and every append has landed**, the sequencer publishes its processed position — the convergence
barrier used by the conformance harness (`GET /tables/<t>/offset` reports the global position).

That position is a `(segment, offset)` pair, because **the sequencer follows rotation pointers**
(ADR-0006). A read of the current segment that comes back `closed` means the log rotated: the
sequencer finishes the batch in hand — crossing only once a read of the closed segment comes back
empty, so a page delivered alongside the close is never left behind — then continues on the segment
the batch's control envelope pointed at, from `-1`, and checkpoints the crossing immediately so a
restart resumes in the right stream. A closed segment whose pointer this process never saw (the
checkpoint it booted from was already past it) steps to **exactly** the next segment, verified to
exist; jumping to the first open segment would skip the closed ones in between, which are unread
changes. A closed segment with **no** successor is refused — logged and backed off, never skipped
past. `replay_changes_for_shape` — the dormant reactivation path — follows the same pointers, one
segment at a time, until it reaches the tail of the open segment.

Shape creation is **two-phase** so a Postgres backfill never stalls the pipeline: `BeginShape`
registers a pending shape that buffers its table's deltas; the creator runs the backfill on a
pooled connection concurrently; `ActivateShape` replays the buffer through the shape's snapshot
gate and goes live. The buffer is registered before the snapshot is taken, so no change can fall
between them.

**Nothing it reads is ever skipped quietly** (ADR-0010). Each envelope carries the digest of the schema
it was decoded under, so the sequencer — which is behind the ingestor — can recognise the two envelopes
it is right not to process: one a drift has orphaned (schema replaced, dependents retired), and one for
a table the engine no longer compiles (dropped or parked unresolved — same reasoning). Both are counted.
Anything else that will not decode — a `type` no producer could have written included — **parks** the
sequencer at that envelope: the transaction is unwound, the cursor rewound to the replay boundary,
nothing is published or checkpointed past it, and the engine latches `degraded` until an operator's
`POST /epoch/reset` — which retires every shape and restarts the replay on a fresh segment. It keeps
serving commands while parked, which is how that reset reaches it. The two replay paths — a pending
shape's buffered deltas and a dormant shape's reactivation, which reads ahead of the live loop to the
head of the open segment — apply the same rule against the one schema they hold, so neither reports a
shape live that is short a change.

### 5.2 Three execution strategies

The shape of the predicate picks the strategy (full detail + cost model: internals doc §3):

- **Equality templates** (`a = 1 AND b = 2`) → **key routing**: one shared `KeyRouter` per key-column
  set; `key_tuple → {shapes}`. Routing is O(log N), independent of shape count; zero table rows held.
- **Standalone** (ranges, OR, NOT, …) → a stateless three-valued filter evaluated directly on the
  delta. No state. A necessary-conjunct index (`(column, op)` — equality hash buckets + ordered
  range bounds) selects only the candidate shapes per change; predicates with no indexable
  conjunct (OR/NOT/LIKE/`!=` at the top) fall back to a scan list.
- **Subqueries** (`col [NOT] IN (SELECT …)`) → the cross-table registry (§6), for every
  subquery form — the registry is the one membership implementation (row data lives in
  Postgres; see §6b).

**Aggregations** (electric-circuits extension, not part of the Electric-compatible API): a scalar
COUNT/SUM/AVG/MIN/MAX over a non-subquery predicate, maintained incrementally as a fold over the
delta — COUNT/SUM/AVG hold running scalars, MIN/MAX a `value → net-weight` multiset so retractions
restore the previous extreme. A COUNT whose predicate decomposes over a counts pipeline's group
columns is served from the circuit instead (§6b). SQL NULL semantics are mirrored exactly: aggregates ignore NULL values,
`COUNT(col)` counts non-NULLs (`COUNT(*)` counts rows), AVG divides by the non-NULL count, and
SUM/AVG/MIN/MAX over zero non-NULL values are NULL. SUM/AVG accumulate **integer** values in `i128`
(`AggSum`), so a `bigint` column sums exactly rather than rounding past `f64`'s `2^53` — a single
`bigint` cell can already exceed it; a float value promotes the accumulator to `f64`. Exactness is
for INTEGER columns: `numeric` maps to `ColumnType::Float` (`pg.rs::map_pg_type`), so it sums in
`f64` like any other float. The seed
(streamed backfill) and the live fold share `fold_agg_row`, so they cannot disagree about it. The
feed carries the current value as a single-row stream (`{ value, n }`), where an exact integer sum is
a JSON **number** while it is exactly representable as one (`|v| ≤ 2^53 - 1`) and a decimal
**string** beyond that — a JSON number that large is silently rounded by every parser that decodes
into a double, so a number there would be a wrong answer that looks right. AVG stays a double.

### 5.3 Shape de-duplication (the sharing layer)

Any two **equal** shapes share one maintained stream, held by a set of named subscriptions
(ADR-0008 — "ref-counted", where the count is that set's size):

- **Signature.** Row shapes: `(table, canonical predicate, sorted projection, changes_only)` —
  predicate canonicalization is order-insensitive (`a AND b` ≡ `b AND a`). Aggregations:
  `(table, canonical predicate, function, column)`, namespaced so the two kinds never collide.
- **Join.** A create whose signature already exists adds this caller's **subscription** to the
  shape's live set and returns the *same* shape id + stream. Joiners **wait for the creator's
  backfill to land** (a watch channel in the share entry) so no caller ever sees a stream whose
  snapshot isn't readable yet — and a *failed* creation propagates to every waiting joiner rather
  than handing them a dead stream. A join abandoned mid-wait (the client disconnected while storage
  was slow) compensates with a `Left` for its own subscription — idempotent, so it cannot take
  anyone else's claim.
- **Subscriptions, not a refcount** (ADR-0008). The "refcount" is the SIZE of a per-shape set of
  caller-named subscription ids. Repeating a create with an id the shape already holds is a
  **renewal**: same handle, nothing counted, lease moved; an id held by a *different* shape is
  refused (409). Releasing names the id, so a repeat is a no-op instead of a second decrement — the
  ambiguity that used to let one client's retried `DELETE` evict a shape under another. A
  subscription is also a **lease**: native reads bypass the engine entirely, so a claim counts as
  live only while renewed within `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`, and the retention sweeper
  releases an unrenewed one exactly as an explicit delete would.
- **A join verifies the retained stream still exists** — one `HEAD` per join. The record is engine
  state; the stream is storage's, and storage can lose it (an operator `DELETE`, a restore from an
  older backup). Gone (404/410) or closed means the registry entry is stale: it is retired properly
  (`Dropped` intent → close-then-delete → deregister) and the create falls through to minting a
  fresh shape id and stream, instead of acknowledging a handle whose every read is 404. Only a share
  that has reported *Ready* is probed — a creator registers its signature before it PUTs its stream,
  so probing a pending one would kill a create in flight. `GET /shapes/{id}` deliberately does NOT
  do this: it is a metadata read on every polling client's hot path, and joining is where a dead
  handle actually gets handed out.
- **Drop.** A delete removes that subscription; the shape itself is retained and ages through the
  retention lifecycle (idle → dormant → evicted, §"Shape retention" in the engine README) — the last
  release is not a teardown. N subscribers hold the same id, each releasing its own; the client
  still enforces one-shot `close()`, but a double or retried close is now harmless on the wire
  because it names a claim that is already gone.
- **Both public surfaces share.** The Electric `/v1/shape` adapter passes `share=true` as well and
  keys its per-request live state by the SHARED shape id (`electric.rs`), so identical Electric
  definitions collapse onto one maintained stream like everything else; `share=false` remains for a
  caller that genuinely needs its own handle, and nothing in the tree currently is one. The join's
  stream-liveness `HEAD` therefore applies to `/v1/shape` too — one storage round trip per join is
  the cheaper half of that trade against handing a client a stream storage has already lost.

### 5.4 Creation is atomic; failures never leave zombies

`create_shape` returns `Ok` only after registration + backfill actually succeeded. On any failure —
backfill error, subquery seeding error, append error — the shape record, share entries, sequencer
registration, and (for subqueries) every node refcount/edge/pending-seed added by the attempt are
rolled back, waiting joiners get the error, and the stream is deleted. This structurally excludes
the "zombie shape" failure mode: a shape that is registered, streams nothing, and pins its
signature so all future identical creates silently join a dead feed.

**The closing check is taken again AFTER the catalog durability wait.** `Created`/`Joined` are
durable-before-ack (§9), and that wait is unbounded external I/O — a whole interval, externally
controllable by anyone who can make storage slow, in which a `TRUNCATE`, a schema drift, an epoch
reset or a native purge can retire the very shape the request is about to acknowledge. So
`Engine::recheck_after_durability` re-runs the degradation latch, the captured schema generations
and the epoch generation, plus the one check they never needed: **is the record still registered, on
the same stream?** (A purge removes the record without moving any generation.) One helper, six
callers — the plain, subquery, aggregate and circuit-aggregate creates and both join paths. A
mismatch unwinds through the ordinary rollback (`CreateGuard` for a create; giving the provisional
refcount back for a join) and is typed `CreateRaced`, which the create **redoes** up to three times:
the definition is still valid, only the attempt lost a race, and every client would otherwise have
to implement that loop. Exhausting the retries answers 503.

### 5.5 Reliability: appends never drop silently

A lost shape-stream append is a permanent divergence for every subscriber, so live-path appends use
`append_reliable`: transient failures retry with capped backoff (backpressuring the sequencer — the same
stance as the ingestor's read-then-commit), and the only non-retried case is a **retired** stream —
404 (deleted), 410 (soft-deleted) or 409 + `stream-closed` (retirement closes the stream before
deleting it, §5.6) — i.e. the shape was dropped/evicted mid-flush; discard is correct. Because
shape envelopes are absolute per-pk (`upsert`/`delete` by key), an ambiguous-failure double-append
is idempotent for readers.

**A terminal answer is reconciled, not taken on trust.** "404" is what a proxy, a storage router or
a failover says just as readily as a real deletion, and discarding the batch makes the sequencer
advance past a committed Postgres change — leaving a still-registered shape permanently, silently
stale. So `append_reliable` asks the engine (`Engine::reconcile_gone_shape_stream`, installed on the
streams client at construction): the shape is still registered on this stream and `HEAD` finds the
stream ⇒ the status was false, keep retrying; storage really does not have it ⇒ **retire the shape**
(`Dropped` intent, close-then-delete, deregister) so subscribers learn through 404/`stream-closed`
and re-subscribe, and only then discard. The invariant: *a registered shape's batch is never
abandoned without either landing it or retiring the shape.*

**Restore-time appends retry instead of costing the shape.** Activation's aggregate re-seed, the
circuit-aggregate seed and a dormant shape's replay are appends whose ERROR is what makes
`apply_catalog` drop and retire an acknowledged subscription — so one transient 503 during a boot
used to delete it for good. They use `append_retrying` (transient per `ds::is_unavailable`, capped
backoff, 30 s budget, joined to the shutdown token); permanent removal is reserved for a definite
refusal, a stream `HEAD` confirms is gone, or an exhausted budget.

### 5.6 Retirement closes the stream before deleting it

When the engine removes a shape stream of its own accord — purge, eviction, drop-at-restore, the
degraded subquery reap — it **retires** it (`ds.retire_stream`): first a close (`POST` with
`Stream-Closed: true`), then the delete. The close releases every waiting long-poll immediately with
`stream-closed` instead of leaving it parked until the 30 s read timeout, and "closed" unambiguously
means *the engine retired this shape; re-subscribe*. Clients **must** treat `stream-closed`, 404 and
410 alike; on the `/v1/shape` adapter the engine does that for them (a closed stream ends the live
poll with `409 must-refetch`, the evicted-handle answer). Closing is terminal, so it is applied only
to retirement: a **dormant** shape's retained
stream stays appendable for reactivation, a rolled-back create's stream was never handed to a
subscriber, and a restart keeps restored shapes on their existing streams
(`docs/adr/0007-retirement-closes-before-delete.md`).

---

## 6. Subqueries: shared inner-set nodes

(Cost model: internals doc §3.3.)

A predicate leaf `col [NOT] IN (SELECT proj FROM inner WHERE …)` routes through a registry the
sequencer feeds every table's deltas into:

- **Node** — one per distinct inner query (canonical signature), ref-counted. The node's value
  set lives in the **membership circuit** (§6b): the registry asserts each inner row's current
  contribution *absolutely* (`(node_id, pk) → value` / absent) into a dbsp **upsert map**, which
  derives the exact retract/insert deltas internally — there is no host-side reverse index to
  keep in sync. A value is in the set iff its contributor count is positive. (Assertions are
  computed host-side because evaluation can read *other* nodes' sets via nested `IN`.)
- **Templates** — nodes are grouped by parameterized template (`predicate.rs::subquery_template`):
  the inner WHERE's top-level equality literals are lifted out as a **bind**, so `user_id = 1` and
  `user_id = 2` share one compiled residual + parameter projection. A delta on the inner table is
  evaluated **once per template** (one residual eval + one bind hash-lookup per touched pk),
  routed to the single affected node — instead of one full-predicate eval per literal-keyed node.
  Flip detection is the circuit's incremental distinct: the step's output deltas ARE the flips.
- **Edges** — `node → dependent` (an outer shape, or a *parent node* for nested subqueries), labeled
  with the connecting column. When a node **flips** a value (∅→non-empty or back), the dependent rows
  with `connecting_col = value` are queried back and re-evaluated, recursing up the DAG. Flip
  propagation runs on a **semaphore-bounded worker pool** (`ELECTRIC_CIRCUITS_FLIP_WORKERS`, default 8),
  off the sequencer hot path: the Postgres query-backs run concurrently (bounded by the shared
  `ELECTRIC_DB_POOL_SIZE` pool) and never hold the registry lock. Membership evaluation and the
  **enqueue** of the resulting envelopes happen atomically under the lock, and each shape stream
  drains through one ordered **emission lane** (`engine/emission.rs`, `ELECTRIC_CIRCUITS_EMIT_LANES`),
  so per-shape append order equals evaluation order — without network under the lock. (Evaluation
  order alone is not freshness for a query-back, whose rows were read before it took the lock; the
  per-pk recency fence below closes that gap.) The engine exposes the in-flight count
  (`GET /replication/lsn` → `pendingFlips`) as the extra convergence-barrier term; it covers both
  undrained flips and enqueued-but-unlanded lane batches. A failed query-back is **retried**, and
  the retry resumes the DAG walk where the failure stopped (the walk consumes transitions, so
  restarting it from the roots would derive nothing and silently lose the dependents' moves). If
  the retries are exhausted the batch's effects are gone for good — the node already moved — so
  the engine **fails closed**: the batch never decrements `pendingFlips` (the barrier can only
  reach zero when every computed effect really landed), `flipFailures` counts it, `/v1/health`
  turns `degraded` (503), every membership-bearing route answers 503, and every subquery shape's
  durable stream is retired — closed, then deleted (§5.6) — so clients reading storage directly
  learn it too: a tailing read is released with `stream-closed` and must re-subscribe. Recovery is a
  **restart**, which re-seeds every node from Postgres; it *drops* every subquery shape (their
  inner-node state is not persisted — see the restart row of the durability table) and clients
  recreate them with `POST /shapes`.
- **Absolute emission via the per-feed key set** — the correctness rule that keeps deferred
  flips convergent: for each touched pk the registry asserts the row's *current* membership into
  the shape's **feed set** (`subq_feed::FeedSet`, one host-side Roaring bitmap per feed), never a
  history-dependent delta. An `upsert` is delivered for every matching candidate (updates to
  continuing members flow); a `delete` is delivered **only when the check-and-set actually removes**,
  so a "not a member" verdict for a pk the stream never contained is structurally a no-op — the
  never-member spurious delete (the PR #30 wake-storm) cannot be emitted at all. Flip
  propagation runs deferred (out of commit order); absolute assertion converges regardless of
  that timing — which is why the Electric-style LSN-buffering/tag protocol isn't needed here.
- **Query-back recency** — absolute assertion makes cross-pk ordering irrelevant, but the *last*
  evaluation of a given pk still has to be the freshest one. A query-back reads its candidates at
  one Postgres snapshot with the registry lock released, so a direct outer-row change committed
  after that snapshot can be evaluated and emitted in between — and the query-back's older row
  would then be the stream's last word for that pk (consumers fold by durable-stream offset, so an
  older LSN on the envelope would not repair it). While a shape has a query-back in flight it
  therefore records, per pk, the commit stamp of the live decision it applied — including
  decisions that emitted nothing, which is the case a stale query-back would otherwise silently
  undo. Each query-back drops the candidates whose recorded stamp is **not visible to its own read
  snapshot** (`SnapshotGate` xid visibility, the same fence a backfill uses — not an LSN compare):
  that decision is newer than the row in hand, so it stands. The map is bounded by the in-flight
  window and cleared when the last query-back for the shape finishes. The same fence applies to a
  parent **node**'s re-derivation, which reads its inner rows the same way: it appends nothing
  itself, but its dependent shape appends what its flips say, so a re-asserted stale contribution
  is the same permanent divergence one level down.
- **NULL sensitivity** — SQL: a NULL in the inner set makes `x NOT IN S` UNKNOWN. A NULL flip
  re-derives exactly the dependents that can change: those whose `IN` leaf is negated **or sits under
  any `Not{…}`** (with no negation above the leaf, NULL only moves the leaf between FALSE and
  UNKNOWN, and AND/OR are monotone over FALSE < UNKNOWN < TRUE, so inclusion can't change).
- **Atomicity** — node creation/refcounts/edges roll back exactly on a failed shape create (§5.4).
- **The creation window** — a subquery create registers its edges before it backfills, so a flip on
  an inner node it *shares* with an already-live shape reaches it while there is no shape to move
  yet. Such work is **queued on the pending create** and replayed once the shape is installed (both
  in one registry step, so a flip either queues or finds the shape), which is what keeps an inner
  change committed after the backfill's snapshot from being lost for the new shape. The same holds
  one tier down: a flip reaching a *parent node* the create is still seeding is **queued on that
  node** (its set is empty until the seed lands, and the seed — from an older snapshot — would be
  installed over the change), then re-derived and walked on down the DAG at install.
- **Phase-C ownership** — the create's rollback state (compile log, buffers, fresh nodes) stays in
  the registry across every await of the install, so a client disconnect anywhere in phase C —
  including after some node seeds have reached the membership circuit — leaves a pending entry the
  detached rollback unwinds exactly, retracting the partial seed with it.

---

## 6b. The circuit tier: counts pipelines + the membership circuit

The circuit tier is two small dbsp circuits per engine (O(1) — never per shape). **Row data
lives in Postgres, never engine-side.** The counts circuit is fully in-memory; the membership
circuit's contributor relation spills to disk by default (a disposable per-boot cache — see its
bullet). Neither circuit checkpoints: both reseed on boot.

- The **counts circuit** (`arrangements.rs`) maintains the configured counts pipelines
  ((group → count) relations, O(distinct groups)). The sequencer feeds each transaction into it
  and steps it **before** fanning the transaction out, so circuit-served aggregates emit within
  the transaction that changed them.
- The **membership circuit** (`subq_circuit.rs`, owned by the subquery registry, always on)
  holds the CONTRIBUTORS **upsert map** (dbsp `add_input_map`; the operator maintains the map
  and derives exact deltas from absolute assertions): `(node_id, pk_id) → value`, projected to
  `(node_id, value)` weighted by contributor count → `integrate_trace` snapshot (serves
  `contains`/`has_null`/introspection) + `distinct → output` (the step's deltas are the
  membership **flips**, §6). The per-feed key sets — the delete gate — live **host-side**
  (`subq_feed.rs`, one Roaring bitmap per feed over `u32` pk-dictionary ids): a synchronous
  check-and-set under the registry lock, dramatically lighter than the former in-circuit feed
  relation (§6). The registry evaluates templates host-side per envelope, under its lock, and
  awaits the step — intra-transaction ordering is identical to the old in-registry kernel, and
  reads are read-your-writes. Structure is fixed at construction (one generic input);
  registering templates/nodes/binds is pure runtime data — no rebuild, ever. State is
  O(contributing inner rows), bind-gated: only subscribed binds hold state, each seeded from
  Postgres like any backfill. The contributor relation **spills to disk by default** (dbsp's
  storage backend: spine batches page to layer files under a per-boot temp dir with a bounded
  buffer cache; without checkpointing the files are a disposable cache, auto-removed at
  shutdown). `ELECTRIC_CIRCUITS_SUBQ_STORAGE=0` keeps it fully in-memory;
  `ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR` pins an explicit location.

- **Counts pipelines** — `ELECTRIC_CIRCUITS_DBSP_COUNTS=table:col+col,…` compiles, per table (at
  most one spec each), a `map_index(group) → weighted_count` pipeline: a live COUNT per
  distinct projection of the group columns.
- **Serving**: COUNT aggregates whose predicate decomposes over a counts pipeline's group
  columns (a conjunction of equalities / IN-lists over group columns only) are seeded by
  summing the matching groups and updated live from each step's group deltas. SUM/AVG/MIN/MAX —
  and COUNTs that don't decompose — use the sequencer's conjunct-indexed incremental fold (§5.2).
- **Boot**: state is in-memory only, so each counts pipeline reseeds on every boot from ONE
  `SELECT <group cols>, count(*) … GROUP BY` per table under a `REPEATABLE READ` snapshot —
  O(groups), not O(rows) — and the seed's `SnapshotGate` (xid visibility) fences change-log
  replay exactly like a shape backfill.
- **Row lookups** (subquery flip re-derivations, full re-derives, membership move-ins) are
  pooled Postgres queries (`engine/membership.rs`) — parallel across the flip-worker pool,
  bounded by `ELECTRIC_DB_POOL_SIZE`. `ELECTRIC_CIRCUITS_DBSP_INDEXES` is **deprecated** and ignored
  (it configured the removed row arrangements).
- **Membership shapes** — including single-level non-negated `col IN (SELECT …)` — are served
  by the subquery registry (§6): two-phase creation (Postgres backfill + gate), shared inner-set
  nodes, flips, absolute emission. There is no separate cohort/arrangement membership tier; its
  reason to exist (local row snapshots) went away with the row arrangements.

### Configuration reference

| variable | default | meaning |
|---|---|---|
| `ELECTRIC_CIRCUITS_DBSP_COUNTS` | none | counts pipelines: `table:col+col[,…]`; at most one per table. Empty = no circuit. |
| `ELECTRIC_CIRCUITS_FLIP_WORKERS` | `8` | concurrent flip-propagation workers (Postgres query-backs). |
| `ELECTRIC_CIRCUITS_EMIT_LANES` | `8` | ordered emission lanes for subquery-shape appends. |
| `ELECTRIC_CIRCUITS_SUBQ_STORAGE` | `1` | `0` disables membership-circuit disk spilling (relations stay fully in-memory). |
| `ELECTRIC_CIRCUITS_SUBQ_STORAGE_DIR` | per-boot temp dir | explicit spill location (kept on shutdown; the default temp dir is auto-removed). |
| `ELECTRIC_CIRCUITS_SUBQ_STORAGE_CACHE_MIB` | `64` | storage buffer-cache budget, in MiB, TOTAL (dbsp uses the value verbatim, not multiplied by workers/thread-types). Bounds dbsp's own unset-default, which for this circuit's 1-worker layout would be 512 MiB (256 MiB × 1 worker × 2 thread-types). |
| `ELECTRIC_CIRCUITS_SUBQ_MIN_STORAGE_KB` | `128` | spine batches above this size page to disk. |

(The former `ELECTRIC_CIRCUITS_DBSP_DIR`/`_CACHE_MIB`/`_MIN_STORAGE_KB`/`_MAX_RSS_MB`/
`_CHECKPOINT_SECS`/`_INDEXES` storage knobs are deprecated no-ops: there is no on-disk circuit
state to tune. `ELECTRIC_CIRCUITS_FEED_TRACE` is likewise removed — the feed relation now lives
host-side (Phase 2), so there is no enumeration copy left to toggle.)

- **Observability**: `/graph` carries an `arrangements` section — the counts pipelines as
  stable-id nodes (`arr:input:<table>`, `arr:counts:<table>`, with seeded flags) plus a
  `consumers` list connecting each counts node to the circuit-served aggregates it feeds.
- **Limits**: a dbsp circuit's structure is fixed at construction, so new **counts specs**
  need a restart (state reseeds from Postgres in O(groups), so a restart is cheap); single
  worker; COUNT only. Subquery templates are NOT structure — the membership circuit's one
  tuple input serves any number of them, registered at runtime.

### The serving model this is one tier of

- **The circuit serves count templates.** Deploy-time counts pipelines, one live count per
  cohort group, never growing with shapes/users/parameter combinations. A COUNT aggregate is a
  selection/sum over those groups.
- **Routing serves query instances.** Equality templates share `KeyRouter` families; standalone
  predicates and aggregates are conjunct-indexed — a change finds its shapes by index lookup,
  never by scan.
- **The registry serves subqueries.** All `[NOT] IN (SELECT …)` shapes: shared inner-set
  nodes grouped as parameterized templates, membership state + flip detection in the
  membership circuit, parallel flip query-backs to Postgres, ordered emission lanes, absolute
  per-pk emission.

---

## 7. Subset queries and client positioning

A **subset query** is the non-materialized counterpart to a shape: one
`SELECT … WHERE … ORDER BY … LIMIT/OFFSET` page against Postgres (subquery predicates evaluated
natively by Postgres) + a **shared** `changes_only` live feed for the base predicate. Ranges live
*only* here — they are never live-tailed, so a change is matched against one base predicate, never
split across ranges. `orderBy`/`limit` are subset knobs, not shape knobs.

The client (`packages/client/src/subset.ts`) merges the page(s) and the live tail by **per-pk LSN
watermarks**: the page's snapshot LSN, and each applied delta's commit LSN. Engine output envelopes
carry their commit LSN for exactly this. Key invariants (all regression-tested):

- The feed is created and its head offset captured **before** the page snapshot, so no delta can fall
  in the gap; overlap reconciles idempotently by pk (`delta lsn ≥ snapshot lsn` applies; the engine's
  backfill-visible side is strictly below).
- **Deletes leave tombstone watermarks** (including for pks never seen): a `loadMore` page whose
  snapshot predates a delete must not resurrect the row / insert a ghost. Tombstones prune when no
  page is in flight.
- Close is one-shot; the feed is deleted with retries; a failed page query-back deletes the
  just-created feed before rethrowing (no refcount pinning).

Paging is **keyset**, and the cursor has to agree with the page query's `ORDER BY <col> <dir>,
<pk> <dir>` exactly:

- **`offset` is the first page's**, and only the first page's — later pages are reached by moving
  the cursor past the boundary row, so re-applying the offset there would skip that many rows again.
  An offset window is also closed at the **bottom**: the first loaded row is a lower bound on
  membership (kept even once the pages run out), so a live delta that moves a row into the region
  the offset deliberately skipped is dropped rather than growing the window past the page asked for.
- **NULL sort keys are a block, not a value.** No comparison is ever TRUE about a NULL, so a plain
  `col > boundary.col` cursor can neither reach the NULL block nor leave it — paging would stop at
  the first NULL-keyed row for ever. The cursor adds explicit `IS NULL` / `IS NOT NULL` arms
  positioned by the `ORDER BY` default the engine emits: **ascending = NULLS LAST**, **descending =
  NULLS FIRST**. The client's window comparator sorts NULL last for the same reason (the order
  direction flips it to first for `desc`), so `inView` and the cursor cannot disagree.
- **`limit: 0` is ended, immediately.** A page can only be "short" — the exhaustion signal — against
  a non-zero page size, and a zero-size page never moves the cursor, so `hasMore()` would otherwise
  promise a next page that could never arrive. `loadMore(0)` is a no-op that reports 0 without
  claiming exhaustion.
- **Text ordering in a subset is CODE-POINT order, not the database's collation.** Membership in the
  loaded window is decided *client-side* — the engine holds no per-page state — so the client can
  only compare the strings it received, and it cannot reproduce an arbitrary Postgres collation. The
  page query therefore spells the order the client can reproduce: `ORDER BY <col> COLLATE "C" <dir>`
  for genuinely collatable text columns (`text`/`varchar`/`bpchar`/`char`/`name`/`citext` — our
  coarse `Text` also covers `uuid`/`timestamptz`, whose order is collation-independent and which
  refuse a collation), and the keyset cursor's range comparisons carry the same `COLLATE "C"`, which
  is also what the engine's own predicate evaluation does (Rust string comparison). All three sides
  agree by construction whatever the database's default collation is. The client compares code
  points, not UTF-16 code units: `<` puts U+1F600 (surrogates `D83D DE00`) *before* U+E000, where
  every other side puts it after — enough to admit a row Postgres placed outside the window. Only
  ordering comparisons are collated; `=`/`<>` are byte equality under any deterministic collation and
  keep their index-eligible form.

  Two limits worth knowing. **The guarantee needs introspection**: the collation is emitted only when
  the column's Postgres type is KNOWN and collatable, so a table declared through `POST /schema`
  rather than introspected (`pg_type: None`) is ordered in the database's default collation — the
  declaration says "text", but the column underneath could be a `uuid`, and `COLLATE "C"` on one is
  an error, not a mis-sort. And **`COLLATE "C"` on `<`/`>` defeats a default-collation btree index**:
  on a `C`/`C.UTF-8` database the index already IS the "C" collation and nothing changes, but on
  `en_US.UTF-8` a range scan over a large table falls back to a sequential scan. The remedy is an
  expression index — `CREATE INDEX … ON t ((col COLLATE "C"))` — on the columns subsets order by.

---

## 8. Electric protocol adapter

`GET /v1/shape` (`electric.rs`) serves the ElectricSQL client protocol directly from the engine:
`table` + SQL `where` (+ `columns`) are parsed (`where_sql.rs`) into the same predicate AST used
everywhere else, identical `/v1/shape` definitions share ONE engine shape (`share=true`, so the
handle is the shared shape id), the shape stream is folded into the Electric message shape
(insert/update/delete + `up-to-date` control messages), and live requests long-poll. Handle state
is evicted after an idle TTL (`ELECTRIC_HANDLE_TTL`); the backing shape + stream are **retained**
and follow the engine's three-tier retention lifecycle (active / dormant / evicted — idle shapes
drop their engine state but keep the stream, and any touch reactivates them by change-log replay
from the captured resume offset (through the sequencer's two-phase pending-buffer handshake);
eviction **retires** the stream: closed, then deleted (§5.6), so a client tailing it is released
with `stream-closed` and must re-subscribe; see `apps/engine/src/retention.rs`). A request with an
evicted handle gets `409 must-refetch`,
which the Electric client handles by re-syncing onto the retained shape. Conformance against Electric's own oracle + integration tests lives in
`electric-conformance/` (see its README for scope and known gaps — e.g. row `tags` are not emitted;
absolute membership emission makes them unnecessary for convergence).

---

## 9. Consistency & durability model (summary)

| seam | mechanism | guarantee |
|---|---|---|
| backfill ↔ live | `SnapshotGate` (xid visibility; LSN fallback) | each change counts exactly once per shape/aggregate/node |
| ingestor → change log | append (chunked past `ELECTRIC_CIRCUITS_CHANGES_APPEND_BYTES`; contiguous on one segment, last envelope marked `headers.last`) → acknowledge **after the last chunk**; between transactions, acknowledge server keepalive WAL ends so idle WAL is recyclable; reader holds an unterminated run + `(lsn,seq)` de-dup, checkpointed together — ADR-0003 | at-least-once delivery, exactly-once effect; no transaction is acknowledged before durability, a commit of any size stays one unit of visibility, and an idle slot does not retain forced WAL indefinitely |
| engine → shape streams | `append_reliable` + offset published only after landing | no silently-lost deltas; barrier implies subscriber streams reflect the batch |
| cross-table subquery order | absolute membership emission + flip query-backs | convergence independent of deferred-flip timing |
| shared shapes | signature + a SET of named subscriptions + ready-watch + atomic rollback (create and join alike) | joiners see a live, backfilled stream or an error; a repeated create/release is one claim, not two; an abandoned join gives its own claim back |
| subscriber liveness | a subscription is a **lease**: created/renewed within `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS` (strictly — a window lasts its whole length), released by the sweeper otherwise (ADR-0008). A native subscriber renews by repeating its create; a `/v1/shape` handle is renewed by its own poll, in memory, since the engine sees those reads and the handle does not survive a restart | a client that vanished cannot pin a shape (and its stream, and its change-log segment) for ever, even though native reads are invisible to the engine; a late renewal simply re-subscribes |
| catalog event → fold | every event carries an `eid` assigned at enqueue; the boot fold applies an `eid` at most once | the writer's retry-in-place (a response lost after the append committed) can never double-apply a join, a leave, a drop or a rotation |
| subset page ↔ live tail | per-pk LSN watermarks + delete tombstones | no double-count, no resurrections/ghosts across the seam (LSN-based; see §4 residual) |
| client lifecycle | one-shot close, delete-with-retry | balanced create/drop; no refcount pinning or steal |
| client-facing mutation → catalog | **durable-before-ack** = every record a CLIENT is told about: `Created`, the `Joined` of a NEW claim, and the `Left`/`Dropped` of a native `DELETE` — awaited to storage before the HTTP answer (`CatalogWriter::send_durable`; a retry of an idempotent removal waits on the same barrier via `CatalogWriter::wait_durable`). **Queued-never-dropped** = what the engine does to itself: a *renewal's* `Joined` (that claim is already in the log), and the removals of drift, `TRUNCATE`, the epoch reset, retention and the `/v1/shape` adapter. The writer retries a transient failure in place, forever, and exits 74 on a definite refusal | an acknowledged create/join is in the durable record: a restart never turns it into an unmaintained stream — and an acknowledged release or purge is in it too, so neither comes back. That matters most under `ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS=0`, a supported setting that disables lease expiry: there is no lease repair to fall back on. A queued record cannot be lost, only delayed — and if a process dies with one still queued, the **lease** reconverges it: the shape comes back with its subscriptions' restored ages, so a `Left` that never landed is re-applied within one idle window, and a `Dropped` that never landed leaves a shape whose stale claims lapse the same way. The cost is availability: a create, a release or a purge while storage is down **waits** rather than lying. A client that times out and gives up loses only its answer — the record still lands, and the teardown a purge promised is finished by a spawned task, not by the dropped request future |
| shape ids → streams | the boot resumes `next_shape_id` past the maximum id of every `Created` in the log, dropped ones included (`CatalogFold::max_shape_id`) | an id is never re-minted while the `shape/*` stream it named still exists: a new shape can never inherit a dead one\'s stream (and its rows), and a pending retirement can never delete a live shape\'s stream |
| shape removal → stream removal | `Dropped` (intent) is written BEFORE the retirement, `Retired` (completion) only after storage accepts the delete; failures go to a background queue that retries to completion, and every boot re-queues each `Dropped` with no `Retired` — ADR-0007 | no shape stream outlives its shape, whatever storage was doing at the moment it was retired or which process was alive at the time; this is also the orphan-`shape/*` GC, bounded by the catalog rather than a storage listing |
| change log ↔ disk | segment rotation by size/age + delete-when-nothing-can-resume (the DURABLE checkpoint past it AND no shape pinning it; a dormant shape pinning past the retain window is evicted first, a reactivating one is never evicted mid-replay) — ADR-0006 | the log is bounded without prefix trimming; no reader ever loses its place (positions are `(segment, offset)`, the pointer is followed, the current segment is never deleted) |
| engine restart | durable shape catalog (`meta/catalog`: create/join/leave (each naming a subscription + its lease time)/drop/retire + change-log position checkpoints + segment rotations, every event carrying an `eid`) | plain/routed shapes + aggregates restore without client re-registration (plain resume via replay + passthrough gates; aggregates re-seed with a fresh gate); counts pipelines reseed from a fresh group-aggregated snapshot (§6b); subquery shapes are dropped loudly (inner-node state is not persisted) and recreated by clients. The restore is all-or-nothing for anything transient — a storage or Postgres failure part-way (each stream `HEAD` retried in place first, 16 at a time) undoes what it installed and fails the boot, which backs off and retries. A failed attempt consumes and checkpoints nothing: its sequencer is spawned holding its reads until every shape has resumed, the retention sweeper starts only on success, and a boot gate answers every create/join/reactivation/release/purge 503 + `Retry-After` until the boot resolves (and `POST /schema` is refused in Postgres mode outright), so nothing else can spawn a sequencer, replace the tables or mint an id meanwhile (a restore that finds a shape or sequencer already there refuses the boot). A shape with a DEFINITIVE reason — its table out of the compiled set, its schema moved, its stream `HEAD` answering 404/410 or closed, a subquery shape — is retired (`Dropped` → close-then-delete) and the rest restore around it (ADR-0009): a restart never serves an empty or partial registry over a catalog that names more, never retires a healthy shape over a blip, and never refuses the whole engine over one shape that is gone |
| compiled schema ↔ Postgres | fingerprint compare on every `Relation` + a 60 s catalog reconciler; drift/TRUNCATE/identity regression retires that table's dependents, and a per-table schema generation refuses any create that overlapped it (ADR-0005) | never serves rows over a schema Postgres no longer has; the catalog records `schemaChanged` as the audit trail for the drops |
| shape record ↔ schema at restart | each `ShapeRecord` carries its table's fingerprint; the catalog restore compares it with boot introspection | a migration applied while the engine was down retires that table's shapes instead of resuming streams shaped by the old schema |
| graceful shutdown | `SIGTERM` → readiness 503 (drain window) → stop accepting → ingestor finishes the commit it is APPENDING → sequencer finishes its batch, flushes, writes a final `Offset` → catalog drains → exit 0 (71 if it does not drain); bounded by a watchdog armed at the signal, second signal exits 70 | a planned stop costs a bounded, de-duplicated replay at worst and nothing at best; shape streams are never closed or deleted (a restored shape continues its stream). The slot is never advanced BY the shutdown: the wire ack rides the client's 1 s status interval, so the last second's commits are re-delivered and dropped by the sequencer's `(lsn, seq)` highwater |
| engine ↔ replication slot | `SlotBound { system_identifier, timeline_id, slot }` in the catalog, verified before every connection; auto-reset or fail-closed refusal on a break (ADR-0004) | a slot lost to a restore, `max_slot_wal_keep_size`, an upgrade or an operator is never silently recreated at the WAL head: every shape is retired into a new epoch, or the engine refuses until one is |
| change-log envelope ↔ the schema that decodes it | every data envelope carries `headers.schema` — the digest of the compiled `SchemaFingerprint` the ingestor decoded it under (never on an output envelope); the sequencer compares it with the schema about to decode it, and with the CURRENT one when they differ (ADR-0010). The live loop, a pending shape's buffered replay and a dormant shape's reactivation replay all apply it | exactly two envelopes may go unprocessed, and both are already orphaned: one whose schema a drift replaced (`sequencer_stale_schema_skipped_total`) and one whose table the engine does not compile (`sequencer_unknown_table_skipped_total`) — in both cases the dependents were retired (ADR-0005). A stale executor is refreshed rather than skipped past, an unstamped envelope is decoded as current, and a stamp that is not exactly 16 lowercase hex characters counts as unstamped rather than as some other schema — a position-based fence could do none of this, because the schema swap happens mid-transaction |
| sequencer ↔ a change it cannot process | the transaction is unwound (highwater restored, counts deltas never applied, nothing flushed), the cursor rewound to the replay boundary (the page, or the page a held run began in), nothing published or checkpointed past it — on shutdown either — and the engine latches `EpochBreakReason::ChangeLogUnprocessable`, which is never auto-reset (ADR-0010) | a change the schema says the engine should be able to process is never skipped: no shape is maintained past it, `/ready` and `/v1/health` say `degraded`, every shape route answers 503, and `GET /metrics` + `/replication/lsn` name the exact envelope. A restart re-derives the same park rather than stepping over it; the sequencer keeps serving commands so the recovery can reach it |
| epoch reset ↔ the change log | after retiring every shape the reset force-rotates the log, restarts the sequencer at the start of the fresh segment (`SequencerCmd::Jump` — executors and pending creations dropped, no highwater) and records that position durably with the drops (ADR-0004, ADR-0010) | nothing written under the epoch that ended is ever read again — including the envelope a parked sequencer stopped on, which is what makes `POST /epoch/reset` a recovery rather than a loop. A reset whose cause was not the slot keeps the slot (healthy, and held by our own walsender) and rebinds it |

The invariant the conformance suite asserts end-to-end: *for any shape and any op stream, the
client-materialized set equals the oracle's `SELECT … WHERE <predicate>`* — through the real API,
stream, and client, including live replication, batched mutations, NULLs, and concurrent writers.

---

## 10. Threading model

| unit | threads | notes |
|------|---------|-------|
| engine main | tokio multi-thread | sequencer + flush run here |
| sequencer (all tables) | 1 task | commit-ordered change processing; per-txn atomic flush (holds a trailing run until its transaction-end marker arrives) |
| shapes (any kind) | **0** | no per-shape thread or circuit |
| replication ingestor | 1 task | stream pgoutput/decode/buffer (spilling past the memory cap)/append in chunks/acknowledge |
| subquery registry | 0 (a mutex) | eval + emission-lane enqueue under it (in-memory only; no network under the lock) |
| flip workers | ≤ `ELECTRIC_CIRCUITS_FLIP_WORKERS` tasks (default 8) | concurrent deferred query-backs; PG round-trips never hold the registry lock |
| emission lanes | `ELECTRIC_CIRCUITS_EMIT_LANES` tasks (default 8) | per-stream FIFO writers: append order = eval order per shape |
| circuit (counts) | 1 OS thread | owns the `DBSPHandle`; blocking steps, fed by a bounded channel (backpressure to the sequencer) |
| circuit (membership) | 1 OS thread | owns the membership `DBSPHandle`; stepped per envelope by the registry (subquery tables only) |

Threads are flat in the number of shapes *and* in the number of equality templates.

**Shutdown is cooperative** (`src/shutdown.rs`): one `tokio::sync::watch` token on `Engine`, flipped
once by the binary's signal handler, joined by every select that could otherwise block — the
sequencer's change-log long-poll, the ingestor's `recv` (and its reconnect backoff), and the
`/v1/shape` live poll — so a parked long-poll returns in milliseconds instead of pinning the
termination grace for its full window. The ingestor and the sequencer each register a named
**party**; the binary waits for both (bounded), then drains the catalog writer, then exits. The
ingestor's only safe point is *between* messages, so a commit that is being appended runs to
completion; a transaction still buffering just stops, having appended nothing, and Postgres
re-delivers it. Acknowledgement is local: standby feedback rides the replication client's 1 s status
interval and is not forced on the way out, so a shutdown never advances the slot and the last
second's commits come back on the next boot, where the highwater drops them.

The grace is a bound on the **process**, not on one await point: a watchdog is armed the instant the
token flips, and forces the exit (naming whoever was outstanding) wherever the process happens to
be. Everything that could block for long joins the token too — the boot's connect/introspect/catalog
fold, a backfill between chunks, the sequencer's read backoff — so the watchdog is a backstop rather
than the mechanism.

---

## 11. Telemetry

- `GET /metrics` — atomic counters (`envelopes_processed`, `shape_appends`, `family_steps`,
  `txn_spills_total` / `txn_spill_bytes` / `txn_chunked_appends_total` for large transactions,
  `backfill_chunked_appends_total` for streamed backfills, `sequencer_orphan_fragments_total`) +
  gauges (`changes_segments_retained`, `sequencer_held_run`, `shutdown_in_progress`, and the
  replication-slot trio below) + log-bucket latency histograms (`process_envelope`, `family_step`,
  `append`) with p50/p99/p999/max.
- **Replication-slot gauges**, engine-owned and sampled every ~10 s on a *pooled* connection (never a
  dedicated one): `replication_slot_retained_wal_bytes`
  (`pg_current_wal_lsn() - restart_lsn` — the WAL the source database holds on disk for this engine),
  `replication_confirmed_flush_lag_bytes` (`… - confirmed_flush_lsn` — ingest lag) and
  `replication_slot_active`. The same sample feeds StatsD, so the numbers exist with or without it.
- `GET /memory` + OTel gauges (`engine_shapes`, `engine_subquery_nodes`, `engine_subquery_contributors`,
  `engine_family_circuits`, …) — the cardinalities that drive RSS. `GET /metrics/prometheus` exports
  those **and** every counter/gauge above (it used to carry only the memory/cardinality half), so it
  is a complete scrape target.
- **Probes:** `GET /health` is liveness (`ok` while the process runs, and never more than that);
  `GET /ready` is readiness (200 `active`, else 503 `waiting` / `starting` / `degraded` /
  `shutting_down`); `GET /v1/health` is unchanged Electric-fleet parity. The HTTP surface comes up
  before Postgres so readiness is answerable while the boot is still retrying.
- `GET /graph`, `GET /graph/node?sig=…`, `GET /shapes/{id}/rows` — the live pipeline topology + node
  indexes + shape contents, consumed by the **pipeline explorer** (`apps/pipeline-viz`).
- `GET /state`, `GET /state/node?id=…` — per-node live state: summaries for every pipeline node
  (offsets, emit counters, routing-index/inner-set cardinalities, fold values) and on-demand deep
  dumps (a family's routing index contents, an aggregate's fold internals incl. the MIN/MAX
  multiset). Summaries are also pushed as `{"type":"state"}` events on `/trace` after each batch,
  which is what makes the explorer's node chips reactive without polling.
- `GET /tables/<t>/families`, `GET /subqueries` — sharing topology (proof that N shapes share one
  router/node).

---

## 12. Potential speedups

The engine's internal per-change cost stays small even at a large shape count; the end-to-end
ceiling under load is **storage throughput** (the single-process durable-streams test server), not
engine compute.

**Storage / append path (current ceiling)**
1. Multi-stream append (one request, many streams) — fan-out to M streams is M HTTP requests today.
2. HTTP/2 multiplexing / persistent pipelined connections to storage.
3. Shard the sequencer's fan-out (partition a table's shapes/key-space across tasks). (Subquery
   flip propagation is already parallel: worker pool + ordered emission lanes.)
4. ~~A production durable-streams backend (the old Node test server fsynced per append).~~ Done:
   the streams layer is the Rust `durable-streams` server (group-commit WAL; `packages/ds-rust`
   wrapper).

**Standalone evaluation (O(K) per change)**
5. ~~Predicate indexing by `(column, op)` — turn O(K) into output-sensitive.~~ Done: standalone
   shapes are indexed by a necessary conjunct (equality → hash bucket, range bound → ordered
   scan); only candidates are evaluated per change. Un-indexable predicates (OR/NOT/LIKE/`!=`)
   remain on a fallback scan list.
6. Widen the shared class beyond pure equality (e.g. single-column range templates).

**Engine compute / representation**
7. ~~Backfill connection pooling for burst shape creation (the fleet benchmark's p99 driver).~~
   Done: backfills/query-backs/subset queries share a per-URL pool (`ELECTRIC_DB_POOL_SIZE`, default 20).
8. Intern stream paths/txids; pack `Value` (smaller enum, interned strings).

---

## 13. Client query layer (two-level querying)

There are **two query layers** with different jobs:

1. **Server-side shape predicate** — *what crosses the network*. One table + a `WHERE` over its
   columns (+ subqueries), optionally narrowed by a `columns` projection (sync only what a view
   needs; the pk is always included). The engine maintains exactly this set on the shape stream.
2. **Client-side live query** (TanStack DB `useLiveQuery` over the materialized collection) — *how
   it's presented*: ordering, text search, finer filtering. Maintained incrementally on the client;
   a refinement (typing in a search box) never touches the engine or re-syncs.

**Windowed / infinite-scroll sync** uses **subset queries** (§7): each page is a bounded keyset range
query (`col < lastSeen OR (col = lastSeen AND id < lastId)` folded into the `WHERE`), no stateful
top-N anywhere. The render layer is virtualized, so a 100k-row deployment stays a few dozen DOM nodes.
For permissioned/faceted lists, prefer **per-facet feeds reused across filter changes** + a client
merge (identical predicates across users ⇒ shared engine families) over folding UI filters into the
predicate (which recreates the feed per click) — see AGENTS.md "gotchas".

## 14. File map

| path | role |
|------|------|
| `apps/engine/src/engine/` | the engine module: `mod.rs` (the `Engine` handle + shared state), `sequencer.rs` (the LSN-ordered sequencer, (lsn,seq) de-dup, per-txn reliable flush), `lifecycle.rs` (shape creation/sharing/retention), `circuit_serving.rs` (circuit-tier serving), `executors.rs` (routers, filters, folds), `planning.rs` (circuit placement), `catalog.rs` (durable catalog + restore), `drift.rs` (schema drift / TRUNCATE retirement + the reconciler), `introspection.rs` (graph/state DTOs + builders), `membership.rs` (the shared membership kernel: flip detection, pooled Postgres query-backs), `emission.rs` (per-stream ordered emission lanes), `output.rs` (envelope ⇄ delta codec) |
| `apps/engine/src/subquery.rs` | subquery registry: shared nodes + templates, edges, absolute emission, atomic create/rollback |
| `apps/engine/src/subq_circuit.rs` | the membership circuit: inner-set state + flip detection (dbsp distinct) |
| `apps/engine/src/arrangements.rs` | the circuit: in-memory dbsp counts pipelines, group-aggregated boot seeding (§6b) |
| `apps/engine/src/replication.rs` | ingestor: streaming pgoutput (decoder: `pgoutput.rs`), per-txn buffering, (lsn, xid, seq) stamping, append-then-acknowledge, schema-drift/TRUNCATE reporting (`SchemaEvents`) |
| `apps/engine/src/pg.rs` | connect/introspect (+ schema fingerprints), slot + REPLICA IDENTITY, backfill (+ `SnapshotGate`), subset query-back, value normalization |
| `apps/engine/src/predicate.rs` | predicate compile, three-valued eval, equality templates, subquery signatures |
| `apps/engine/src/sql.rs` / `where_sql.rs` | predicate → SQL (pushdown) / SQL `WHERE` → predicate (Electric path) |
| `apps/engine/src/electric.rs` | Electric `/v1/shape` adapter (handles, offsets, TTL eviction) |
| `apps/engine/src/ds.rs` | durable-streams client: `append`, `append_checked`, `append_reliable`, `head`, `close_stream`, `retire_stream`, `delete_stream`, reads |
| `apps/engine/src/changelog.rs` | the segmented change log (ADR-0006): `LogPosition`, the control envelope, the rotation writer + boot walk-forward, the segment-deletion planner |
| `apps/engine/src/http.rs` | control-plane HTTP |
| `apps/engine/src/retention.rs` | shape retention: the active / dormant / evicted lifecycle + layered dormant-only eviction |
| `apps/engine/src/config.rs` | boot config: `ELECTRIC_CIRCUITS_*` env + Electric fleet-surface mapping |
| `apps/engine/src/params.rs` | Electric `params[N]` / `$N` substitution for `/v1/shape` |
| `apps/engine/src/statsd.rs` | StatsD (datadog wire) telemetry for the benchmarking fleet |
| `apps/engine/src/trace.rs` | per-envelope pipeline trace broadcast (`GET /trace` SSE, feeds the explorer) |
| `apps/api/src/core.ts` | extended API core (writes, shape/subset/aggregate forwarding) |
| `packages/client/src/index.ts` | client: shapes/aggregations, tracked lifecycles, `awaitTxId` |
| `packages/client/src/subset.ts` | subset queries: page merge, LSN watermarks, tombstones, feed lifecycle |
| `docker/` | containerized stack (engine, durable-streams, API, Postgres) |
| `apps/pipeline-viz` | live pipeline explorer over `GET /graph` + `/state` + `/trace` |
