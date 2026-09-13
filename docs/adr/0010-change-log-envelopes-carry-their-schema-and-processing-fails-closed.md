# Every change-log envelope carries the schema it was decoded under, and a change the engine cannot process stops ingest

Status: accepted (2026-09-13)

A change the sequencer could not turn into a delta used to be **logged and skipped**: the envelope was
dropped, the highwater advanced, the rest of its transaction was flushed, and the position was
published. A shape silently missed a change, its subscribers converged on the wrong set, and the only
trace was one `ERROR` line in a log nobody reads during an incident. Four sites did it — the fan-out
(`process_envelope`), the counts tier's feed, the buffered replay a pending shape drains at activation,
and `replay_changes_for_shape`, the replay a dormant shape reactivates through — and every one of them
was a **permanent** divergence dressed up as a warning. The last is the worst of the four: that replay
runs from the shape's resume position to the HEAD of the open segment, so it reads ahead of the live
loop, and a change it skipped was served as a live shape until the live loop reached the same envelope.

There is one legitimate reason an envelope will not decode, and one more why it should not be decoded at
all. The ingestor detects schema drift
inline (ADR-0005): it retires every dependent of the table, swaps the compiled schema, and tells the
sequencer to drop its executor. But the sequencer runs BEHIND the ingestor, so it then reads envelopes
that were encoded under the OLD schema and would be decoded against the NEW one — and the decode is
strict (a number against a `text` column fails). Those envelopes belong to shapes that no longer
exist; skipping them is right. The second case is the same fact by another route: an envelope for a table
the engine does not compile **at all** — dropped, or parked unresolved (ADR-0005) — whose dependents were
retired when it left. **Everything else is an engine bug or corrupt storage.**

Position-based fences cannot separate the two: the schema swap happens mid-transaction (one commit can
carry DML and DDL on the same table), so there is no offset before which envelopes are old and after
which they are new. So the fence is the schema itself. Each table's compiled schema carries a
**digest** — FNV-1a over the JSON of its `SchemaFingerprint` (columns in `attnum` order with
`(name, type OID, typmod)`, the replica identity, the primary key) — and the ingestor stamps it on
every data envelope it appends, as `headers.schema`, 16 hex characters. Only there: an output envelope
never carries it, because the engine's own fence is no part of what a subscriber reads.

For a data envelope of table `T` reaching the sequencer:

- the stamp equals the digest of the schema about to decode it (or both are absent, which is library
  mode) → **decode**, and a failure is fatal;
- there is no stamp but the table has a digest — an envelope written by an engine from before the
  stamp existed → **decode as current**, strictly, and a failure is fatal. There is no compatibility
  branch beyond that one line: the safe direction for an envelope whose provenance is unknown is the
  loud one. A stamp that is not EXACTLY what the engine writes (16 lowercase hex characters) counts as
  absent for the same reason — a truncated or mangled one would otherwise parse as some other digest
  and be skipped;
- the stamps differ → re-read the shared schema view. If the SHARED digest is the envelope's, only
  this executor is behind (its `ResetTable` is still queued behind the page being processed): replace
  it exactly as that command would and decode. Otherwise the envelope predates a drift the engine has
  already resolved → **skip it**, count `sequencer_stale_schema_skipped_total`, and say so once per
  (table, schema) per process;
- `T` is not a table the engine compiles → **skip it**, count `sequencer_unknown_table_skipped_total`,
  once per table. Except when `T` is not a canonical `schema.name` at all: no producer the engine has
  can write one (the ingestor stamps `TableRef::to_string()`, library-mode writes go through
  `canonicalTable`), so that is corrupt storage and it PARKS. This is the one place the old code was
  most obviously wrong for the right-looking reason: it logged `ERROR change for unknown table` per
  envelope and stepped over both.

**A skip is consumption, not a loss.** The envelope's position and de-duplication highwater advance
normally, because every shape that could have wanted it was retired when the schema moved (or when the
table left). Leaving it unconsumed would park the engine on a change nothing will ever want.

**A failure parks the sequencer and latches the engine broken.** The transaction is unwound: its
highwater is restored, its staged counts deltas are never applied to the circuit (which has no runtime
rebuild), its partial output is never flushed — a subscriber must not see a fraction of a commit — and
the read cursor is rewound to where a replay must start: the page the envelope arrived in, or the page
a held run began in when one completed on the same page (ADR-0003 — its appends went out from there).
Nothing is published or checkpointed past it, on shutdown either, so a restart re-derives the same park
rather than stepping over the change. The sequencer stops READING and keeps serving commands, because
the recovery arrives as one. The engine latches `EpochBreakReason::ChangeLogUnprocessable`, so
`GET /ready` and `/v1/health` say `degraded`, every shape route answers 503, and the ingestor refuses
to reconnect — and `GET /metrics` and `GET /replication/lsn` name the envelope (table, key, txid, lsn,
position, the error chain), because "degraded" alone tells an operator nothing they can act on.

**That break is never reset automatically** — not under `ELECTRIC_CIRCUITS_RESET_ON_SLOT_LOSS=true`,
the default, and not on the ingestor's re-entry, which re-derives the latch on every reconnect
attempt. A reset destroys every shape; doing that in reply to an engine bug would do it on a loop and
throw away the evidence. The slot reasons are the opposite case — a new slot and a resync genuinely
repair them — which is what `EpochBreakReason::needs_operator` distinguishes.

**`POST /epoch/reset` is the recovery, and it now moves the replay start.** The reset already retired
every shape and force-rotated the change log; it now also restarts the sequencer at the beginning of
the fresh segment (`SequencerCmd::Jump`: executors and pending creations dropped, no highwater) and
clears the recorded failure. The sequencer **records the new position durably itself**, from its own
task, before it acknowledges the jump and before it reads again: sends from one task reach the catalog
writer in order, so a checkpoint it writes a moment later cannot overtake it. An `Offset` sent from the
engine could, and the fold is last-wins, so the durable start would be free to regress. Only a reset
with no sequencer running — at boot, before the restore spawns one — records the position from the
engine side. Without any of this a reset would leave the sequencer where it was and it would park on the
same envelope again — the reset would not be a recovery at all. This applies to **every** reset, slot loss included: the old segments belong to shapes
that no longer exist either way. One thing the reset skips when the break was never about the slot: it
keeps the slot. It is healthy, our own walsender is streaming from it (Postgres refuses to drop one an
active walsender holds), and replacing it would buy nothing — the new binding is what ends the old
epoch.

## Considered options

- Keep logging and skipping: rejected — it is a silent, permanent divergence for that shape's
  subscribers, which is the one failure mode the engine is built not to have.
- Exit the process instead of parking: rejected — a restart re-reads the same envelope and fails the
  same way, so it is a crash loop that also loses the diagnosis. Parking holds the engine at the
  failure with the evidence attached, and an operator decides.
- Retry the envelope: rejected — nothing about it changes. The decode is a pure function of the
  envelope and the schema.
- Fence by change-log position (everything before offset X is pre-drift): rejected — the swap happens
  mid-transaction, so no such offset exists. A transaction that mixes DML and DDL on one table would
  be half on each side of any fence.
- Drop the drifted envelopes at the ingestor instead: rejected — the ingestor is the only writer of the
  change log and must never decide what a reader may see; a shape whose dormant replay is behind reads
  the same segments, and a change-log the ingestor filtered would be a different log for every reader.
- Auto-reset the break under the default policy, like a lost slot: rejected — see above.
- A cryptographic digest: rejected as unnecessary. A collision would let a pre-drift envelope be
  decoded as current, which fails loudly rather than silently; 64 bits of FNV-1a is the right trade,
  and it adds no dependency.

## Consequences

`sequencer_stale_schema_skipped_total` is non-zero after a migration the sequencer was behind on, and
is bounded by the DML that migration outran; a climbing value with no migrations means something is
writing envelopes under a schema the engine does not have. `sequencer_unknown_table_skipped_total` is
non-zero after a table is dropped or parked unresolved, and is bounded by the writes that followed it.
The digest is a **durable format**: changing
its encoding — the hash, the JSON form, or `SchemaFingerprint`'s fields — changes every stored digest,
so every envelope already on the log would read as pre-drift and be skipped. An upgrade that does that
needs an epoch reset, and the golden unit test in `schema.rs` is there to make the decision deliberate.

A parked engine serves nothing and destroys nothing until an operator acts: `GET /ready` is 503
`degraded`, shape routes are 503, the change log keeps growing (the ingestor is untouched — its appends
are discarded by the reset), and the retained WAL and change-log segments grow with it, because the
durable checkpoint cannot advance past the envelope. That is the cost of not lying, and
`replication_slot_retained_wal_bytes` is where it shows.

The two replay paths apply the same rule, narrowed: each holds ONE compiled schema and cannot refresh it
from the shared view, so a mismatch is always "pre-drift, skip" there. A create whose table drifted
mid-flight therefore either skips envelopes it has no business decoding or fails loudly, and a dormant
shape's reactivation does the same — which matters most, because that replay reads from the shape's
resume position to the head of the open segment and can reach an envelope the live loop will park on.
Failing it leaves the shape dormant and refuses the read that touched it; the same envelope reaches the
main loop, which is what parks the engine.
