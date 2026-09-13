# A boot restore that cannot complete fails the boot; a shape that is definitively gone is retired

Status: accepted (2026-09-13)

The durable catalog is the restart authority: every shape it names is one a client was told exists.
A restore that fails part-way used to log and **continue with an empty registry** — every catalog
shape unmaintained while the process looked healthy, and the next `Offset` checkpoint moving the
replay start past changes those shapes never saw, so a later restart could not repair them either. A
single shape that failed to resume was dropped and its stream retired on the spot, whatever the
cause — so one Postgres blip during an aggregate's re-seed, or storage hiccupping on a stream,
permanently deleted an acknowledged subscription.

The restore now separates the two things that can stop a shape resuming. **Anything that may clear by
itself fails the whole restore**: transport errors, 5xx and 429 from durable-streams (each stream
check is first retried in place), a Postgres connection error, and a stream that passed its check but
was gone by the time the resume wrote to it (the retried attempt's check retires it). Nothing is
installed, or everything installed is undone — records, shares and their subscriptions, lifecycles,
circuit placements, and the sequencer. The error reaches the boot typed, and `pg::boot_disposition`
backs off and retries it like any other dependency that is not up yet; a genuine refusal — including
a sequencer that stopped mid-restore, which is an engine bug — is still refused by name. **A
definitive answer about one shape retires that shape** and the rest restore around it:

- its table is no longer in the compiled set (`table_gone`): dropped while the engine was down
  under a wildcard selector (`schema.*`), or removed from `ELECTRIC_CIRCUITS_PG_TABLES`. A table an
  explicit entry still names refuses the boot earlier, at introspection, as the misconfiguration it
  is;
- its schema moved while the engine was down (`schema`, ADR-0005);
- storage answers its stream's `HEAD` with 404/410 (`stream_missing`) or `stream-closed`
  (`stream_closed`);
- it is a subquery shape, whose inner-node state is not persisted (`subquery`).

The order is fixed: classify what boot introspection already knows, `HEAD` every remaining record's
stream (a bounded number at a time, each retried through a transient failure, the answers used in id
order), retire everything either step condemned — `Dropped`, then close-then-delete (ADR-0007) —
then install and resume the rest as one unit. Park mode (a broken epoch, ADR-0004) is unchanged: it
asks storage nothing and parks every record for the reset to retire.

A rolled-back attempt must leave nothing that moves the replay start, or the retry would resume its
shapes after changes they never saw. Three things make that true. The restore's sequencer is spawned
**held**: it serves the registrations but reads no change until every shape has resumed, so an
attempt that fails has consumed and checkpointed nothing. The retention sweeper — which advances the
durable checkpoint on its own when no sequencer exists — starts only once the restore has succeeded.
And a **boot gate** closes every other way in: in Postgres mode, creating, joining, reactivating,
releasing or purging a shape (and reading one the restore has not installed yet) answers 503 with
`Retry-After` until the boot resolves, and `POST /schema` — library mode's way to install tables and
start a sequencer — is refused in Postgres mode outright (409). Without them, a create during a
retry's backoff would spawn an ordinary sequencer that reads the backlog with nothing registered and
checkpoints past it, or mint an id a catalog record's stream still owns. The boot itself runs once to
success: a second `setup_postgres` after one is refused. The restore therefore refuses to resume into an engine
that already holds a shape or a running sequencer: that state cannot arise, and resuming over it
would hide whatever it consumed.

A missing stream is an ordinary state, not an impossible one. A plain or subquery create registers
its record and enqueues the `Created` event before it `PUT`s the stream, and the two are not awaited
against each other (`engine/lifecycle.rs`: `send_durable(Created)` under the state lock, then
`ensure_stream` once the lock is released; the `Created` is awaited only at the end of the create). A
process killed after the append landed and before the `PUT` leaves a durable record with no stream
behind it, and no process remains to roll the create back. Storage losing a stream, or an operator
deleting one, while the engine is down ends the same way.

## Considered options

- Refuse the boot on any unresumable shape: rejected — the loss of one shape's stream, or one table
  leaving the selection, becomes an outage of every healthy shape, recoverable only by catalog
  surgery. A table that is gone fails the same way on every boot; that is ADR-0005's per-table
  retirement, and refusing the engine for it is exactly the whole-engine granularity that ADR rejects.
- Keep dropping a shape whose resume fails, whatever the cause: rejected — a transient failure is not
  evidence the shape is gone, and the drop is irreversible.
- Recreate a missing stream under its old id: rejected — the engine would present a fresh projection
  as the continuation of a stream subscribers were reading, and the durable record would no longer
  say what happened.
- Treat a `HEAD` that storage could not answer as "missing": rejected — uncertainty would delete
  healthy shapes during every storage restart.

## Consequences

A storage or Postgres outage during a restore now costs boot time rather than shapes: `GET /ready`
stays `waiting` and each attempt is logged, and an aggregate whose re-seed append exhausts its 30 s
budget fails the attempt instead of retiring the aggregate. Each attempt re-folds the catalog, so a
shape whose `Dropped` an earlier attempt recorded is not there to classify again; its retirement,
if unfinished, is re-queued once per stream, however many attempts re-enqueue it. One whose `Dropped`
had not landed yet is decided again and counted again — possibly under a different reason, since a
stream the earlier attempt already closed or deleted now reads `stream_closed` or `stream_missing`,
whatever condemned it first. Clients see 503 with `Retry-After` for the whole boot rather than a 404
or 400 for shapes that simply are not restored yet. A record that fails to resume for
any other reason — a predicate this build cannot compile, say — is not transient and not one of the
definitive answers above, so the boot is refused by name (exit 78): a pathological state for an
operator, not something the restore gets to paper over by deleting the shape.

Every retirement is logged with the shape, its table and the reason, and counted by reason:
`catalog_restore_retired_<reason>_total` on `GET /metrics`, `engine_catalog_restore_retired_total`
with a `reason` attribute on `GET /metrics/prometheus`. Non-zero `schema` and `subquery` after a
boot are expected; `table_gone`, `stream_missing` and `stream_closed` mean something outside the
engine moved, and a burst is worth a look.
