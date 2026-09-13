# Electric Circuits (fork)

A reactive sync engine: application writes go to Postgres, the engine turns logical-replication
changes into live, incrementally maintained shapes, and durable streams are the log between them. This
glossary fixes the terms the fork relies on; the architecture itself is in `docs/ARCHITECTURE.md`.

## Language

**Table**:
A Postgres relation identified by its schema and name together. Canonical spelling is `schema.name`;
a bare name is only shorthand for `public.<name>` at the API boundary.
_Avoid_: bare table name, relation OID

**Native path**:
The engine's own control plane (`POST /shapes`, the predicate AST) plus reads straight from durable
streams — the surface the fork develops. Upstream's docs call it the extended API.

**Compat adapter**:
The Electric-protocol `GET /v1/shape` surface, maintained for upstream parity only.
_Avoid_: Electric path, legacy API

**Shape**:
A live, incrementally maintained selection of a table's rows, materialised as one stream.

**Active / Dormant / Evicted**:
The retention lifecycle of a shape — maintained live; parked with its stream retained and no engine
state; removed entirely.

**Retirement**:
The engine's own removal of a shape stream (eviction, purge, schema drift, epoch reset, or a boot
restore that finds the shape definitively gone), always closing the stream before deleting it.
_Avoid_: invalidation, drop (for streams)

**Epoch**:
One binding of the engine to a replication slot, and the whole world of shapes and streams built on
it. An epoch break is a slot the engine can no longer trust; recovery is a new epoch.

**Change log**:
The single ordered stream of committed changes the ingestor appends to and the sequencer reads from,
rotated into **segments**.
_Avoid_: table stream

**Schema drift**:
A difference between a table's compiled schema and what Postgres now reports for it.

**Subscription**:
One caller's named claim on a shape, identified by a caller-chosen id. Creating or releasing it twice
is the same act once; it stays live only while renewed within the idle window (a **lease**).
_Avoid_: refcount (for the caller's side), handle (that is what the caller receives)

**Lease**:
The liveness of a subscription: it counts only if created or renewed within
`ELECTRIC_CIRCUITS_SHAPE_IDLE_SECS`. Renewal is the same `POST /shapes` with the same subscription id.
