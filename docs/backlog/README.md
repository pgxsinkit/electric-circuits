# Backlog

The documented ledger for work we deliberately are **not** doing now: parked investigations (with
their evidence), improvement candidates, and escape-hatched designs. Same rules as pgxsinkit's
`docs/backlog`:

- One numbered file per item. Entries are **never deleted** — status flips instead, so a symptom
  someone trips over next year finds the prior investigation instead of restarting it.
- Every item carries a **Reopen trigger**: the concrete event or evidence that justifies picking it
  up. Until that fires, the item is settled — do not re-litigate it from scratch.
- `Status: parked` (investigated, evidence recorded, waiting on the trigger) · `candidate`
  (improvement we would take, unscheduled) · `promoted → adr/00xx` (one-line pointer to the ADR that
  superseded it) · `dropped` (decided against; keep the why).
- This directory is an engineering ledger, not user documentation.

## Items

- [0001 — A refused shape create is not logged](0001-refused-shape-create-not-logged.md) — candidate
