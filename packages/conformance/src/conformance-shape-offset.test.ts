// Resume-offset conformance for `GET /v1/shape`, against the REAL pinned durable-streams server —
// the substrate whose answers the engine has to translate. durable-streams refuses a position it
// cannot place (PROTOCOL.md §GET: 400 for a malformed offset, 410 for one below the earliest
// retained position). The engine must turn that into Electric's own answer for an unusable
// position, `409` carrying the `must-refetch` control message, so the client re-snapshots onto the
// shape it still has. Answering `500` — as it did — tells the client nothing it can act on, and a
// persisted offset that outlived the stream would keep getting one forever.
//
// The engine's own tests drive an in-process fake ds, which is exactly what hid this: a fake that
// coerces an unparseable token to zero answers a garbage offset with a full replay instead. This
// lane has the real server, so it is where the claim belongs.

import type { Schema } from '@electric-circuits/protocol'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { applyOp, bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: { items: { columns: { id: { type: 'text' }, name: { type: 'text' } }, primaryKey: 'id' } },
}

describe('conformance: an offset the stream cannot place', () => {
  let h: Harness

  beforeAll(async () => {
    h = await bootHarness(schema)
    await applyOp(h, 'items', { type: 'insert', row: { id: 'i1', name: 'one' } })
    await drainEngine(h)
  }, 120_000)

  afterAll(async () => {
    await h?.shutdown()
  })

  it('answers must-refetch, and keeps serving the shape', async () => {
    // A live handle at a real position: the snapshot is what a client persists alongside its rows.
    const snap = await fetch(`${h.engineUrl}/v1/shape?table=items&offset=-1`)
    expect(snap.status).toBe(200)
    const handle = snap.headers.get('electric-handle')
    expect(handle).toBeTruthy()
    expect((await snap.json()).filter((m: { headers: { operation?: string } }) => m.headers.operation)).toHaveLength(1)

    // The offset, however, is not a token this stream ever issued — truncated storage, a client that
    // built one itself, a token format that changed under it.
    const res = await fetch(
      `${h.engineUrl}/v1/shape?table=items&handle=${handle}&offset=${encodeURIComponent('not-a-real-offset')}`,
    )
    expect(res.status).toBe(409)
    const body = (await res.json()) as Array<{ headers: { control?: string } }>
    expect(body).toEqual([{ headers: { control: 'must-refetch' } }])

    // Which is only useful if the advice works: the shape itself is fine, and a re-snapshot serves.
    const again = await fetch(`${h.engineUrl}/v1/shape?table=items&offset=-1`)
    expect(again.status).toBe(200)
    const rows = (await again.json()) as Array<{ headers: { operation?: string }; key?: string }>
    expect(rows.filter((m) => m.headers.operation).map((m) => m.key)).toEqual(['i1'])
  })
})
