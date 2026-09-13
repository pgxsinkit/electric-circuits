// Characterization of the Durable Streams operations Electric Circuits actually uses.  This is
// deliberately a real-process lane: the wrapper resolves the same pinned durable-streams Rust
// binary used by the conformance harness and gives every test a fresh data directory and port.
//
// It pins the provider wire behaviour the engine depends on; it is not a durability or fault
// qualification of the store.

import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { DurableStreamTestServer } from './index.js'

const JSON_TYPE = 'application/json'

function stream(base: string, path: string, params?: Record<string, string>): string {
  const url = new URL(path, `${base}/`)
  for (const [key, value] of Object.entries(params ?? {})) url.searchParams.set(key, value)
  return url.toString()
}

describe('pgxsinkit durable-streams provider contract used by Electric Circuits', () => {
  let server: DurableStreamTestServer
  let base: string

  beforeEach(async () => {
    // A short server-owned long-poll deadline bounds the idle-tail characterization without
    // manufacturing ordering with sleeps.
    server = new DurableStreamTestServer({ port: 0, longPollTimeout: 50 })
    base = await server.start()
  })

  afterEach(async () => {
    await server.stop()
  })

  it('creates idempotently, appends JSON arrays atomically in order, and resumes by opaque offset', async () => {
    const path = 'circuits-provider-contract/json-and-envelope'

    const created = await fetch(stream(base, path), { method: 'PUT', headers: { 'content-type': JSON_TYPE } })
    expect(created.status).toBe(201)
    expect(created.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')

    const existing = await fetch(stream(base, path), { method: 'PUT', headers: { 'content-type': JSON_TYPE } })
    expect(existing.status).toBe(200)
    expect(existing.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')

    const first = [{ kind: 'catalog', n: 1 }, { kind: 'catalog', n: 2 }]
    const appended = await fetch(stream(base, path), {
      method: 'POST',
      headers: { 'content-type': JSON_TYPE },
      body: JSON.stringify(first),
    })
    expect(appended.status).toBe(204)
    const firstTail = appended.headers.get('stream-next-offset')
    expect(firstTail).toMatch(/^\d{16}_\d{16}$/)

    const catchup = await fetch(stream(base, path, { offset: '-1' }))
    expect(catchup.status).toBe(200)
    expect(catchup.headers.get('stream-next-offset')).toBe(firstTail)
    expect(catchup.headers.has('stream-up-to-date')).toBe(true)
    expect(await catchup.json()).toEqual(first)

    const envelope = {
      type: 'public.items',
      key: 'item-3',
      value: { id: 'item-3' },
      headers: { operation: 'upsert', last: true },
    }
    const second = await fetch(stream(base, path), {
      method: 'POST',
      headers: { 'content-type': JSON_TYPE },
      body: JSON.stringify([envelope]),
    })
    expect(second.status).toBe(204)
    const secondTail = second.headers.get('stream-next-offset')
    expect(secondTail).not.toBe(firstTail)

    // The token is opaque to Circuits: it is only sent back to this provider.  Resuming from the
    // prior returned token yields precisely the subsequent append, not a parsed byte position.
    const resumed = await fetch(stream(base, path, { offset: firstTail! }))
    expect(resumed.status).toBe(200)
    expect(resumed.headers.get('stream-next-offset')).toBe(secondTail)
    expect(await resumed.json()).toEqual([envelope])
  })

  it('reports HEAD state, gives an idle live read a bounded up-to-date result, and closes terminally', async () => {
    const path = 'circuits-provider-contract/lifecycle'
    await fetch(stream(base, path), { method: 'PUT', headers: { 'content-type': JSON_TYPE } })

    const headOpen = await fetch(stream(base, path), { method: 'HEAD' })
    expect(headOpen.status).toBe(200)
    expect(headOpen.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')
    expect(headOpen.headers.has('stream-closed')).toBe(false)

    const idle = await fetch(stream(base, path, { offset: '-1', live: 'long-poll' }))
    expect(idle.status).toBe(204)
    expect(idle.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')
    expect(idle.headers.has('stream-up-to-date')).toBe(true)
    expect(idle.headers.has('stream-closed')).toBe(false)

    const closed = await fetch(stream(base, path), { method: 'POST', headers: { 'stream-closed': 'true' } })
    expect(closed.status).toBe(204)

    const headClosed = await fetch(stream(base, path), { method: 'HEAD' })
    expect(headClosed.status).toBe(200)
    expect(headClosed.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')
    expect(headClosed.headers.get('stream-closed')).toBe('true')

    const appendClosed = await fetch(stream(base, path), {
      method: 'POST',
      headers: { 'content-type': JSON_TYPE },
      body: JSON.stringify([{ ignored: true }]),
    })
    expect(appendClosed.status).toBe(409)
    expect(appendClosed.headers.get('stream-closed')).toBe('true')

    // A close wakes the provider's long-poll protocol as terminal state.  The engine maps this to
    // a shape refetch or a change-log segment transition; it must never tail the closed stream.
    const closedTail = await fetch(stream(base, path, { offset: '-1', live: 'long-poll' }))
    expect(closedTail.status).toBe(204)
    expect(closedTail.headers.get('stream-closed')).toBe('true')
    expect(closedTail.headers.get('stream-next-offset')).toBe('0000000000000000_0000000000000000')
  })

  it('deletes idempotently and exposes missing streams as 404 for HEAD and DELETE', async () => {
    const path = 'circuits-provider-contract/delete'
    await fetch(stream(base, path), { method: 'PUT', headers: { 'content-type': JSON_TYPE } })

    const deleted = await fetch(stream(base, path), { method: 'DELETE' })
    expect(deleted.status).toBe(204)

    const missingHead = await fetch(stream(base, path), { method: 'HEAD' })
    expect(missingHead.status).toBe(404)

    const deletedAgain = await fetch(stream(base, path), { method: 'DELETE' })
    expect(deletedAgain.status).toBe(404)
  })
})
