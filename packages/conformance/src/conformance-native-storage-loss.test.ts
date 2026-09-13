// Native shape handles promise a durable, appendable stream. Storage failures must not turn an
// acknowledged shape into a silently stale stream or cause a new subscriber to receive a dead URL.

import { createServer, request } from 'node:http'

import type { Schema } from '@electric-circuits/protocol'
import { afterEach, describe, expect, it } from 'vitest'

import { createShape, foldStream, pgQuery } from './engine-native.js'
import { bootHarness, drainEngine, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, payload: { type: 'text' } },
      primaryKey: 'id',
    },
  },
}

let h: Harness | undefined
afterEach(async () => {
  await h?.shutdown()
  h = undefined
})

interface AppendFault {
  armed: boolean
  status: number
  hits: number
  headArmed?: boolean
  headStatus?: number
  headHits?: number
  /** Answer this many further `HEAD`s on `/shape/*` with `headStatus`, one after another. */
  headFailures?: number
}

function oneShotShapeAppendProxy(fault: AppendFault) {
  return async (upstreamUrl: string) => {
    const upstream = new URL(upstreamUrl)
    const server = createServer((incoming, outgoing) => {
      const target = new URL(incoming.url ?? '/', upstream)
      const failHead = fault.headArmed || (fault.headFailures ?? 0) > 0
      if (failHead && incoming.method === 'HEAD' && target.pathname.startsWith('/shape/')) {
        if (fault.headArmed) fault.headArmed = false
        else fault.headFailures = (fault.headFailures ?? 0) - 1
        fault.headHits = (fault.headHits ?? 0) + 1
        incoming.resume()
        outgoing.writeHead(fault.headStatus ?? 503, { 'content-type': 'text/plain' })
        outgoing.end('one-shot HEAD failure')
        return
      }
      if (fault.armed && incoming.method === 'POST' && target.pathname.startsWith('/shape/')) {
        fault.armed = false
        fault.hits += 1
        incoming.resume()
        outgoing.writeHead(fault.status, { 'content-type': 'text/plain' })
        outgoing.end('one-shot storage failure')
        return
      }
      const forwarded = request(
        target,
        { method: incoming.method, headers: { ...incoming.headers, host: upstream.host } },
        (response) => {
          outgoing.writeHead(response.statusCode ?? 502, response.headers)
          response.pipe(outgoing)
        },
      )
      forwarded.on('error', (error) => {
        if (!outgoing.headersSent) outgoing.writeHead(502)
        outgoing.end(String(error))
      })
      incoming.pipe(forwarded)
    })
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject)
      server.listen(0, '127.0.0.1', resolve)
    })
    const address = server.address()
    if (!address || typeof address === 'string') throw new Error('proxy did not bind')
    return {
      url: `http://127.0.0.1:${address.port}`,
      close: () => new Promise<void>((resolve, reject) => server.close((e) => (e ? reject(e) : resolve()))),
    }
  }
}

describe('native shape storage loss', () => {
  it('does not silently lose a PostgreSQL change after one false 404 from the shape-stream service', async () => {
    const fault: AppendFault = { armed: false, status: 404, hits: 0 }
    h = await bootHarness(schema, {
      wrapEngineDs: oneShotShapeAppendProxy(fault),
    })

    const shape = await createShape(h, { table: 'items' })
    fault.armed = true
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['lost'])
    await drainEngine(h)
    expect(fault.hits).toBe(1)

    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (2, $1)', ['later'])
    await drainEngine(h)
    const rows = await foldStream(shape.streamUrl)
    expect(rows.has('1'), 'the change rejected once by storage must not be discarded').toBe(true)
    expect(rows.has('2'), 'the same returned stream remains healthy for later changes').toBe(true)
  })

  it('does not join a retained shape whose returned durable stream has disappeared', async () => {
    h = await bootHarness(schema)
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['present'])
    await drainEngine(h)

    const first = await createShape(h, { table: 'items' })
    expect((await fetch(first.streamUrl, { method: 'DELETE' })).ok).toBe(true)
    expect((await fetch(first.streamUrl)).status).toBe(404)

    const replacement = await createShape(h, { table: 'items' })
    expect(replacement.shapeId, 'a new subscription must not reuse the known-dead handle').not.toBe(first.shapeId)
    expect((await fetch(replacement.streamUrl)).status).toBe(200)
  })

  it('does not return a dead retained handle when its storage check is transiently unavailable', async () => {
    const fault: AppendFault = { armed: false, status: 503, hits: 0 }
    h = await bootHarness(schema, { wrapEngineDs: oneShotShapeAppendProxy(fault) })
    const first = await createShape(h, { table: 'items' })
    expect((await fetch(first.streamUrl, { method: 'DELETE' })).ok).toBe(true)
    expect((await fetch(first.streamUrl)).status).toBe(404)

    // The stream is really gone, but the engine sees one ordinary transient failure while checking
    // that external service. Uncertainty must not be converted into a successful known-dead handle.
    fault.headArmed = true
    fault.headStatus = 503
    const replacement = await createShape(h, { table: 'items' })
    expect(fault.headHits).toBe(1)
    expect(replacement.shapeId).not.toBe(first.shapeId)
    expect((await fetch(replacement.streamUrl)).status).toBe(200)
  })

  it('retries the boot when storage cannot answer for a restored stream, instead of serving without the shape', async () => {
    const fault: AppendFault = { armed: false, status: 503, hits: 0 }
    h = await bootHarness(schema, { wrapEngineDs: oneShotShapeAppendProxy(fault) })
    const shape = await createShape(h, { table: 'items' })

    // The restore vouches for every retained stream before installing anything (ADR-0009). Each check
    // is retried in place (5 tries), so it takes storage failing all of them to fail the attempt. An
    // unanswered HEAD is not "missing" — reading it that way would retire a healthy shape — and it
    // is not something to boot past either, which would serve a registry without the shape while
    // its subscribers read a stream nothing maintains. The attempt is undone and the boot retries.
    fault.headFailures = 5
    fault.headStatus = 503
    await h.restartEngine()
    expect(fault.headHits, 'every in-place retry of the one check was spent').toBe(5)
    const log = h.engineStderr()
    expect(log).toContain('restoring the durable shape catalog')
    expect(log).toContain('retrying in')
    expect(log).not.toContain('boot refused')

    expect((await fetch(`${h.engineUrl}/shapes/${shape.shapeId}`)).status).toBe(200)
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['after'])
    await drainEngine(h)
    expect((await foldStream(shape.streamUrl)).has('1'), 'the shape restored by the retry is maintained').toBe(true)
  })

  it('retires a restored shape whose stream vanished while the engine was down, and restores the others', async () => {
    h = await bootHarness(schema)
    const lost = await createShape(h, { table: 'items' })
    const kept = await createShape(h, { table: 'items', where: { col: 'id', op: 'gte', value: 0 } })
    expect(kept.shapeId).not.toBe(lost.shapeId)

    // Storage loses one stream while no engine runs. That is a definitive answer about that one
    // shape (ADR-0009): the boot retires it, rather than refusing the whole engine or resuming a
    // shape whose every append would be answered 404.
    await h.restartEngine(async () => {
      expect((await fetch(lost.streamUrl, { method: 'DELETE' })).ok).toBe(true)
    })

    expect((await fetch(`${h.engineUrl}/shapes/${lost.shapeId}`)).status).toBe(404)
    expect((await fetch(lost.streamUrl)).status, 'a missing stream is never recreated').toBe(404)
    expect((await fetch(`${h.engineUrl}/shapes/${kept.shapeId}`)).status).toBe(200)
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (2, $1)', ['after'])
    await drainEngine(h)
    expect((await foldStream(kept.streamUrl)).has('2'), 'the other shape restored and is maintained').toBe(true)

    const metrics = (await (await fetch(`${h.engineUrl}/metrics`)).json()) as { counters: Record<string, number> }
    expect(metrics.counters.catalog_restore_retired_stream_missing_total).toBe(1)
  })

  it('does not retire an acknowledged aggregate after one transient append failure during restore', async () => {
    const fault: AppendFault = { armed: false, status: 503, hits: 0 }
    h = await bootHarness(schema, { wrapEngineDs: oneShotShapeAppendProxy(fault) })
    await pgQuery(h, 'INSERT INTO items (id, payload) VALUES (1, $1)', ['present'])
    await drainEngine(h)

    const response = await fetch(`${h.engineUrl}/aggregate`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', fn: 'count' }),
    })
    expect(response.status).toBe(200)
    const aggregate = (await response.json()) as { shapeId: string; streamUrl: string }

    fault.armed = true
    await h.restartEngine()
    expect(fault.hits).toBe(1)

    expect(
      (await fetch(`${h.engineUrl}/shapes/${aggregate.shapeId}`)).status,
      'a transient storage response during boot must not permanently remove an acknowledged aggregate',
    ).toBe(200)
    expect((await fetch(aggregate.streamUrl)).status).toBe(200)
  })
})
