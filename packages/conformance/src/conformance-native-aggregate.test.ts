import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type { Row, Schema } from '@electric-circuits/protocol'

import { foldStream, pgQuery } from './engine-native.js'
import { bootHarness, type Harness } from './harness.js'

const schema: Schema = {
  tables: {
    items: {
      columns: { id: { type: 'int' }, amount: { type: 'int' } },
      primaryKey: 'id',
    },
  },
}

describe('native aggregate precision', () => {
  let h: Harness

  beforeEach(async () => {
    h = await bootHarness(schema, {
      ddl: `
        CREATE TABLE items (id integer PRIMARY KEY, amount bigint);
        ALTER TABLE items REPLICA IDENTITY FULL;
      `,
    })
  })
  afterEach(async () => await h.shutdown())

  it('emits the exact PostgreSQL SUM for a bigint above the JavaScript safe-integer boundary', async () => {
    await pgQuery(h, 'INSERT INTO items (id, amount) VALUES (1, 9007199254740993)')

    const res = await fetch(`${h.engineUrl}/aggregate`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ table: 'items', fn: 'sum', col: 'amount' }),
    })
    expect(res.status).toBe(200)
    const handle = (await res.json()) as { streamUrl: string }

    const expected = String((await pgQuery(h, 'SELECT sum(amount)::text AS value FROM items'))[0]!.value)
    const aggregate = (await foldStream(handle.streamUrl)).get('agg') as Row
    expect(String(aggregate.value)).toBe(expected)
  })
})
