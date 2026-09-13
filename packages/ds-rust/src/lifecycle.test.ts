// The public wrapper is reusable: callers commonly keep a test-server instance in a fixture and
// cycle it between tests. A normal stop must therefore discard wrapper-owned endpoint/storage
// state; only crashAndRestart is allowed to retain it in place.

import { existsSync, rmSync } from 'node:fs'

import { describe, expect, it } from 'vitest'

import { DurableStreamTestServer } from './index.js'

describe('DurableStreamTestServer public lifecycle', () => {
  it('allocates and removes a fresh owned store on each start-stop cycle', async () => {
    const server = new DurableStreamTestServer({ port: 0 })
    const created = new Set<string>()

    try {
      await server.start()
      const first = server.testStoragePath
      expect(first).toBeDefined()
      created.add(first!)
      expect(server.pid).toBeTypeOf('number')
      const firstPid = server.pid!

      await server.stop()
      expect(server.pid).toBeUndefined()
      expect(server.testStoragePath).toBeUndefined()
      expect(() => process.kill(firstPid, 0), 'the first server process must be gone').toThrow()
      expect(existsSync(first!)).toBe(false)

      await server.start()
      const second = server.testStoragePath
      expect(second).toBeDefined()
      created.add(second!)
      expect(second, 'a fresh public start must not reuse a removed owned store').not.toBe(first)
      expect(server.pid).toBeTypeOf('number')
      const secondPid = server.pid!

      await server.stop()
      expect(server.pid).toBeUndefined()
      expect(server.testStoragePath).toBeUndefined()
      expect(() => process.kill(secondPid, 0), 'the second server process must be gone').toThrow()
      expect(existsSync(first!)).toBe(false)
      expect(existsSync(second!)).toBe(false)
    } finally {
      await server.stop().catch(() => {})
      // A red run must not leave the demonstrated leak behind for neighboring workers.
      for (const path of created) rmSync(path, { recursive: true, force: true })
    }
  }, 30000)
})
