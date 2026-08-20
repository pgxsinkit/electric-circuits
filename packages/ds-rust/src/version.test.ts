import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

// One durable-streams version, pinned in three places that cannot import each other: this package
// (which `cargo install`s the binary for local runs and tests) and the two Dockerfiles that build it
// into the images. Drift is silent and nasty — the test lane would exercise one server version while
// the deployed image runs another — so assert them equal rather than trusting the bump to be
// three-handed.
const root = join(dirname(fileURLToPath(import.meta.url)), '../../..')

function dockerfilePin(file: string): string {
  const text = readFileSync(join(root, 'docker', file), 'utf8')
  const match = text.match(/cargo install durable-streams --version ([^\s]+)/)
  expect(match, `no durable-streams pin found in docker/${file}`).toBeTruthy()
  return match![1]
}

describe('durable-streams pin', () => {
  it('matches CRATE_VERSION in every Dockerfile that installs it', () => {
    const source = readFileSync(join(root, 'packages/ds-rust/src/index.ts'), 'utf8')
    const crateVersion = source.match(/const CRATE_VERSION = '([^']+)'/)?.[1]
    expect(crateVersion).toBeTruthy()

    expect(dockerfilePin('Dockerfile.ds')).toBe(crateVersion)
    expect(dockerfilePin('Dockerfile.electric')).toBe(crateVersion)
  })
})
