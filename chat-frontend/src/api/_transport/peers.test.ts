import { describe, expect, it } from 'vitest'
import { shufflePeers, type Peer } from './peers'

const sites: Peer[] = [
  { siteId: 'site-a', natsUrl: 'wss://a' },
  { siteId: 'site-b', natsUrl: 'wss://b' },
  { siteId: 'site-c', natsUrl: 'wss://c' },
]

describe('shufflePeers', () => {
  it('excludes the home site — it is the one that is down', () => {
    const out = shufflePeers(sites, 'site-a')
    expect(out.map((p) => p.siteId)).not.toContain('site-a')
    expect(out).toHaveLength(2)
  })

  it('returns every remaining peer exactly once', () => {
    const ids = shufflePeers(sites, 'site-a')
      .map((p) => p.siteId)
      .sort()
    expect(ids).toEqual(['site-b', 'site-c'])
  })

  it('carries each peer through unchanged', () => {
    const out = shufflePeers(sites, 'site-a')
    for (const p of out) {
      expect(p.natsUrl).toBe(sites.find((s) => s.siteId === p.siteId)?.natsUrl)
    }
  })

  it('returns empty when there are no peers', () => {
    expect(shufflePeers([{ siteId: 'site-a', natsUrl: 'wss://a' }], 'site-a')).toEqual([])
  })

  it('returns empty for an empty registry', () => {
    expect(shufflePeers([], 'site-a')).toEqual([])
  })

  // Uniform spreading is the whole point: if every displaced client walked the
  // same order they would flatten the first peer, which is the hotspot this
  // exists to prevent.
  it('does not always produce the same order', () => {
    const many = Array.from({ length: 40 }, () => shufflePeers(sites, 'site-a')[0].siteId)
    expect(new Set(many).size).toBeGreaterThan(1)
  })

  it('does not mutate its input', () => {
    const before = sites.map((p) => p.siteId)
    shufflePeers(sites, 'site-a')
    expect(sites.map((p) => p.siteId)).toEqual(before)
  })

  // An unknown home site must not silently drop a candidate.
  it('keeps every site when home is not in the list', () => {
    expect(shufflePeers(sites, 'site-zz')).toHaveLength(3)
  })
})
