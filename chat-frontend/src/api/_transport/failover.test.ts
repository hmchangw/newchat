import { beforeEach, describe, expect, it } from 'vitest'
import { isFailoverMode, setFailoverMode } from './failover'
import { msgSend, roomEvent } from './subjects'

describe('failover mode', () => {
  beforeEach(() => setFailoverMode(false))

  it('defaults to off', () => {
    expect(isFailoverMode()).toBe(false)
  })

  it('reports the flag it was set to', () => {
    setFailoverMode(true)
    expect(isFailoverMode()).toBe(true)
    setFailoverMode(false)
    expect(isFailoverMode()).toBe(false)
  })
})

describe('roomEvent under failover', () => {
  beforeEach(() => setFailoverMode(false))

  it('routes same-site rooms to the local subject when off', () => {
    expect(roomEvent('r1', false)).toBe('chat.local.room.r1.event')
  })

  // The local root is filtered from gateway interest, so a client sitting on a
  // peer cluster would never receive a same-site room's events — silently, and
  // for the rooms that make up most of its traffic.
  it('forces the global subject for same-site rooms when on', () => {
    setFailoverMode(true)
    expect(roomEvent('r1', false)).toBe('chat.room.r1.event')
  })

  it('leaves cross-site rooms on the global subject either way', () => {
    expect(roomEvent('r1', true)).toBe('chat.room.r1.event')
    setFailoverMode(true)
    expect(roomEvent('r1', true)).toBe('chat.room.r1.event')
  })

  // The fail-safe is unchanged: only an explicit false selects the local root.
  it('keeps undefined crossSite on the global subject when off', () => {
    expect(roomEvent('r1', undefined as unknown as boolean)).toBe('chat.room.r1.event')
  })
})

describe('msgSend under failover', () => {
  beforeEach(() => setFailoverMode(false))

  it('uses the live send subject when off', () => {
    expect(msgSend('alice', 'r1', 'site-a')).toBe('chat.user.alice.room.r1.site-a.msg.send')
  })

  // The live MESSAGES stream lives on the cluster that is down, so a send
  // published there would go nowhere. The failover token routes it to
  // MESSAGES-FAILOVER on the buddy instead.
  it('inserts the failover token when on', () => {
    setFailoverMode(true)
    expect(msgSend('alice', 'r1', 'site-a')).toBe('chat.user.alice.room.r1.site-a.failover.msg.send')
  })
})
