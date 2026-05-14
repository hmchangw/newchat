import { describe, it, expect } from 'vitest'
import { roomPrefix, roomDisplayName, roomFromSearchHit, searchRoomPrefix } from './roomFormat'

describe('roomPrefix', () => {
  it('uses "@ " for dm and botDM rooms, "# " for everything else', () => {
    expect(roomPrefix('dm')).toBe('@ ')
    expect(roomPrefix('botDM')).toBe('@ ')
    expect(roomPrefix('channel')).toBe('# ')
    expect(roomPrefix('unknown')).toBe('# ')
  })
})

describe('searchRoomPrefix', () => {
  it('mirrors roomPrefix without the trailing space', () => {
    expect(searchRoomPrefix('dm')).toBe('@')
    expect(searchRoomPrefix('channel')).toBe('#')
  })
})

describe('roomDisplayName', () => {
  it('returns "" for null / undefined', () => {
    expect(roomDisplayName(null)).toBe('')
    expect(roomDisplayName(undefined)).toBe('')
  })

  it('prefers room.subscriptionName for channel rooms', () => {
    expect(
      roomDisplayName({ name: 'fallback', subscriptionName: 'frontend', type: 'channel', id: 'r1' })
    ).toBe('frontend')
  })

  it('prefers room.subscriptionName for botDM rooms', () => {
    expect(
      roomDisplayName({ subscriptionName: 'weather-bot', type: 'botDM', id: 'r1' })
    ).toBe('weather-bot')
  })

  it('prefers room.subscriptionName for discussion rooms', () => {
    expect(
      roomDisplayName({ name: 'fallback', subscriptionName: 'design-thread', type: 'discussion', id: 'r1' })
    ).toBe('design-thread')
  })

  it('falls back to room.name for channel rooms when subscriptionName is missing', () => {
    expect(roomDisplayName({ name: 'frontend', type: 'channel', id: 'r1' })).toBe('frontend')
  })

  it('falls back to room.id for channel rooms with no subscriptionName and no name', () => {
    expect(roomDisplayName({ type: 'channel', id: 'r-orphan' })).toBe('r-orphan')
  })

  it('renders dm rooms using HRInfo.engName + " " + HRInfo.name', () => {
    expect(
      roomDisplayName({ type: 'dm', id: 'r-dm', hrInfo: { engName: 'John Smith', name: '約翰史密斯' } })
    ).toBe('John Smith 約翰史密斯')
  })

  it('collapses dm display to a single HRInfo.name when engName equals name', () => {
    expect(
      roomDisplayName({ type: 'dm', id: 'r-dm', hrInfo: { engName: 'John Smith', name: 'John Smith' } })
    ).toBe('John Smith')
  })

  it('falls back to "(DM)" for dm rooms with no hrInfo', () => {
    expect(roomDisplayName({ type: 'dm', id: 'r-dm' })).toBe('(DM)')
  })

  it('falls back to "(DM)" for dm rooms with an empty hrInfo object', () => {
    expect(roomDisplayName({ type: 'dm', id: 'r-dm', hrInfo: {} })).toBe('(DM)')
  })
})

describe('roomFromSearchHit', () => {
  it('maps search-hit field names onto the room shape', () => {
    const hit = { roomId: 'r1', roomName: 'frontend', roomType: 'channel', siteId: 'site-A' }
    expect(roomFromSearchHit(hit)).toEqual({
      id: 'r1',
      name: 'frontend',
      type: 'channel',
      siteId: 'site-A',
    })
  })
})
