import { describe, it, expect } from 'vitest'
import { isSystemMessageType, SYSTEM_MESSAGE_TYPES } from './messageType'

describe('isSystemMessageType', () => {
  it.each([
    'room_created',
    'members_added',
    'member_removed',
    'member_left',
    'room_renamed',
    'room_restricted',
    'teams_meet_started',
  ])('treats %s as a system message type', (type) => {
    expect(isSystemMessageType(type)).toBe(true)
  })

  it('does NOT treat "important" as a system message type', () => {
    // MessageTypeImportant is client-settable and previews/notifies like a
    // normal message — see pkg/model/event.go:661-667.
    expect(isSystemMessageType('important')).toBe(false)
  })

  it('does not treat an empty string as a system message type', () => {
    expect(isSystemMessageType('')).toBe(false)
  })

  it('does not treat undefined as a system message type', () => {
    expect(isSystemMessageType(undefined)).toBe(false)
  })

  it('does not treat an unrecognized type as a system message type', () => {
    expect(isSystemMessageType('some_future_type')).toBe(false)
  })

  it('SYSTEM_MESSAGE_TYPES contains exactly the seven known types', () => {
    expect([...SYSTEM_MESSAGE_TYPES].sort()).toEqual(
      [
        'room_created',
        'members_added',
        'member_removed',
        'member_left',
        'room_renamed',
        'room_restricted',
        'teams_meet_started',
      ].sort()
    )
  })
})
