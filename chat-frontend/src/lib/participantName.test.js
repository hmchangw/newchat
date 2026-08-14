import { describe, it, expect } from 'vitest'
import { participantDisplayName, messageSenderName } from './participantName'

describe('participantDisplayName', () => {
  it('prefers the server-composed displayName', () => {
    expect(participantDisplayName({ displayName: 'Alice Chen', engName: 'Alice', account: 'alice' }))
      .toBe('Alice Chen')
  })

  it('falls back to engName, then account, then userId', () => {
    expect(participantDisplayName({ engName: 'Alice', account: 'alice' })).toBe('Alice')
    expect(participantDisplayName({ account: 'alice', userId: 'u1' })).toBe('alice')
    expect(participantDisplayName({ userId: 'u1' })).toBe('u1')
  })

  it('returns Unknown for null, undefined, and an empty participant', () => {
    expect(participantDisplayName(null)).toBe('Unknown')
    expect(participantDisplayName(undefined)).toBe('Unknown')
    expect(participantDisplayName({})).toBe('Unknown')
  })
})

describe('messageSenderName', () => {
  it('prefers the top-level userDisplayName', () => {
    expect(messageSenderName({ userDisplayName: 'Alice Chen', sender: { engName: 'Alice' } }))
      .toBe('Alice Chen')
  })

  it('falls back to the nested sender participant', () => {
    expect(messageSenderName({ sender: { displayName: 'Deploy Bot' } })).toBe('Deploy Bot')
    expect(messageSenderName({ sender: { engName: 'Alice' } })).toBe('Alice')
  })

  it('falls back to userAccount then userId when there is no sender', () => {
    expect(messageSenderName({ userAccount: 'alice' })).toBe('alice')
    expect(messageSenderName({ userId: 'u1' })).toBe('u1')
  })

  it('returns Unknown for an empty or missing message', () => {
    expect(messageSenderName({})).toBe('Unknown')
    expect(messageSenderName(null)).toBe('Unknown')
  })
})
