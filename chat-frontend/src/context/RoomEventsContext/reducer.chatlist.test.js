import { describe, it, expect } from 'vitest'
import { initialState, roomEventsReducer } from './reducer'
import { defaultChatlistState } from '@/lib/chatlist'

function withSub(roomId, sub = {}) {
  return {
    ...initialState,
    subscriptions: { [roomId]: { roomId, roomType: 'channel', ...sub } },
  }
}

describe('reducer: chatlist state', () => {
  it('defaults to the built-in overlay', () => {
    expect(initialState.chatlist).toEqual(defaultChatlistState())
  })

  it('CHATLIST_LOADED replaces the overlay', () => {
    const chatlist = { sectionOrder: ['favorites', 'work', 'chats'], sections: [], lastUpdatedAt: 5 }
    const next = roomEventsReducer(initialState, { type: 'CHATLIST_LOADED', chatlist })
    expect(next.chatlist).toBe(chatlist)
  })

  it('CHATLIST_UPDATED applies a newer overlay (LWW) and drops a stale one', () => {
    const seeded = roomEventsReducer(initialState, {
      type: 'CHATLIST_LOADED',
      chatlist: { sectionOrder: [], sections: [], lastUpdatedAt: 100 },
    })
    const newer = { sectionOrder: ['chats'], sections: [], lastUpdatedAt: 200 }
    const afterNew = roomEventsReducer(seeded, { type: 'CHATLIST_UPDATED', chatlist: newer })
    expect(afterNew.chatlist).toBe(newer)
    const stale = { sectionOrder: [], sections: [], lastUpdatedAt: 150 }
    const afterStale = roomEventsReducer(afterNew, { type: 'CHATLIST_UPDATED', chatlist: stale })
    expect(afterStale.chatlist).toBe(newer) // stale dropped
  })
})

describe('reducer: SUBSCRIPTION_SECTION_MOVED', () => {
  it('sets sectionId + sectionOrder on the sub', () => {
    const state = withSub('r1')
    const next = roomEventsReducer(state, {
      type: 'SUBSCRIPTION_SECTION_MOVED',
      roomId: 'r1',
      sectionId: 'work',
      sectionOrder: 3.5,
    })
    expect(next.subscriptions.r1).toMatchObject({ sectionId: 'work', sectionOrder: 3.5 })
  })

  it('clears both fields on a remove (undefined), not leaving the old section', () => {
    const state = withSub('r1', { sectionId: 'work', sectionOrder: 2 })
    const next = roomEventsReducer(state, {
      type: 'SUBSCRIPTION_SECTION_MOVED',
      roomId: 'r1',
      sectionId: undefined,
      sectionOrder: undefined,
    })
    expect(next.subscriptions.r1.sectionId).toBeUndefined()
    expect(next.subscriptions.r1.sectionOrder).toBeUndefined()
  })

  it('is a no-op for an unknown room or an unchanged placement', () => {
    const unknown = roomEventsReducer(initialState, {
      type: 'SUBSCRIPTION_SECTION_MOVED',
      roomId: 'nope',
      sectionId: 'work',
    })
    expect(unknown).toBe(initialState)
    const state = withSub('r1', { sectionId: 'work', sectionOrder: 2 })
    const same = roomEventsReducer(state, {
      type: 'SUBSCRIPTION_SECTION_MOVED',
      roomId: 'r1',
      sectionId: 'work',
      sectionOrder: 2,
    })
    expect(same).toBe(state)
  })
})
