import { describe, it, expect, beforeEach, vi } from 'vitest'
import {
  __resetChatlistMock,
  getChatlistMock,
  createSectionMock,
  renameSectionMock,
  deleteSectionMock,
  reorderSectionsMock,
  setSectionSortModeMock,
  moveChatMock,
  onChatlistUpdateMock,
  onSectionMovedMock,
  MockChatlistError,
} from './chatlistMock'
import type { DMSubscription } from '../types'

beforeEach(() => __resetChatlistMock())

describe('chatlistMock section defs', () => {
  it('starts from the four built-ins', () => {
    const s = getChatlistMock()
    expect(s.sectionOrder).toEqual(['favorites', 'apps', 'teams', 'chats'])
    expect(s.sections.every((x) => x.builtIn)).toBe(true)
  })

  it('creates a custom section slotted above chats and emits an update', () => {
    const cb = vi.fn()
    onChatlistUpdateMock(cb)
    const s = createSectionMock('Work', 'custom')
    const work = s.sections.find((x) => x.name === 'Work')
    expect(work).toMatchObject({ builtIn: false, sortMode: 'custom' })
    // slotted just before chats
    expect(s.sectionOrder.indexOf(work!.id)).toBe(s.sectionOrder.indexOf('chats') - 1)
    expect(cb).toHaveBeenCalledOnce()
  })

  it('rejects a duplicate custom name', () => {
    createSectionMock('Work')
    expect(() => createSectionMock('Work')).toThrow(MockChatlistError)
  })

  it('rejects an invalid name', () => {
    expect(() => createSectionMock('bad  spaces')).toThrowError(/chatlist_invalid_name/)
  })

  it('rejects rename/delete of a built-in', () => {
    expect(() => renameSectionMock('chats', 'Nope')).toThrowError(/chatlist_builtin_immutable/)
    expect(() => deleteSectionMock('favorites')).toThrowError(/chatlist_builtin_immutable/)
  })

  it('delete orphans members (drops membership) and removes the def', () => {
    const s = createSectionMock('Work')
    const id = s.sections.find((x) => x.name === 'Work')!.id
    moveChatMock('r1', id)
    deleteSectionMock(id)
    expect(getChatlistMock().sections.some((x) => x.id === id)).toBe(false)
    // moving r1 again appends at 1 (membership was cleared)
    const res = moveChatMock('r1', createSectionMock('P').sections.find((x) => x.name === 'P')!.id)
    expect(res.sectionOrder).toBe(1)
  })

  it('reorder rejects a non-permutation', () => {
    expect(() => reorderSectionsMock(['favorites'])).toThrowError(/chatlist_invalid_order/)
  })

  it('setSortMode flips a built-in to custom', () => {
    const s = setSectionSortModeMock('chats', 'custom')
    expect(s.sections.find((x) => x.id === 'chats')!.sortMode).toBe('custom')
  })
})

describe('chatlistMock moveChat', () => {
  it('appends fractional order and fans section_moved', () => {
    const cb = vi.fn()
    onSectionMovedMock(cb)
    const id = createSectionMock('Work').sections.find((x) => x.name === 'Work')!.id
    expect(moveChatMock('r1', id).sectionOrder).toBe(1)
    expect(moveChatMock('r2', id).sectionOrder).toBe(2)
    // insert r3 after r1 → midpoint 1.5
    expect(moveChatMock('r3', id, 'r1').sectionOrder).toBe(1.5)
    expect(cb).toHaveBeenCalledTimes(3)
    expect(cb.mock.calls[0][0]).toMatchObject({ action: 'section_moved' })
  })

  it('rejects a built-in target', () => {
    expect(() => moveChatMock('r1', 'favorites')).toThrowError(/chatlist_builtin_target/)
  })

  it('remove (null) clears membership and emits with no order', () => {
    const cb = vi.fn()
    onSectionMovedMock(cb)
    const id = createSectionMock('Work').sections.find((x) => x.name === 'Work')!.id
    moveChatMock('r1', id)
    const res = moveChatMock('r1', null)
    expect(res).toEqual({ status: 'ok' })
    expect(cb.mock.calls.at(-1)![0].subscription.sectionId).toBeUndefined()
  })
})

describe('chatlistMock unsubscribe', () => {
  it('stops delivering after unsubscribe', () => {
    const cb = vi.fn()
    const unsub = onChatlistUpdateMock(cb)
    createSectionMock('A')
    unsub()
    createSectionMock('B')
    expect(cb).toHaveBeenCalledOnce()
  })
})

// seedChatlistMock is exercised via the reducer/hook wiring; a light check here.
describe('seedChatlistMock', () => {
  it('is a no-op-safe reseed once custom sections exist', async () => {
    const { seedChatlistMock } = await import('./chatlistMock')
    const subs = Array.from({ length: 8 }, (_, i) => ({
      roomId: `r${i}`,
      roomType: 'channel',
      u: { id: `u${i}`, account: `a${i}` },
    })) as unknown as DMSubscription[]
    const first = seedChatlistMock(subs)
    expect(Object.keys(first.membership).length).toBeGreaterThan(0)
    const second = seedChatlistMock(subs)
    // already seeded → returns existing membership, doesn't double-create sections
    expect(getChatlistMock().sections.filter((s) => !s.builtIn)).toHaveLength(2)
    expect(Object.keys(second.membership).length).toBe(Object.keys(first.membership).length)
  })
})
