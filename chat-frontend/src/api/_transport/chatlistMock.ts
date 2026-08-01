// DEV-ONLY in-memory mock of the chatlist backend (user-service + room-service
// chat.move). Enabled via the CHATLIST_MOCK runtime flag so the grouped
// sidebar can be demoed before the backend RPCs land. Mirrors the documented
// wire shapes 1:1, and emits through two emitters that mirror the two real
// subjects (chatlist.update + subscription.update/section_moved), so flipping
// the flag off routes every op to real NATS with no consumer change.
//
// Not imported by production paths — every call site guards on CHATLIST_MOCK.

import {
  BUILTIN_CHATS,
  defaultChatlistState,
  isBuiltinSectionId,
  validateSectionName,
} from '@/lib/chatlist'
import type {
  ChatlistState,
  ChatlistSortMode,
  DMSubscription,
  SubscriptionUpdateEvent,
} from '../types'

type ChatlistListener = (evt: { timestamp: number; chatlist: ChatlistState }) => void
type SectionMovedListener = (evt: SubscriptionUpdateEvent) => void

/** Wire-shaped errcode-lite thrown by the mock so callers see the same
 *  `.reason` they'd get from the real backend envelope. */
export class MockChatlistError extends Error {
  reason: string
  constructor(reason: string) {
    super(reason)
    this.reason = reason
  }
}

let state: ChatlistState = defaultChatlistState()
const membership = new Map<string, { sectionId?: string; sectionOrder?: number }>()
const chatlistListeners = new Set<ChatlistListener>()
const sectionMovedListeners = new Set<SectionMovedListener>()
let idCounter = 0

function now(): number {
  return Date.now()
}

function clone(s: ChatlistState): ChatlistState {
  return { sectionOrder: [...s.sectionOrder], sections: s.sections.map((x) => ({ ...x })), lastUpdatedAt: s.lastUpdatedAt }
}

function emitChatlist(): void {
  state.lastUpdatedAt = now()
  const snapshot = clone(state)
  chatlistListeners.forEach((cb) => cb({ timestamp: snapshot.lastUpdatedAt, chatlist: snapshot }))
}

/** Reset all mock state — test helper. */
export function __resetChatlistMock(): void {
  state = defaultChatlistState()
  membership.clear()
  chatlistListeners.clear()
  sectionMovedListeners.clear()
  idCounter = 0
}

export function getChatlistMock(): ChatlistState {
  return clone(state)
}

function requireCustom(sectionId: string) {
  const sec = state.sections.find((s) => s.id === sectionId)
  if (!sec) throw new MockChatlistError('chatlist_section_not_found')
  if (sec.builtIn) throw new MockChatlistError('chatlist_builtin_immutable')
  return sec
}

export function createSectionMock(name: string, sortMode: ChatlistSortMode = 'custom'): ChatlistState {
  const v = validateSectionName(name)
  if (!v.ok) throw new MockChatlistError(v.reason)
  if (state.sections.some((s) => !s.builtIn && s.name === v.name))
    throw new MockChatlistError('chatlist_duplicate_name')
  const id = `mock-section-${++idCounter}`
  state.sections.push({ id, name: v.name, builtIn: false, sortMode })
  // Slot the new custom section just above the built-in Chats.
  const chatsIdx = state.sectionOrder.indexOf(BUILTIN_CHATS)
  state.sectionOrder.splice(chatsIdx < 0 ? state.sectionOrder.length : chatsIdx, 0, id)
  emitChatlist()
  return clone(state)
}

export function renameSectionMock(sectionId: string, name: string): ChatlistState {
  const sec = requireCustom(sectionId)
  const v = validateSectionName(name)
  if (!v.ok) throw new MockChatlistError(v.reason)
  if (state.sections.some((s) => !s.builtIn && s.id !== sectionId && s.name === v.name))
    throw new MockChatlistError('chatlist_duplicate_name')
  sec.name = v.name
  emitChatlist()
  return clone(state)
}

export function deleteSectionMock(sectionId: string): ChatlistState {
  requireCustom(sectionId)
  state.sections = state.sections.filter((s) => s.id !== sectionId)
  state.sectionOrder = state.sectionOrder.filter((id) => id !== sectionId)
  // Members orphan to Chats — drop their membership (derived fallback handles it).
  membership.forEach((m, roomId) => {
    if (m.sectionId === sectionId) membership.delete(roomId)
  })
  emitChatlist()
  return clone(state)
}

export function reorderSectionsMock(sectionOrder: string[]): ChatlistState {
  const known = new Set(state.sections.map((s) => s.id))
  const sameSet = sectionOrder.length === known.size && sectionOrder.every((id) => known.has(id))
  if (!sameSet) throw new MockChatlistError('chatlist_invalid_order')
  state.sectionOrder = [...sectionOrder]
  emitChatlist()
  return clone(state)
}

export function setSectionSortModeMock(sectionId: string, sortMode: ChatlistSortMode): ChatlistState {
  if (sortMode !== 'custom' && sortMode !== 'mostRecent')
    throw new MockChatlistError('chatlist_invalid_sort_mode')
  const sec = state.sections.find((s) => s.id === sectionId)
  if (!sec) throw new MockChatlistError('chatlist_section_not_found')
  sec.sortMode = sortMode
  emitChatlist()
  return clone(state)
}

/** Fractional order for an insert: midpoint after `afterRoomId`, else append. */
function computeOrder(sectionId: string, afterRoomId?: string): number {
  const orders = [...membership.entries()]
    .filter(([, m]) => m.sectionId === sectionId && typeof m.sectionOrder === 'number')
    .map(([roomId, m]) => ({ roomId, order: m.sectionOrder as number }))
    .sort((a, b) => a.order - b.order)
  if (!afterRoomId) return (orders.at(-1)?.order ?? 0) + 1
  const idx = orders.findIndex((o) => o.roomId === afterRoomId)
  if (idx < 0) return (orders.at(-1)?.order ?? 0) + 1
  const prev = orders[idx].order
  const next = orders[idx + 1]?.order
  return next === undefined ? prev + 1 : (prev + next) / 2
}

export interface MoveChatMockResult {
  status: 'ok'
  sectionId?: string
  sectionOrder?: number
}

export function moveChatMock(
  roomId: string,
  sectionId: string | null,
  afterRoomId?: string,
): MoveChatMockResult {
  if (sectionId !== null) {
    if (isBuiltinSectionId(sectionId)) throw new MockChatlistError('chatlist_builtin_target')
    if (!state.sections.some((s) => s.id === sectionId))
      throw new MockChatlistError('chatlist_section_not_found')
  }
  let result: MoveChatMockResult
  if (sectionId === null) {
    membership.delete(roomId)
    result = { status: 'ok' }
  } else {
    const sectionOrder = computeOrder(sectionId, afterRoomId)
    membership.set(roomId, { sectionId, sectionOrder })
    result = { status: 'ok', sectionId, sectionOrder }
  }
  // Fan out a section_moved subscription.update, exactly as the backend would.
  const evt: SubscriptionUpdateEvent = {
    userId: 'mock',
    action: 'section_moved',
    timestamp: now(),
    subscription: {
      roomId,
      sectionId: sectionId ?? undefined,
      sectionOrder: result.sectionOrder,
    } as DMSubscription,
  }
  sectionMovedListeners.forEach((cb) => cb(evt))
  return result
}

export function onChatlistUpdateMock(cb: ChatlistListener): () => void {
  chatlistListeners.add(cb)
  return () => chatlistListeners.delete(cb)
}

export function onSectionMovedMock(cb: SectionMovedListener): () => void {
  sectionMovedListeners.add(cb)
  return () => sectionMovedListeners.delete(cb)
}

/** Demo seed: from the just-loaded subscriptions, create a couple sample
 *  custom sections and drop a few non-favorite/non-bot rooms into them, plus
 *  mark two rooms favorite — so the grouped sidebar shows populated sections
 *  in a screenshot. Returns the section membership + favorite ids to patch
 *  onto the reducer's subscriptions (the mock owns defs; the reducer owns
 *  the sub records). Idempotent-ish: only seeds when no custom section exists. */
export interface MockSeed {
  membership: Record<string, { sectionId: string; sectionOrder: number }>
  favoriteRoomIds: string[]
}

export function seedChatlistMock(subs: DMSubscription[]): MockSeed {
  if (state.sections.some((s) => !s.builtIn)) {
    // Already seeded — return current membership.
    const m: MockSeed['membership'] = {}
    membership.forEach((v, roomId) => {
      if (v.sectionId && typeof v.sectionOrder === 'number')
        m[roomId] = { sectionId: v.sectionId, sectionOrder: v.sectionOrder }
    })
    return { membership: m, favoriteRoomIds: [] }
  }
  const plain = subs.filter((s) => s.roomType !== 'botDM' && !s.u?.isBot && !s.favorite)
  const work = createSectionMock('Work', 'custom')
  const projects = createSectionMock('Projects', 'mostRecent')
  const workId = work.sections.find((s) => s.name === 'Work')!.id
  const projId = projects.sections.find((s) => s.name === 'Projects')!.id
  const seededMembership: MockSeed['membership'] = {}
  plain.slice(0, 3).forEach((s, i) => {
    membership.set(s.roomId, { sectionId: workId, sectionOrder: i + 1 })
    seededMembership[s.roomId] = { sectionId: workId, sectionOrder: i + 1 }
  })
  plain.slice(3, 5).forEach((s, i) => {
    membership.set(s.roomId, { sectionId: projId, sectionOrder: i + 1 })
    seededMembership[s.roomId] = { sectionId: projId, sectionOrder: i + 1 }
  })
  const favoriteRoomIds = plain.slice(5, 7).map((s) => s.roomId)
  return { membership: seededMembership, favoriteRoomIds }
}
