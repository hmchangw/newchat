import { describe, it, expect } from 'vitest'
import {
  BUILTIN_SECTION_IDS,
  isBuiltinSectionId,
  defaultChatlistState,
  deriveSidebarSections,
  validateSectionName,
} from './chatlist'

function summary(id, over = {}) {
  return { id, name: id, type: 'channel', lastMsgAt: '2026-04-17T10:00:00Z', ...over }
}

// overlay with one custom section "work"
function overlayWithWork() {
  const base = defaultChatlistState()
  return {
    sectionOrder: ['favorites', 'apps', 'teams', 'work', 'chats'],
    sections: [...base.sections, { id: 'work', name: 'Work', builtIn: false, sortMode: 'custom' }],
    lastUpdatedAt: 1,
  }
}

function sectionsByKey(list) {
  return Object.fromEntries(list.map((s) => [s.key, s.rooms.map((r) => r.id)]))
}

describe('defaultChatlistState', () => {
  it('is the four built-ins in canonical order, mostRecent', () => {
    const s = defaultChatlistState()
    expect(s.sectionOrder).toEqual(BUILTIN_SECTION_IDS)
    expect(s.sections.map((x) => x.sortMode)).toEqual(['mostRecent', 'mostRecent', 'mostRecent', 'mostRecent'])
    expect(s.sections.every((x) => x.builtIn)).toBe(true)
  })
})

describe('isBuiltinSectionId', () => {
  it('true for built-ins, false for custom', () => {
    expect(isBuiltinSectionId('teams')).toBe(true)
    expect(isBuiltinSectionId('mock-section-1')).toBe(false)
  })
})

describe('deriveSidebarSections', () => {
  it('groups by flag precedence: favorite > bot > sectionId > chats', () => {
    const summaries = [
      summary('fav'),
      summary('bot', { type: 'botDM' }),
      summary('team'),
      summary('work1'),
      summary('plain'),
    ]
    const subs = {
      fav: { roomId: 'fav', favorite: true, sectionId: 'work' }, // favorite wins over custom
      bot: { roomId: 'bot', sectionId: 'work' }, // bot (by room type) wins over custom
      team: { roomId: 'team', sectionId: 'teams' },
      work1: { roomId: 'work1', sectionId: 'work', sectionOrder: 1 },
      plain: { roomId: 'plain' },
    }
    const out = sectionsByKey(deriveSidebarSections(summaries, subs, overlayWithWork()))
    expect(out.favorites).toEqual(['fav'])
    expect(out.apps).toEqual(['bot'])
    expect(out.teams).toEqual(['team'])
    expect(out.work).toEqual(['work1'])
    expect(out.chats).toEqual(['plain'])
  })

  it('renders an orphaned sectionId in chats', () => {
    const summaries = [summary('r1')]
    const subs = { r1: { roomId: 'r1', sectionId: 'deleted-section' } }
    const out = sectionsByKey(deriveSidebarSections(summaries, subs, overlayWithWork()))
    expect(out.chats).toEqual(['r1'])
  })

  it('sorts a custom section by sectionOrder, others by last message', () => {
    const summaries = [
      summary('w-b', { lastMsgAt: '2026-04-17T09:00:00Z' }),
      summary('w-a', { lastMsgAt: '2026-04-17T12:00:00Z' }),
      summary('c-old', { lastMsgAt: '2026-04-17T08:00:00Z' }),
      summary('c-new', { lastMsgAt: '2026-04-17T13:00:00Z' }),
    ]
    const subs = {
      'w-b': { roomId: 'w-b', sectionId: 'work', sectionOrder: 1 },
      'w-a': { roomId: 'w-a', sectionId: 'work', sectionOrder: 2 },
      'c-old': { roomId: 'c-old' },
      'c-new': { roomId: 'c-new' },
    }
    const out = sectionsByKey(deriveSidebarSections(summaries, subs, overlayWithWork()))
    expect(out.work).toEqual(['w-b', 'w-a']) // by sectionOrder, not last-msg
    expect(out.chats).toEqual(['c-new', 'c-old']) // by last-msg desc
  })

  it('hides empty built-ins (except chats) but always shows custom sections', () => {
    const summaries = [summary('plain')]
    const subs = { plain: { roomId: 'plain' } }
    const list = deriveSidebarSections(summaries, subs, overlayWithWork())
    const keys = list.map((s) => s.key)
    expect(keys).toContain('chats') // always
    expect(keys).toContain('work') // custom, even though empty
    expect(keys).not.toContain('favorites') // empty built-in hidden
    expect(keys).not.toContain('apps')
    expect(keys).not.toContain('teams')
  })

  it('falls back to the built-in overlay when none is provided', () => {
    const out = sectionsByKey(deriveSidebarSections([summary('r1')], { r1: { roomId: 'r1' } }, undefined))
    expect(out.chats).toEqual(['r1'])
  })
})

describe('validateSectionName', () => {
  it('accepts a trimmed valid name', () => {
    expect(validateSectionName('  Work Chat  ')).toEqual({ ok: true, name: 'Work Chat' })
  })
  it('accepts non-latin letters + allowed punctuation', () => {
    expect(validateSectionName('工作-群/組(A)')).toEqual({ ok: true, name: '工作-群/組(A)' })
  })
  it('rejects empty, too-long, consecutive-space, and bad chars', () => {
    expect(validateSectionName('').ok).toBe(false)
    expect(validateSectionName('x'.repeat(51)).ok).toBe(false)
    expect(validateSectionName('a  b').ok).toBe(false)
    expect(validateSectionName('bad@name').ok).toBe(false)
  })
})
