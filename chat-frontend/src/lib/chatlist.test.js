import { describe, it, expect } from 'vitest'
import {
  BUILTIN_SECTION_IDS,
  isBuiltinSectionId,
  defaultChatlistState,
  validateSectionName,
} from './chatlist'

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
