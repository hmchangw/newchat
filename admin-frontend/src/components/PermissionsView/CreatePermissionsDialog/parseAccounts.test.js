import { describe, it, expect } from 'vitest'
import { parsePastedAccounts } from './parseAccounts'

describe('parsePastedAccounts', () => {
  it.each([
    ['spaces', 'alice bob carol'],
    ['newlines', 'alice\nbob\ncarol'],
    ['commas', 'alice,bob,carol'],
    ['semicolons', 'alice;bob;carol'],
    ['tabs', 'alice\tbob\tcarol'],
    ['mixed separators', 'alice, bob;\n\tcarol'],
  ])('splits on %s', (_name, text) => {
    expect(parsePastedAccounts(text)).toEqual({ accounts: ['alice', 'bob', 'carol'], duplicates: 0 })
  })

  it('trims surrounding whitespace and drops empty tokens', () => {
    expect(parsePastedAccounts('  alice ,, \n\n bob ; \t')).toEqual({
      accounts: ['alice', 'bob'],
      duplicates: 0,
    })
  })

  it('dedups preserving first occurrence and counts the dropped duplicates', () => {
    expect(parsePastedAccounts('bob alice,bob\ncarol bob')).toEqual({
      accounts: ['bob', 'alice', 'carol'],
      duplicates: 2,
    })
  })

  it('keeps accounts verbatim — no case folding (matches backend semantics)', () => {
    expect(parsePastedAccounts('Alice alice ALICE')).toEqual({
      accounts: ['Alice', 'alice', 'ALICE'],
      duplicates: 0,
    })
  })

  it('returns an empty result for an empty string', () => {
    expect(parsePastedAccounts('')).toEqual({ accounts: [], duplicates: 0 })
  })

  it('returns an empty result for whitespace/separators only', () => {
    expect(parsePastedAccounts(' \n,\t; ')).toEqual({ accounts: [], duplicates: 0 })
  })
})
