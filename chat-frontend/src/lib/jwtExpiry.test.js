import { describe, it, expect } from 'vitest'
import { parseNatsJwtExp } from './jwtExpiry'

function makeJwt(payload) {
  const b64 = (obj) =>
    btoa(JSON.stringify(obj)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
  return `${b64({ alg: 'ed25519' })}.${b64(payload)}.sig`
}

describe('parseNatsJwtExp', () => {
  it('returns the exp claim in unix seconds', () => {
    expect(parseNatsJwtExp(makeJwt({ exp: 1700000000 }))).toBe(1700000000)
  })
  it('returns null when exp is missing', () => {
    expect(parseNatsJwtExp(makeJwt({ sub: 'x' }))).toBeNull()
  })
  it('returns null when exp is not a number', () => {
    expect(parseNatsJwtExp(makeJwt({ exp: 'soon' }))).toBeNull()
  })
  it('returns null for a malformed token', () => {
    expect(parseNatsJwtExp('not-a-jwt')).toBeNull()
  })
  it('returns null for non-string input', () => {
    expect(parseNatsJwtExp(null)).toBeNull()
  })
})
