import { describe, it, expect } from 'vitest'
import { AsyncJobError, formatAsyncJobError, parseHttpEnvelopeError } from './httpEnvelope'

function mockResponse(status: number, body: unknown): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
  } as unknown as Response
}

describe('parseHttpEnvelopeError', () => {
  it('throws AsyncJobError populated from the envelope body', async () => {
    const resp = mockResponse(403, {
      code: 'forbidden',
      reason: 'not_admin',
      error: 'admin role required',
    })
    await expect(parseHttpEnvelopeError(resp, 'request failed')).rejects.toBeInstanceOf(
      AsyncJobError,
    )
    await expect(parseHttpEnvelopeError(resp, 'request failed')).rejects.toMatchObject({
      code: 'forbidden',
      reason: 'not_admin',
      message: 'admin role required',
    })
  })

  it('falls back to the fallback message when the body has no error/code', async () => {
    const resp = mockResponse(500, { some: 'unrelated shape' })
    await expect(parseHttpEnvelopeError(resp, 'request failed')).rejects.toMatchObject({
      message: 'request failed',
    })
  })

  it('falls back to the fallback message when the body is not JSON', async () => {
    const resp = {
      ok: false,
      status: 500,
      json: async () => {
        throw new Error('unexpected token')
      },
    } as unknown as Response
    await expect(parseHttpEnvelopeError(resp, 'request failed')).rejects.toMatchObject({
      message: 'request failed',
    })
  })
})

describe('formatAsyncJobError', () => {
  it('returns friendly copy for not_admin', () => {
    const err = new AsyncJobError('admin role required', { reason: 'not_admin' })
    const msg = formatAsyncJobError(err)
    expect(msg).not.toBe('')
    expect(msg).not.toBe('admin role required')
  })

  it('returns friendly copy for account_exists', () => {
    const err = new AsyncJobError('email already in use', { reason: 'account_exists' })
    const msg = formatAsyncJobError(err)
    expect(msg).not.toBe('')
    expect(msg).not.toBe('email already in use')
  })

  it('returns friendly copy for invalid_token', () => {
    const err = new AsyncJobError('token invalid', { reason: 'invalid_token' })
    const msg = formatAsyncJobError(err)
    expect(msg).not.toBe('')
    expect(msg).not.toBe('token invalid')
  })

  it('falls back to err.message for an unknown reason', () => {
    const err = new AsyncJobError('something odd happened', { reason: 'not_in_catalog' })
    expect(formatAsyncJobError(err)).toBe('something odd happened')
  })

  it('falls back to err.message when reason is absent', () => {
    const err = new AsyncJobError('plain failure')
    expect(formatAsyncJobError(err)).toBe('plain failure')
  })

  it('returns empty string for a falsy input', () => {
    expect(formatAsyncJobError(undefined)).toBe('')
  })
})
