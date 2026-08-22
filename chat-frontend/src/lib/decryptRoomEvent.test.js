import { describe, it, expect, vi } from 'vitest'
import { decryptRoomEvent } from './decryptRoomEvent'

const envelope = { version: 3, nonce: 'bm9uY2U=', ciphertext: 'Y2lwaGVy' }
const deps = (decrypt, ensureKey = vi.fn(async () => false)) => ({
  decrypt,
  ensureKey,
  fallbackSiteId: 'siteA',
})

describe('decryptRoomEvent', () => {
  it('passes a plaintext event through untouched', async () => {
    const evt = { type: 'new_thread_message', roomId: 'r1', message: { id: 'm1' } }
    const decrypt = vi.fn()
    expect(await decryptRoomEvent(evt, deps(decrypt))).toBe(evt)
    expect(decrypt).not.toHaveBeenCalled()
  })

  it('decrypts encryptedMessage into message', async () => {
    const decrypt = vi.fn(async () => JSON.stringify({ id: 'm1', content: 'hi' }))
    const out = await decryptRoomEvent(
      { type: 'new_thread_message', roomId: 'r1', siteId: 'siteB', encryptedMessage: envelope },
      deps(decrypt)
    )
    expect(out.message).toEqual({ id: 'm1', content: 'hi' })
    expect(out.encryptedMessage).toBeUndefined()
    expect(decrypt).toHaveBeenCalledWith({
      roomId: 'r1', version: 3, nonceB64: 'bm9uY2U=', ciphertextB64: 'Y2lwaGVy',
    })
  })

  it('fetches the key once and retries when the first decrypt misses', async () => {
    const decrypt = vi.fn().mockResolvedValueOnce(null).mockResolvedValueOnce(JSON.stringify({ id: 'm1' }))
    const ensureKey = vi.fn(async () => true)
    const out = await decryptRoomEvent(
      { type: 'new_thread_message', roomId: 'r1', encryptedMessage: envelope },
      deps(decrypt, ensureKey)
    )
    expect(ensureKey).toHaveBeenCalledWith('r1', 3, 'siteA')
    expect(decrypt).toHaveBeenCalledTimes(2)
    expect(out.message).toEqual({ id: 'm1' })
  })

  it('leaves the event encrypted when the key never arrives', async () => {
    const evt = { type: 'new_thread_message', roomId: 'r1', encryptedMessage: envelope }
    const out = await decryptRoomEvent(evt, deps(vi.fn(async () => null)))
    expect(out.message).toBeUndefined()
    expect(out.encryptedMessage).toEqual(envelope)
  })

  it('decrypts encryptedNewContent into newContent on an edit', async () => {
    const decrypt = vi.fn(async () => 'edited body')
    const out = await decryptRoomEvent(
      { type: 'message_edited', roomId: 'r1', messageId: 'm1', encryptedNewContent: envelope },
      deps(decrypt)
    )
    expect(out.newContent).toBe('edited body')
    expect(out.encryptedNewContent).toBeUndefined()
  })

  it('returns the original event when decryption throws', async () => {
    const evt = { type: 'new_thread_message', roomId: 'r1', encryptedMessage: envelope }
    const out = await decryptRoomEvent(evt, deps(vi.fn(async () => { throw new Error('boom') })))
    expect(out).toBe(evt)
  })

  // A malformed envelope must not reach decrypt: the room key is irrelevant if
  // the nonce or ciphertext is missing.
  it('ignores a malformed envelope', async () => {
    const decrypt = vi.fn()
    const evt = { type: 'new_thread_message', roomId: 'r1', encryptedMessage: { version: 3 } }
    expect(await decryptRoomEvent(evt, deps(decrypt))).toBe(evt)
    expect(decrypt).not.toHaveBeenCalled()
  })
})
