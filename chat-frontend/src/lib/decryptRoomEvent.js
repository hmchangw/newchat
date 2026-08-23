/** Decrypts one encrypted envelope, fetching the room key once if the first
 *  attempt misses. Returns null when the key never arrives. */
async function open(envelope, roomId, siteId, { decrypt, ensureKey }) {
  const { version, nonce, ciphertext } = envelope
  if (typeof version !== 'number' || !nonce || !ciphertext) return null
  const input = { roomId, version, nonceB64: nonce, ciphertextB64: ciphertext }
  const plaintext = await decrypt(input)
  if (plaintext != null || !roomId || !siteId) return plaintext
  return (await ensureKey(roomId, version, siteId)) ? decrypt(input) : null
}

/**
 * Resolves a room event's encrypted fields into their plaintext counterparts:
 * `encryptedMessage` → `message`, and an edit's `encryptedNewContent` →
 * `newContent`. Shared by every lane that carries room events, so a viewer's
 * thread subscription decrypts exactly as the room subscription does.
 *
 * Returns the event unchanged when it carries no ciphertext, when the room key
 * never arrives, or when decryption throws — callers render the placeholder.
 *
 * @param {object} evt Room event as received off the wire.
 * @param {{decrypt: Function, ensureKey: Function, fallbackSiteId?: string}} deps
 * @returns {Promise<object>}
 */
export async function decryptRoomEvent(evt, deps) {
  let decoded = evt
  try {
    const siteId = evt.siteId ?? deps.fallbackSiteId
    if (decoded.encryptedMessage && !decoded.message) {
      const plaintext = await open(decoded.encryptedMessage, decoded.roomId, siteId, deps)
      if (plaintext != null) {
        decoded = { ...decoded, message: JSON.parse(plaintext), encryptedMessage: undefined }
      }
    }
    if (decoded.type === 'message_edited' && decoded.encryptedNewContent && !decoded.newContent) {
      const plaintext = await open(decoded.encryptedNewContent, decoded.roomId, siteId, deps)
      if (plaintext != null) {
        decoded = { ...decoded, newContent: plaintext, encryptedNewContent: undefined }
      }
    }
  } catch (err) {
    // eslint-disable-next-line no-console
    console.warn('decryptRoomEvent failed; forwarding original event', err)
    return evt
  }
  return decoded
}
