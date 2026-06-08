// Decode the `exp` (unix seconds) claim from a NATS user JWT WITHOUT verifying
// the signature. Used only to schedule client-side token refresh; never for
// any trust/authorization decision. Returns null on any malformed input.
export function parseNatsJwtExp(jwt) {
  const parts = String(jwt).split('.')
  if (parts.length < 2) return null
  try {
    const b64 = parts[1].replace(/-/g, '+').replace(/_/g, '/')
    const payload = JSON.parse(atob(b64))
    const exp = payload?.exp
    return typeof exp === 'number' && Number.isFinite(exp) ? exp : null
  } catch {
    return null
  }
}
