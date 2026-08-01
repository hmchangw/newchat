// Pure chatlist helpers — built-in section ids, the default overlay, and
// section-name validation. No React, no NATS. Mirrors the backend's derived
// built-ins + name rules (docs/client-api.md "Chatlist Sections").

/** Built-in section ids. Membership is derived client-side, never stored on
 *  a subscription. Order here is the default display order (custom sections
 *  slot in just above `chats`). */
export const BUILTIN_FAVORITES = 'favorites'
export const BUILTIN_APPS = 'apps'
export const BUILTIN_TEAMS = 'teams'
export const BUILTIN_CHATS = 'chats'

export const BUILTIN_SECTION_IDS = [
  BUILTIN_FAVORITES,
  BUILTIN_APPS,
  BUILTIN_TEAMS,
  BUILTIN_CHATS,
]

const BUILTIN_TITLES = {
  [BUILTIN_FAVORITES]: 'Favorites',
  [BUILTIN_APPS]: 'Apps',
  [BUILTIN_TEAMS]: 'Teams',
  [BUILTIN_CHATS]: 'Chats',
}

export function isBuiltinSectionId(id) {
  return BUILTIN_SECTION_IDS.includes(id)
}

/** The default overlay a never-customized user starts from: the four
 *  built-ins, `mostRecent` sort, in canonical order.
 *  @returns {import('../api/types').ChatlistState} */
export function defaultChatlistState() {
  return {
    sectionOrder: [...BUILTIN_SECTION_IDS],
    sections: BUILTIN_SECTION_IDS.map((id) => ({
      id,
      name: BUILTIN_TITLES[id],
      builtIn: true,
      sortMode: 'mostRecent',
    })),
    lastUpdatedAt: 0,
  }
}

// Section name: 1–50 chars trimmed, no consecutive spaces, only
// letters/digits (any script) / space / - _ . / ( ). Mirrors the backend
// validator so the UI can pre-flight before the RPC round-trip.
const NAME_ALLOWED = /^[\p{L}\p{N} \-_./()]+$/u

/** Validate a custom-section name. Returns `{ ok: true, name }` (trimmed) or
 *  `{ ok: false, reason }` with the same reason code the backend emits.
 *  @returns {{ ok: true, name: string } | { ok: false, reason: string }} */
export function validateSectionName(raw) {
  const name = String(raw ?? '').trim()
  if (name.length < 1 || name.length > 50) return { ok: false, reason: 'chatlist_invalid_name' }
  if (name.includes('  ')) return { ok: false, reason: 'chatlist_invalid_name' }
  if (!NAME_ALLOWED.test(name)) return { ok: false, reason: 'chatlist_invalid_name' }
  return { ok: true, name }
}
