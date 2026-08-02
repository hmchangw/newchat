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

function lastMsgTime(summary) {
  return summary.lastMsgAt ? new Date(summary.lastMsgAt).getTime() : 0
}

function sortByLastMsgDesc(rooms) {
  return [...rooms].sort((a, b) => lastMsgTime(b) - lastMsgTime(a))
}

/**
 * Derive the grouped sidebar sections from the room summaries, the per-room
 * subscriptions, and the section-definition overlay — the P3 read model.
 *
 * Each room lands in EXACTLY ONE section, by this precedence (mirrors the
 * backend read model): favorite → Favorites; bot → Apps; sectionId=='teams'
 * → Teams; sectionId==<known custom> → that section; otherwise (no sectionId,
 * an orphaned sectionId, or a plain chat) → Chats.
 *
 * Within a section: `sortMode == 'custom'` orders by the members'
 * `subscription.sectionOrder` (unordered members last); otherwise by last
 * message, descending.
 *
 * Visibility: empty built-in sections (Favorites/Apps/Teams) are hidden;
 * custom sections and Chats always render (so a custom section stays a visible
 * drop target and Chats is the always-present home).
 *
 * @param {import('../context/RoomEventsContext/RoomEventsContext').RoomSummary[]} summaries
 * @param {Record<string, import('../api/types').DMSubscription>} subscriptions
 * @param {import('../api/types').ChatlistState} chatlist
 */
export function deriveSidebarSections(summaries, subscriptions, chatlist) {
  // Built-in section defs (Favorites/Apps/Teams/Chats) are ALWAYS available as
  // derivation targets — they're derived client-side, not carried in the backend
  // overlay (which holds only custom section defs). Merge the built-in defaults
  // UNDER any custom overlay so favorite/bot/sectionId=='teams' rooms always have
  // a home; without this the built-in buckets vanished the moment a custom section
  // existed, and those rooms wrongly fell back to Chats. Order: built-ins top,
  // custom sections (overlay order) in the middle, Chats last. Empty built-ins
  // (Favorites/Apps/Teams) are hidden by the visibility filter below.
  const base = defaultChatlistState()
  const custom = chatlist?.sections?.length ? chatlist : { sectionOrder: [], sections: [] }
  const byId = new Map()
  for (const s of base.sections) byId.set(s.id, s)
  for (const s of custom.sections) byId.set(s.id, s)
  const customIds = custom.sectionOrder.filter((id) => byId.has(id) && !isBuiltinSectionId(id))
  const order = [BUILTIN_FAVORITES, BUILTIN_APPS, BUILTIN_TEAMS, ...customIds, BUILTIN_CHATS].filter(
    (id, i, a) => a.indexOf(id) === i,
  )
  const buckets = new Map(order.map((id) => [id, []]))

  for (const room of summaries) {
    const sub = subscriptions[room.id]
    const isBot = room.type === 'botDM' || !!sub?.u?.isBot
    const sectionId = sub?.sectionId
    let target
    if (sub?.favorite) target = BUILTIN_FAVORITES
    else if (isBot) target = BUILTIN_APPS
    else if (sectionId && byId.has(sectionId)) target = sectionId
    else target = BUILTIN_CHATS
    if (!buckets.has(target)) target = BUILTIN_CHATS
    buckets.get(target).push(room)
  }

  const orderOf = (room) => {
    const o = subscriptions[room.id]?.sectionOrder
    return typeof o === 'number' ? o : Infinity
  }
  const sections = order.map((id) => {
    const def = byId.get(id)
    const rooms = buckets.get(id) ?? []
    const sorted =
      def.sortMode === 'custom'
        ? [...rooms].sort((a, b) => orderOf(a) - orderOf(b))
        : sortByLastMsgDesc(rooms)
    return { key: id, title: def.name, sortMode: def.sortMode, builtIn: def.builtIn, rooms: sorted }
  })
  return sections.filter((s) => s.rooms.length > 0 || !s.builtIn || s.key === BUILTIN_CHATS)
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
