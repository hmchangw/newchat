/**
 * Which rooms count toward the unread badge.
 *
 * Mirrors user-service's `unreadRooms()` (the `subscription.count` rule) so the
 * locally folded number and the server's agree: a room counts ONCE when it is
 * not muted and either its newest message is past the user's read position or
 * it carries an unread followed thread.
 *
 * The one deliberate deviation is `activeRoomId` — the server has no concept of
 * an open window. The two converge because opening a room marks it read; see
 * the design note in docs/superpowers/specs/2026-08-19-client-side-unread-badge-design.md.
 */

/** A room event at `at` is unread relative to `lastSeenAt`. Never read (no
 *  lastSeenAt) counts as unread as long as the room has content at all. */
function isAfter(at, lastSeenAt) {
  if (!at) return false
  if (!lastSeenAt) return true
  return Date.parse(at) > Date.parse(lastSeenAt)
}

/**
 * @param {object} state RoomEventsContext state (summaries + subscriptions + activeRoomId)
 * @param {boolean} isVisible whether the window is in front of the user — when
 *   false there is no room being read, so the active room is NOT suppressed
 * @returns {string[]} unread room IDs, in `summaries` order
 */
export function selectUnreadRoomIds(state, isVisible) {
  const ids = []
  for (const room of state.summaries) {
    const sub = state.subscriptions[room.id]
    if (sub?.muted) continue
    // Suppression covers the MAIN FEED only: sitting in a room does not read
    // its threads, and the server counts an unread thread either way.
    const reading = isVisible && room.id === state.activeRoomId
    const unreadByMessage = !reading && isAfter(room.lastMsgAt, sub?.lastSeenAt)
    const unreadByThread = (sub?.threadUnread?.length ?? 0) > 0
    if (unreadByMessage || unreadByThread) ids.push(room.id)
  }
  return ids
}
