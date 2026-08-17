// Mirrors the server's system-message type set (pkg/model/event.go:624-659).
// A normal user message has Type === '' — these seven are server-emitted
// events that happen to ride the same MESSAGES-CANONICAL -> new_message
// fan-out as ordinary content, so eligibility for previews/notifications
// must be decided by set membership, never by `type !== ''`.
//
// Critical: MessageTypeImportant ("important") is deliberately NOT in this
// set (event.go:661-667) — it's the sole client-settable type, and it
// previews + notifies like a normal message.

export const SYSTEM_MESSAGE_TYPES = new Set([
  'room_created',
  'members_added',
  'member_removed',
  'member_left',
  'room_renamed',
  'room_restricted',
  'teams_meet_started',
])

/**
 * @param {string | null | undefined} type
 * @returns {boolean}
 */
export function isSystemMessageType(type) {
  return !!type && SYSTEM_MESSAGE_TYPES.has(type)
}
