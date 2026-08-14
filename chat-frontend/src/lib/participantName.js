// Display-name resolution for message senders. One rule, two shapes: a wire
// Participant (nested on a PreviewMessage) and a Message (which carries the
// server-composed name at the top level). No React, no I/O.

/**
 * Resolve a wire Participant to a render-ready name. `displayName` is composed
 * server-side (engName + chineseName + account fallback; a bot's is its app
 * name), so it wins when present.
 *
 * @param {{ displayName?: string, engName?: string, account?: string, userId?: string } | null | undefined} participant
 * @returns {string}
 */
export function participantDisplayName(participant) {
  if (!participant) return 'Unknown'
  return participant.displayName || participant.engName || participant.account || participant.userId || 'Unknown'
}

/**
 * Resolve a Message's sender name. The top-level `userDisplayName` is the
 * server's render-ready composition and is preferred over the nested sender.
 *
 * @param {{ userDisplayName?: string, sender?: object, userAccount?: string, userId?: string } | null | undefined} msg
 * @returns {string}
 */
export function messageSenderName(msg) {
  if (!msg) return 'Unknown'
  if (msg.userDisplayName) return msg.userDisplayName
  if (msg.sender) return participantDisplayName(msg.sender)
  return msg.userAccount || msg.userId || 'Unknown'
}
