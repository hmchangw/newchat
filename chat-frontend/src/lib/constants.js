// Frontend mirror of the server contract: constants the wire uses and a few
// tightly-coupled predicates that test them. Keep in sync with:
//   pkg/model/subscription.go (Role)
//   pkg/model/member.go       (HistoryMode)
//   pkg/model/event.go        (CreateRoomStatusExists)

export const ROLE_OWNER = 'owner'
// Legacy spelling of the plain-user role; the server normalizes it to 'user' on
// read and still accepts it as a role-update input.
export const ROLE_MEMBER = 'member'
export const ROLE_USER = 'user'

export const HISTORY_MODE_ALL = 'all'
export const HISTORY_MODE_NONE = 'none'

// New canonical DM-exists status (post errcode migration). The backend
// returns {status: STATUS_EXISTS, roomId: <existing>} as a SUCCESS reply.
export const STATUS_EXISTS = 'exists'

// Legacy DM-exists error string — the pre-migration reply was the error-shaped
// {error: this, roomId: existingId}. Accepted by isDMExistsReply during the
// backend rollout window so the frontend can deploy first; removed in a
// follow-up release once the new envelope is everywhere.
export const ERR_DM_ALREADY_EXISTS = 'dm already exists'

// Predicate for the DM-exists sync reply. Accepts BOTH shapes during the
// rollout window: the new {status:"exists", roomId} success envelope and the
// legacy {error:"dm already exists", roomId} 200-with-error. Callers that
// want to treat dedup as success should use this rather than re-deriving the
// check — see plan Chapter 19 for the cutover details.
export function isDMExistsReply(reply) {
  if (!reply || !reply.roomId) return false
  return reply.status === STATUS_EXISTS || reply.error === ERR_DM_ALREADY_EXISTS
}
