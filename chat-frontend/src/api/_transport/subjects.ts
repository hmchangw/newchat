// Subject builders take the account exactly as auth-service returned it in
// `user.account`. That value has to match the JWT's own `account` tag, which
// is what the scoped template's `chat.user.{{tag(account)}}.>` grant is
// evaluated against — so it is used verbatim here. Do NOT re-normalise it: Go
// and JavaScript disagree on non-ASCII lowercasing, and re-deriving would
// reintroduce that divergence.

// NATS subject builders — mirrors Go pkg/subject/subject.go
// Keep in sync with the Go definitions when adding new subjects.

export function msgSend(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.room.${roomId}.${siteId}.msg.send`
}

export function msgHistory(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.msg.history`
}

export function msgSurrounding(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.msg.surrounding`
}

export function msgThread(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.msg.thread`
}

export function msgEdit(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.msg.edit`
}

export function msgDelete(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.msg.delete`
}

// roomBase is the per-room subject root, mirroring Go's subject.roomBase.
// Cross-site rooms stay on the global namespace; same-site rooms route to the
// local one so a down remote peer can't affect same-site delivery. Fail-safe:
// only an explicit `false` routes to the site-local (leaf-filtered) namespace;
// true/undefined/missing → global, matching the server-side default.
function roomBase(roomId: string, crossSite: boolean): string {
  return crossSite === false ? `chat.local.room.${roomId}` : `chat.room.${roomId}`
}

export function roomEvent(roomId: string, crossSite: boolean): string {
  return `${roomBase(roomId, crossSite)}.event`
}

// roomThreadEvent is the thread-scoped lane a client subscribes to while a
// thread panel is open, so a viewer who follows nothing still sees replies.
export function roomThreadEvent(roomId: string, parentMessageId: string, crossSite: boolean): string {
  return `${roomBase(roomId, crossSite)}.thread.${parentMessageId}.event`
}

// roomCreate is the room-service create subject. The site segment is the
// requester's site — room-service queue-subscribes on its own siteID, so a
// caller from site-A always lands its create on the site-A room-service.
export function roomCreate(account: string, siteId: string): string {
  return `chat.user.${account}.request.room.${siteId}.create`
}

export function subscriptionUpdate(account: string): string {
  return `chat.user.${account}.event.subscription.update`
}

export function roomMetadataUpdate(account: string): string {
  return `chat.user.${account}.event.room.metadata.update`
}

export function userRoomEvent(account: string): string {
  return `chat.user.${account}.event.room`
}

export function userRoomKey(account: string): string {
  return `chat.user.${account}.event.room.key`
}

export function memberAdd(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.member.add`
}

export function memberRemove(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.member.remove`
}

export function memberRoleUpdate(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.member.role-update`
}

export function readReceipt(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.message.read-receipt`
}

// messageRead is fire-and-forget — advances the caller's lastSeenAt to
// `now()` on the server so subsequent read-receipt RPCs reflect the
// current state. Mirrors pkg/subject/subject.go::MessageRead.
export function messageRead(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.message.read`
}

export function memberList(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.member.list`
}

// roomKeyGet requests the room key bytes for (roomId, version?) from
// room-service. Pair with src/api/requestRoomKey/. Mirrors
// pkg/subject/subject.go::RoomKeyGet.
export function roomKeyGet(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.key.get`
}

// userResponse is where room-worker publishes AsyncJobResult after finishing
// a deferred operation. The client subscribes here before publishing the
// request and X-Request-ID header so it can match the result back.
export function userResponse(account: string, requestId: string): string {
  return `chat.user.${account}.response.${requestId}`
}

export function searchRooms(account: string, siteId: string): string {
  return `chat.user.${account}.request.search.${siteId}.rooms`
}

export function searchMessages(account: string, siteId: string): string {
  return `chat.user.${account}.request.search.${siteId}.messages`
}

// orgMembers requests the enriched member list of a single org (sect).
// Used by MemberRoster to expand an org row into its individual members.
// Response shape: { members: [{ id, account, engName, chineseName, siteId }] }.
// Mirrors pkg/subject/subject.go::OrgMembers.
export function orgMembers(account: string, orgId: string): string {
  return `chat.user.${account}.request.orgs.${orgId}.members`
}

// userSubscriptionList fetches the caller's subscriptions filtered by type.
// Pass `{ type: "current", favorite: true }` for the Favorite section,
// `{ type: "apps" }` for the Apps section, or `{ type: "rooms" }` for the
// Channels and DMs section. Mirrors
// pkg/subject/subject.go::UserSubscriptionList.
export function userSubscriptionList(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.subscription.list`
}

// userSubscriptionCount fetches a count of the caller's subscriptions.
// The unread badge passes `{ unread: true }` to get the unread-message
// total. Mirrors pkg/subject/subject.go::UserSubscriptionCount.
export function userSubscriptionCount(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.subscription.count`
}

// Chatlist custom-sections RPCs (user-service). The section DEFINITIONS live
// in a small per-user overlay; a chat's section MEMBERSHIP rides its
// subscription (see chatMove). Mirrors pkg/subject chatlist builders.
export function chatlistGet(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.get`
}

export function chatlistSectionCreate(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.section.create`
}

export function chatlistSectionRename(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.section.rename`
}

export function chatlistSectionDelete(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.section.delete`
}

export function chatlistSectionReorder(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.section.reorder`
}

export function chatlistSectionSetSortMode(account: string, siteId: string): string {
  return `chat.user.${account}.request.user.${siteId}.chatlist.section.setsortmode`
}

// chatMove sets/clears a chat's section membership + manual order on its
// subscription. `{siteID}` is the room's origin site, like the other
// room-scoped RPCs. Emits subscription.update (action "section_moved").
export function chatMove(account: string, roomId: string, siteId: string): string {
  return `chat.user.${account}.request.room.${roomId}.${siteId}.chat.move`
}

// chatlistUpdate is the caller-fanned event carrying the full post-update
// section definitions (ChatlistUpdateEvent). Replace-wholesale, LWW by
// chatlist.lastUpdatedAt.
export function chatlistUpdate(account: string): string {
  return `chat.user.${account}.event.chatlist.update`
}
