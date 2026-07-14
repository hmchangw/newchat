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

export function roomEvent(roomId: string): string {
  return `chat.room.${roomId}.event`
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

export function searchRooms(account: string): string {
  return `chat.user.${account}.request.search.rooms`
}

export function searchMessages(account: string): string {
  return `chat.user.${account}.request.search.messages`
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
