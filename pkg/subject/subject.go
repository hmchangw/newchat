package subject

import (
	"fmt"
	"strings"
)

// ParseUserRoomSubject extracts the user account and roomID from subjects
// matching the pattern "chat.user.{account}.*.room.{roomID}.…".
// Returns the user account, roomID, and ok=true on success.
func ParseUserRoomSubject(subj string) (account, roomID string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) < 5 || parts[0] != "chat" || parts[1] != "user" {
		return "", "", false
	}
	account = parts[2]
	// Find "room" token after user position
	for i := 3; i < len(parts)-1; i++ {
		if parts[i] == "room" {
			return account, parts[i+1], true
		}
	}
	return "", "", false
}

func ParseUserRoomSiteSubject(subj string) (account, roomID, siteID string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) < 7 || parts[0] != "chat" || parts[1] != "user" || parts[3] != "room" {
		return "", "", "", false
	}
	return parts[2], parts[4], parts[5], true
}

// --- Specific subject builders ---

func MsgSend(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.room.%s.%s.msg.send", account, roomID, siteID)
}

// MsgGet returns the concrete subject for issuing a GetMessageByID request to
// history-service on behalf of a given user/room. Pair with MsgGetPattern,
// which is the natsrouter pattern used by history-service to register the
// handler.
func MsgGet(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.msg.get", account, roomID, siteID)
}

func UserResponse(account, requestID string) string {
	return fmt.Sprintf("chat.user.%s.response.%s", account, requestID)
}

func RoomMetadataUpdate(roomID string) string {
	return fmt.Sprintf("chat.room.%s.event.metadata.update", roomID)
}

func RoomMsgStream(roomID string) string {
	return fmt.Sprintf("chat.room.%s.stream.msg", roomID)
}

func UserRoomUpdate(account string) string {
	return fmt.Sprintf("chat.user.%s.event.room.update", account)
}

func UserMsgStream(account string) string {
	return fmt.Sprintf("chat.user.%s.stream.msg", account)
}

func MemberRoleUpdate(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.role-update", account, roomID, siteID)
}

func MemberRemove(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.remove", account, roomID, siteID)
}

func MemberList(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.list", account, roomID, siteID)
}

func MemberEvent(roomID string) string {
	return fmt.Sprintf("chat.room.%s.event.member", roomID)
}

func RoomCanonical(siteID, operation string) string {
	return fmt.Sprintf("chat.room.canonical.%s.%s", siteID, operation)
}

func SubscriptionUpdate(account string) string {
	return fmt.Sprintf("chat.user.%s.event.subscription.update", account)
}

func RoomMetadataChanged(account string) string {
	return fmt.Sprintf("chat.user.%s.event.room.metadata.update", account)
}

func Notification(account string) string {
	return fmt.Sprintf("chat.user.%s.notification", account)
}

func Outbox(siteID, destSiteID, eventType string) string {
	return fmt.Sprintf("outbox.%s.to.%s.%s", siteID, destSiteID, eventType)
}

// InboxMemberAdded is the local-publish subject for a same-site member_added
// event. It lands in the local INBOX stream without the `aggregate` segment.
func InboxMemberAdded(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.member_added", siteID)
}

// InboxMemberRemoved is the local-publish subject for a same-site
// member_removed event. It lands in the local INBOX stream without the
// `aggregate` segment.
func InboxMemberRemoved(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.member_removed", siteID)
}

// InboxMemberAddedAggregate is the transformed subject for a federated
// member_added event after INBOX SubjectTransform rewrites
// `outbox.{src}.to.{siteID}.member_added` to this form.
func InboxMemberAddedAggregate(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.aggregate.member_added", siteID)
}

// InboxMemberRemovedAggregate is the transformed subject for a federated
// member_removed event.
func InboxMemberRemovedAggregate(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.aggregate.member_removed", siteID)
}

// InboxAggregateAll returns the wildcard pattern matching every federated
// (aggregate-lane) event on a site's INBOX stream:
// `chat.inbox.{siteID}.aggregate.>`. Use with
// jetstream.ConsumerConfig.FilterSubjects to scope a consumer to the
// federated lane only — excluding local-lane publishes that are reserved
// for search-sync-worker.
func InboxAggregateAll(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.aggregate.>", siteID)
}

// InboxMemberEventSubjects returns the subject filters a consumer should use
// to receive both local and federated member_added/member_removed events for
// the given site. Use with jetstream.ConsumerConfig.FilterSubjects (NATS 2.10+).
func InboxMemberEventSubjects(siteID string) []string {
	return []string{
		InboxMemberAdded(siteID),
		InboxMemberRemoved(siteID),
		InboxMemberAddedAggregate(siteID),
		InboxMemberRemovedAggregate(siteID),
	}
}

func MsgCanonicalCreated(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.created", siteID)
}

func MsgCanonicalUpdated(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.updated", siteID)
}

func MsgCanonicalDeleted(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.deleted", siteID)
}

func MsgCanonicalPinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.pinned", siteID)
}

func MsgCanonicalUnpinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.unpinned", siteID)
}

func RoomEvent(roomID string) string {
	return fmt.Sprintf("chat.room.%s.event", roomID)
}

func UserRoomEvent(account string) string {
	return fmt.Sprintf("chat.user.%s.event.room", account)
}

func RoomKeyUpdate(account string) string {
	return fmt.Sprintf("chat.user.%s.event.room.key", account)
}

// --- Room CRUD request builders ---

// RoomsInfoBatch is the server-to-server request subject for batch room info lookups.
func RoomsInfoBatch(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.info.batch", siteID)
}

// RoomKeyEnsure is the server-to-server request subject for the room key ensure
// RPC. Callers send a RoomKeyEnsureRequest and receive a RoomKeyEnsureResponse
// confirming the room has a key pair in Valkey at the returned version.
func RoomKeyEnsure(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.key.ensure", siteID)
}

// RoomKeyGet is the user-facing request subject for the on-demand room
// key fetch RPC. Pair with RoomKeyGetWildcard for room-service's
// QueueSubscribe. The reply mirrors RoomKeyEvent minus Timestamp.
func RoomKeyGet(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.key.get", account, roomID, siteID)
}

// RoomKeyGetWildcard is the subscription pattern room-service uses to
// receive RoomKeyGet requests from any account / roomID at its siteID.
func RoomKeyGetWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.key.get", siteID)
}

// RoomCreateDMSync is the server-to-server request subject for synchronous DM/botDM creation.
func RoomCreateDMSync(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.create.dm", siteID)
}

// --- Wildcard patterns for subscriptions ---

func MsgSendWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.room.*.%s.msg.send", siteID)
}

func MemberRoleUpdateWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.role-update", siteID)
}

func MemberRemoveWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.remove", siteID)
}

func MemberListWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.list", siteID)
}

// OrgMembers builds the subject for listing members of an org.
func OrgMembers(account, orgID string) string {
	return fmt.Sprintf("chat.user.%s.request.orgs.%s.members", account, orgID)
}

// OrgMembersWildcard is the subscription pattern for the list-org-members endpoint.
func OrgMembersWildcard() string {
	return "chat.user.*.request.orgs.*.members"
}

// ParseOrgMembersSubject returns the orgID from a subject matching the
// pattern "chat.user.{account}.request.orgs.{orgId}.members".
// Tokens (by strings.Split on "."): [0]chat [1]user [2]{account} [3]request
// [4]orgs [5]{orgId} [6]members. orgID is at positional index 5.
func ParseOrgMembersSubject(subj string) (orgID string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 7 {
		return "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" ||
		parts[4] != "orgs" || parts[6] != "members" {
		return "", false
	}
	return parts[5], true
}

func RoomCanonicalWildcard(siteID string) string {
	return fmt.Sprintf("chat.room.canonical.%s.>", siteID)
}

func MsgHistoryWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.msg.history", siteID)
}

func MsgThreadWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.msg.thread", siteID)
}

func MsgCanonicalWildcard(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.>", siteID)
}

func OutboxWildcard(siteID string) string {
	return fmt.Sprintf("outbox.%s.>", siteID)
}

// RoomsInfoBatchSubscribe is the per-site subscription subject for room-service.
func RoomsInfoBatchSubscribe(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.info.batch", siteID)
}

func UserResponseWildcard() string {
	return "chat.user.*.response.>"
}

func RoomEventWildcard() string {
	return "chat.room.*.event"
}

func UserRoomEventWildcard() string {
	return "chat.user.*.event.room"
}

// --- natsrouter patterns (use {param} placeholders for named extraction) ---

func MsgHistoryPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.history", siteID)
}

func MsgNextPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.next", siteID)
}

func MsgSurroundingPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.surrounding", siteID)
}

// MsgGetPattern is the natsrouter pattern history-service uses to register
// its GetMessageByID handler. Pair with MsgGet for the concrete-subject form
// callers publish on.
func MsgGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.get", siteID)
}

// MsgEditPattern is the natsrouter pattern for editing a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgEditPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.edit", siteID)
}

// MsgDeletePattern is the natsrouter pattern for soft-deleting a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgDeletePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.delete", siteID)
}

// MsgPinPattern is the natsrouter pattern for pinning a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgPinPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.pin", siteID)
}

// MsgUnpinPattern is the natsrouter pattern for unpinning a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgUnpinPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.unpin", siteID)
}

// MsgPinnedListPattern is the natsrouter pattern for listing a room's pinned messages.
func MsgPinnedListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.pinned.list", siteID)
}

func MsgThreadPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.thread", siteID)
}

// MsgHistory is the concrete-subject form clients publish on to invoke
// LoadHistory. Pair with MsgHistoryPattern for the server-side registration.
func MsgHistory(account, roomID, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.msg.history", account, roomID, siteID)
}

// MsgThread is the concrete-subject form clients publish on to invoke
// GetThreadMessages. Pair with MsgThreadPattern for the server-side registration.
func MsgThread(account, roomID, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.msg.thread", account, roomID, siteID)
}

func MemberAdd(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.add", account, roomID, siteID)
}

func MemberAddWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.add", siteID)
}

// MessageRead returns the concrete subject for the per-user message-read RPC.
// Pair with MessageReadWildcard for room-service's QueueSubscribe.
func MessageRead(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.message.read", account, roomID, siteID)
}

// MessageReadWildcard is the per-site subscription pattern for the message-read RPC.
func MessageReadWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.message.read", siteID)
}

func MessageReadReceipt(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.message.read-receipt", account, roomID, siteID)
}

func MessageReadReceiptWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.message.read-receipt", siteID)
}

// MessageThreadRead returns the concrete subject for the per-user mark-thread-as-read RPC.
func MessageThreadRead(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.message.thread.read", account, roomID, siteID)
}

// MessageThreadReadWildcard is the per-site subscription pattern for the mark-thread-as-read RPC.
func MessageThreadReadWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.message.thread.read", siteID)
}

// MuteToggle returns the concrete subject for the per-user mute.toggle RPC.
func MuteToggle(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.mute.toggle", account, roomID, siteID)
}

// MuteToggleWildcard is the per-site subscription pattern for the mute.toggle RPC.
func MuteToggleWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.mute.toggle", siteID)
}

// RoomCreate: client→room-service create subject; siteID is the requester's site.
func RoomCreate(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.create", account, siteID)
}

// RoomCreateWildcard is the queue-subscribe pattern for room-service.
func RoomCreateWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.%s.create", siteID)
}

func RoomMemberEvent(roomID string) string {
	return fmt.Sprintf("chat.room.%s.event.member", roomID)
}

// RoomMemberEventWildcard is the subscription pattern matching member events
// (member_added / member_removed) across all rooms on this site.
func RoomMemberEventWildcard() string {
	return "chat.room.*.event.member"
}

func MsgThreadParentPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.thread.parent", siteID)
}

func MsgThreadParentWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.msg.thread.parent", siteID)
}

// --- search-service request/reply builders ---

// SearchMessages builds the concrete subject for a message search request.
// The siteID routes the request through the NATS supercluster to the
// search-service running on that specific site.
func SearchMessages(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.search.%s.messages", account, siteID)
}

// SearchRooms builds the concrete subject for a subscription search request.
func SearchRooms(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.search.%s.rooms", account, siteID)
}

// SearchMessagesPattern is the natsrouter pattern for message search, used
// during registration to extract `{account}` from incoming subjects. siteID
// is baked in so each site only handles its own search traffic.
func SearchMessagesPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.search.%s.messages", siteID)
}

// SearchRoomsPattern is the natsrouter pattern for subscription search.
func SearchRoomsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.search.%s.rooms", siteID)
}

// SearchApps builds the concrete subject for an app search request.
func SearchApps(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.search.%s.apps", account, siteID)
}

// SearchAppsPattern is the natsrouter pattern for app search.
func SearchAppsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.search.%s.apps", siteID)
}

// SearchUsers builds the concrete subject for a user search request.
func SearchUsers(account, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.search.%s.users", account, siteID)
}

// SearchUsersPattern is the natsrouter pattern for user search.
func SearchUsersPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.search.%s.users", siteID)
}

// isValidAccountToken rejects empty tokens and tokens containing NATS wildcard
// characters ('*' or '>'). Subject parsers use it as the boundary guard for the
// account token so wildcard semantics never leak into identity parsing.
func isValidAccountToken(token string) bool {
	return token != "" && !strings.ContainsAny(token, "*>")
}

// ParseRoomCreateSubject extracts the account from chat.user.{account}.request.room.{siteID}.create.
func ParseRoomCreateSubject(s string) (account string, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 7 {
		return "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "room" || parts[6] != "create" {
		return "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", false
	}
	return parts[2], true
}

// RoomCanonicalOperation returns the trailing op (e.g. "member.add") from chat.room.canonical.{siteID}.{op}.
func RoomCanonicalOperation(s string) (string, bool) {
	const prefix = "chat.room.canonical."
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(s, prefix)
	dot := strings.IndexByte(rest, '.')
	if dot == -1 {
		return "", false
	}
	op := rest[dot+1:]
	if op == "" {
		return "", false
	}
	return op, true
}

// --- mock-user-service / future user-service builders ---

func UserStatusGetByName(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.getByName", account, siteID)
}

func UserStatusSet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.set", account, siteID)
}

func UserProfileGetByName(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.profile.getByName", account, siteID)
}

func UserSubscriptionGetCurrent(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getCurrent", account, siteID)
}

func UserSubscriptionGetRooms(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getRooms", account, siteID)
}

func UserSubscriptionGetChannels(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getChannels", account, siteID)
}

func UserSubscriptionGetDM(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getDM", account, siteID)
}

func UserSubscriptionGetApps(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getApps", account, siteID)
}

func UserSubscriptionCount(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.count", account, siteID)
}

func UserSubscriptionSubscribeApp(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.subscribeApp", account, siteID)
}

func UserSubscriptionUnsubscribeApp(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.unsubscribeApp", account, siteID)
}

func UserRoomSubscriptionGet(account, siteID, roomID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.room.%s.subscription.get", account, siteID, roomID)
}

func UserAppsList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.apps.list", account, siteID)
}

// --- natsrouter pattern builders (siteID baked in, account left as {account} placeholder) ---

func UserStatusGetByNamePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.status.getByName", siteID)
}

func UserStatusSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.status.set", siteID)
}

func UserProfileGetByNamePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.profile.getByName", siteID)
}

func UserSubscriptionGetCurrentPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getCurrent", siteID)
}

func UserSubscriptionGetRoomsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getRooms", siteID)
}

func UserSubscriptionGetChannelsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getChannels", siteID)
}

func UserSubscriptionGetDMPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getDM", siteID)
}

func UserSubscriptionGetAppsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getApps", siteID)
}

func UserSubscriptionCountPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.count", siteID)
}

func UserSubscriptionSubscribeAppPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.subscribeApp", siteID)
}

func UserSubscriptionUnsubscribeAppPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.unsubscribeApp", siteID)
}

func UserRoomSubscriptionGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.room.{roomID}.subscription.get", siteID)
}

func UserAppsListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.apps.list", siteID)
}

// ParseUserSubject parses any 8-token subject of the form
//
//	chat.user.{account}.request.user.{siteID}.{area}.{action}
//
// where area is one of "status", "subscription", "profile", "apps".
// Does NOT match the room-scoped form — use ParseRoomSubject for those.
func ParseUserSubject(subj string) (account, siteID, area, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 8 {
		return "", "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" {
		return "", "", "", "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", "", "", "", false
	}
	switch parts[6] {
	case "status", "subscription", "profile", "apps":
	default:
		return "", "", "", "", false
	}
	return parts[2], parts[5], parts[6], parts[7], true
}

func ParseStatusSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "status" {
		return "", "", false
	}
	return a, act, true
}

func ParseSubscriptionSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "subscription" {
		return "", "", false
	}
	return a, act, true
}

func ParseProfileSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "profile" {
		return "", "", false
	}
	return a, act, true
}

func ParseAppsSubject(subj string) (account, action string, ok bool) {
	a, _, area, act, k := ParseUserSubject(subj)
	if !k || area != "apps" {
		return "", "", false
	}
	return a, act, true
}

// ParseRoomSubject parses the 10-token room-scoped form
//
//	chat.user.{account}.request.user.{siteID}.room.{roomID}.{area}.{action}
//
// Returns the trailing `{action}` token (e.g. "get" for subscription.get).
// Returns ok=false if the subject is not exactly 10 tokens or does not
// start with `chat.user.*.request.user.*.room.*.`.
func ParseRoomSubject(subj string) (account, roomID, action string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 10 {
		return "", "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" || parts[4] != "user" || parts[6] != "room" {
		return "", "", "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", "", "", false
	}
	return parts[2], parts[7], parts[9], true
}

func UserStatusWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.status.>", siteID)
}

func UserSubscriptionWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.subscription.>", siteID)
}

func UserProfileWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.profile.>", siteID)
}

func UserRoomWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.room.>", siteID)
}

func UserAppsWildCard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.user.%s.apps.>", siteID)
}
