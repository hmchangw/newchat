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

func RoomsCreate(account string) string {
	return fmt.Sprintf("chat.user.%s.request.rooms.create", account)
}

func RoomsList(account string) string {
	return fmt.Sprintf("chat.user.%s.request.rooms.list", account)
}

func RoomsGet(account, roomID string) string {
	return fmt.Sprintf("chat.user.%s.request.rooms.get.%s", account, roomID)
}

// RoomsInfoBatch is the server-to-server request subject for batch room info lookups.
func RoomsInfoBatch(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.info.batch", siteID)
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

func MsgCanonicalWildcard(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.>", siteID)
}

func OutboxWildcard(siteID string) string {
	return fmt.Sprintf("outbox.%s.>", siteID)
}

func RoomsCreateWildcard() string {
	return "chat.user.*.request.rooms.create"
}

func RoomsListWildcard() string {
	return "chat.user.*.request.rooms.list"
}

func RoomsGetWildcard() string {
	return "chat.user.*.request.rooms.get.*"
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

func MemberAdd(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.add", account, roomID, siteID)
}

func MemberAddWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.add", siteID)
}

func RoomMemberEvent(roomID string) string {
	return fmt.Sprintf("chat.room.%s.event.member", roomID)
}

// --- search-service request/reply builders ---

// SearchMessages builds the concrete subject for a message search request.
func SearchMessages(account string) string {
	return fmt.Sprintf("chat.user.%s.request.search.messages", account)
}

// SearchRooms builds the concrete subject for a room search request.
func SearchRooms(account string) string {
	return fmt.Sprintf("chat.user.%s.request.search.rooms", account)
}

// SearchMessagesPattern is the natsrouter pattern for message search, used
// during registration to extract `{account}` from incoming subjects.
func SearchMessagesPattern() string {
	return "chat.user.{account}.request.search.messages"
}

// SearchRoomsPattern is the natsrouter pattern for room search.
func SearchRoomsPattern() string {
	return "chat.user.{account}.request.search.rooms"
}
