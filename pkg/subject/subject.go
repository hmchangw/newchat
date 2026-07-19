package subject

import (
	"fmt"
	"strings"
	"unicode"
)

// IsValidAccountToken reports whether s can serve as the {account} token of a
// NATS subject: non-empty, no '.'/'*'/'>' runes, no whitespace or control runes.
func IsValidAccountToken(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r == '.' || r == '*' || r == '>' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// ParseUserRoomSubject extracts the user account and roomID from subjects
// matching the pattern "chat.user.{account}.*.room.{roomID}.…".
// Returns the user account, roomID, and ok=true on success.
func ParseUserRoomSubject(subj string) (account, roomID string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) < 5 || parts[0] != "chat" || parts[1] != "user" {
		return "", "", false
	}
	account = parts[2]
	if !isValidAccountToken(account) {
		return "", "", false
	}
	// Find "room" token after user position
	for i := 3; i < len(parts)-1; i++ {
		if parts[i] == "room" {
			roomID = parts[i+1]
			if !isValidAccountToken(roomID) {
				return "", "", false
			}
			return account, roomID, true
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

// MsgGetIDs returns the concrete subject for a GetMessagesByIDs batch request.
// Pair with MsgGetIDsPattern, which history-service uses to register the handler.
func MsgGetIDs(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.msg.get.ids", account, roomID, siteID)
}

// RoomsGet is the server-to-server request subject for the rooms.get batch RPC:
// user-service asks history-service for each room's last message. Account-agnostic
// (roomIds batch in the body); the publish and subscribe forms are identical, like
// ThreadRoomInfoBatch.
func RoomsGet(siteID string) string {
	return fmt.Sprintf("chat.server.request.history.%s.rooms.get", siteID)
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

// RoomRename is the request/reply subject for the rename RPC (owner or admin).
// Callers are responsible for ensuring `account` is a server-derived auth
// identity; the builder does not validate, since panicking on a server-side
// invariant would crash the process.
func RoomRename(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.room.rename", account, roomID, siteID)
}

// RoomRestricted is the synchronous server-to-server request/reply subject for
// the restricted (formerly visibility) RPC. Admin-only and not exposed to
// clients — admin tooling targets this directly; room-service does all the
// work in the request handler and replies with the result.
func RoomRestricted(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.restricted", siteID)
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

// RoomCanonicalTeamsCreate returns the room-canonical subject for a batch of
// Teams-derived room-creation events for one site. Lands in ROOMS_{siteID}.
func RoomCanonicalTeamsCreate(siteID string) string {
	return fmt.Sprintf("chat.room.canonical.%s.teams.create", siteID)
}

// RoomCanonicalMemberEvent returns the post-mutation member-event subject (mute-only today).
func RoomCanonicalMemberEvent(siteID, eventType string) string {
	return fmt.Sprintf("chat.room.canonical.%s.event.member.%s", siteID, eventType)
}

// Outbox returns the OUTBOX-stream subject a service publishes a federation relay
// event on: chat.outbox.{originSiteID}.{destSiteID}.{eventType}. outbox-worker
// consumes the OUTBOX stream and forwards each event's Envelope to the
// destination site's INBOX. Destination and event type ride the subject so a
// per-destination consumer can filter (or pause) on a single peer.
func Outbox(originSiteID, destSiteID, eventType string) string {
	return fmt.Sprintf("chat.outbox.%s.%s.%s", originSiteID, destSiteID, eventType)
}

// OutboxWildcard matches every event on a site's OUTBOX stream:
// chat.outbox.{originSiteID}.>. Use as the OUTBOX_{siteID} stream's subject
// pattern and for a consumer draining all destinations; a per-destination
// consumer filters chat.outbox.{originSiteID}.{destSiteID}.> instead.
func OutboxWildcard(originSiteID string) string {
	return fmt.Sprintf("chat.outbox.%s.>", originSiteID)
}

// ParseOutbox extracts (originSiteID, destSiteID, eventType) from an OUTBOX
// subject of the form chat.outbox.{origin}.{dest}.{eventType}. Returns ok=false
// on any malformed subject. Event types are single dot-free tokens (the
// pkg/outbox partition and the consumer FilterSubjects both treat them as one
// token), so the subject is always exactly five tokens.
func ParseOutbox(subj string) (originSiteID, destSiteID, eventType string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 5 || parts[0] != "chat" || parts[1] != "outbox" ||
		parts[2] == "" || parts[3] == "" || parts[4] == "" {
		return "", "", "", false
	}
	return parts[2], parts[3], parts[4], true
}

func SubscriptionUpdate(account string) string {
	return fmt.Sprintf("chat.user.%s.event.subscription.update", account)
}

// SettingsUpdate is the client-facing fanout subject published by
// user-service after a successful settings.set (same delivery pattern as
// subscription.update — ephemeral, core NATS).
func SettingsUpdate(account string) string {
	return fmt.Sprintf("chat.user.%s.event.settings.update", account)
}

func RoomMetadataChanged(account string) string {
	return fmt.Sprintf("chat.user.%s.event.room.metadata.update", account)
}

func Notification(account string) string {
	return fmt.Sprintf("chat.user.%s.notification", account)
}

// InboxExternal is the subject a service uses to publish a cross-site
// (remote-origin) federation event directly into the destination site's INBOX
// stream: `chat.inbox.{siteID}.external.{eventType}`. The JetStream publish is
// routed across the NATS supercluster to the destination's INBOX. inbox-worker
// consumes this lane and applies the event to the destination's DB.
func InboxExternal(siteID, eventType string) string {
	return fmt.Sprintf("chat.inbox.%s.external.%s", siteID, eventType)
}

// InboxInternal is the subject a same-site service uses to publish a
// local-origin event into its own INBOX stream:
// `chat.inbox.{siteID}.internal.{eventType}`. The internal lane is a
// search-indexing feed only — inbox-worker does NOT consume it, because the
// originating service already applied the change to the local DB synchronously.
func InboxInternal(siteID, eventType string) string {
	return fmt.Sprintf("chat.inbox.%s.internal.%s", siteID, eventType)
}

// InboxExternalAll returns the wildcard matching every external-lane event on a
// site's INBOX stream: `chat.inbox.{siteID}.external.>`. Use with
// jetstream.ConsumerConfig.FilterSubjects to scope a consumer to the remote-
// origin lane only — excluding internal-lane publishes reserved for
// search-sync-worker.
func InboxExternalAll(siteID string) string {
	return fmt.Sprintf("chat.inbox.%s.external.>", siteID)
}

// InboxMemberEventSubjects returns the subject filters a consumer should use to
// receive member_added/member_removed events on both the internal (same-site)
// and external (cross-site) lanes for the given site. Use with
// jetstream.ConsumerConfig.FilterSubjects (NATS 2.10+).
func InboxMemberEventSubjects(siteID string) []string {
	return []string{
		InboxInternal(siteID, "member_added"),
		InboxInternal(siteID, "member_removed"),
		InboxExternal(siteID, "member_added"),
		InboxExternal(siteID, "member_removed"),
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

// MigrationInternalMsgEdit is the server-only request subject for applying a migrated
// message edit. MUST be locked to server identities in NATS permissions (no client access).
func MigrationInternalMsgEdit(siteID string) string {
	return fmt.Sprintf("chat.migration.internal.%s.msg.edit", siteID)
}

// MigrationInternalMsgDelete is the server-only request subject for a migrated soft-delete.
func MigrationInternalMsgDelete(siteID string) string {
	return fmt.Sprintf("chat.migration.internal.%s.msg.delete", siteID)
}

func MsgCanonicalPinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.pinned", siteID)
}

func MsgCanonicalUnpinned(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.unpinned", siteID)
}

func MsgCanonicalReacted(siteID string) string {
	return fmt.Sprintf("chat.msg.canonical.%s.reacted", siteID)
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

// ThreadRoomInfoBatch is the server-to-server request subject for a batch
// lookup of thread rooms' lastMsgAt + parent room type; room-service also
// registers its handler on this subject. Mirrors RoomsInfoBatch.
func ThreadRoomInfoBatch(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.info.batch", siteID)
}

// RoomThreadReadAll is the internal server-to-server subject user-service uses to
// ask a site's room-service to clear all of an account's thread-unread state.
func RoomThreadReadAll(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.read.all", siteID)
}

// RoomThreadReadAllSubscribe is room-service's registration subject — the same
// concrete subject, mirroring the RoomsInfoBatch/RoomsInfoBatchSubscribe pair.
func RoomThreadReadAllSubscribe(siteID string) string {
	return fmt.Sprintf("chat.server.request.room.%s.thread.read.all", siteID)
}

// ThreadSubscriptionList is the server-to-server request subject for the per-site
// leaf of the cross-site thread inbox: the user-service aggregator fans out one
// request per candidate site to history-service, which subscribes on the same
// subject. Mirrors RoomsInfoBatch.
func ThreadSubscriptionList(siteID string) string {
	return fmt.Sprintf("chat.server.request.thread.%s.subscription.list", siteID)
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

// RoomRenameWildcard is the queue-subscribe pattern on a site.
func RoomRenameWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.room.rename", siteID)
}

func MemberRemoveWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.remove", siteID)
}

func MemberListWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.list", siteID)
}

// MemberStatuses is the concrete subject for the per-room member.statuses RPC.
// Pair with MemberStatusesWildcard for room-service's QueueSubscribe.
func MemberStatuses(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.member.statuses", account, roomID, siteID)
}

// MemberStatusesWildcard is the per-site subscription pattern for member.statuses.
func MemberStatusesWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.member.statuses", siteID)
}

// MentionableSubscriptions is the concrete subject for the per-room
// subscription.mentionable RPC. Pair with MentionableSubscriptionsWildcard
// for room-service's QueueSubscribe.
func MentionableSubscriptions(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.subscription.mentionable", account, roomID, siteID)
}

// MentionableSubscriptionsWildcard is the per-site subscription pattern for subscription.mentionable.
func MentionableSubscriptionsWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.subscription.mentionable", siteID)
}

// OrgMembers builds the subject for listing members of an org. siteID
// selects which site's user directory to query — each site has its own
// users collection, so org membership is per-site. Token order matches
// the room-scoped builders ("identifier → site → action"). Panics on
// any token containing NATS wildcard characters.
func OrgMembers(account, orgID, siteID string) string {
	if !isValidAccountToken(account) || !isValidAccountToken(orgID) || !isValidAccountToken(siteID) {
		panic("invalid subject token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.orgs.%s.%s.members", account, orgID, siteID)
}

// OrgMembersWildcard is the per-site subscription pattern for the
// list-org-members endpoint.
func OrgMembersWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.orgs.*.%s.members", siteID)
}

// ParseOrgMembersSubject returns (orgID, siteID) from a subject
// matching "chat.user.{account}.request.orgs.{orgId}.{siteId}.members".
// Tokens: [0]chat [1]user [2]{account} [3]request [4]orgs [5]{orgId}
// [6]{siteId} [7]members. Returns ok=false when any token contains
// NATS wildcard characters.
func ParseOrgMembersSubject(subj string) (orgID, siteID string, ok bool) {
	parts := strings.Split(subj, ".")
	if len(parts) != 8 {
		return "", "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "request" ||
		parts[4] != "orgs" || parts[7] != "members" {
		return "", "", false
	}
	if !isValidAccountToken(parts[2]) || !isValidAccountToken(parts[5]) || !isValidAccountToken(parts[6]) {
		return "", "", false
	}
	return parts[5], parts[6], true
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

// MsgGetIDsPattern is the natsrouter pattern for the GetMessagesByIDs batch handler.
// Pair with MsgGetIDs for the concrete-subject form callers publish on.
func MsgGetIDsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.get.ids", siteID)
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

// MsgReactPattern is the natsrouter pattern for toggling a reaction on a message.
// The {account} and {roomID} placeholders are extracted by natsrouter.
func MsgReactPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.react", siteID)
}

func MsgThreadPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.msg.thread", siteID)
}

// --- presence ---

// Presence write subjects carry the user's home siteID so the message routes
// to the presence service that owns that user's state, regardless of which
// site the client is connected to. {account} is the JWT-enforced self token;
// the service registers each pattern with its own literal siteID.

// PresenceHelloPattern is the natsrouter pattern for connection init.
func PresenceHelloPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.event.presence.%s.hello", siteID)
}

// PresencePingPattern is the natsrouter pattern for liveness pings (heartbeat).
func PresencePingPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.event.presence.%s.ping", siteID)
}

// PresenceActivityPattern is the natsrouter pattern for active/inactive updates.
func PresenceActivityPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.event.presence.%s.activity", siteID)
}

// PresenceByePattern is the natsrouter pattern for best-effort disconnects.
func PresenceByePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.event.presence.%s.bye", siteID)
}

// PresenceManualSetPattern is the natsrouter pattern for manual-override set/clear.
func PresenceManualSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.presence.%s.manual.set", siteID)
}

// PresenceQueryBatch is the concrete (per-site, literal) subject a client sends
// a batch initial-state query to. The client targets its OWN local site; that
// site resolves each account's home site and fans out to peers as needed.
func PresenceQueryBatch(siteID string) string {
	return fmt.Sprintf("chat.user.presence.%s.query.batch", siteID)
}

// PresenceQueryBatchPeer is the server-to-server request subject a presence
// service uses to fetch presence for accounts homed on a remote site (the
// fan-out leaf — local lookup only, no further fan-out). Mirrors RoomsInfoBatch.
func PresenceQueryBatchPeer(siteID string) string {
	return fmt.Sprintf("chat.server.request.presence.%s.query.batch", siteID)
}

// PresenceState is the live-state subject the owning site publishes a user's
// effective status to; clients subscribe to it (possibly cross-site). It omits
// siteID: the broadcast is a global per-user event, so a subscriber needs only
// the account and does not have to resolve the user's home site first.
// Callers pass a server-derived auth identity (the publishing handler's
// JWT-pinned account), so the builder does not validate — panicking on a
// server-side invariant would crash the process.
func PresenceState(account string) string {
	return fmt.Sprintf("chat.user.presence.state.%s", account)
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

// FavoriteToggle returns the concrete subject for the per-user favorite.toggle RPC.
func FavoriteToggle(account, roomID, siteID string) string {
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.favorite.toggle", account, roomID, siteID)
}

// FavoriteToggleWildcard is the per-site subscription pattern for the favorite.toggle RPC.
func FavoriteToggleWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.favorite.toggle", siteID)
}

// RoomAppTabs returns the concrete subject for the GetRoomAppTabs RPC.
// Pair with RoomAppTabsWildcard for room-service's QueueSubscribe.
func RoomAppTabs(account, roomID, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.tabs", account, roomID, siteID)
}

// RoomAppTabsWildcard is the per-site subscription pattern for the
// GetRoomAppTabs RPC.
func RoomAppTabsWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.app.tabs", siteID)
}

// RoomAppCmdMenu returns the concrete subject for the
// GetRoomAppCommandMenu RPC.
func RoomAppCmdMenu(account, roomID, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.app.cmd-menu", account, roomID, siteID)
}

// RoomAppCmdMenuWildcard is the per-site subscription pattern for the
// GetRoomAppCommandMenu RPC.
func RoomAppCmdMenuWildcard(siteID string) string {
	return fmt.Sprintf("chat.user.*.request.room.*.%s.app.cmd-menu", siteID)
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

// --- custom emoji (media-service) ---

// EmojiList builds the concrete subject for listing a site's custom emoji.
func EmojiList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.emoji.%s.list", account, siteID)
}

// EmojiListPattern is the natsrouter pattern for the emoji list RPC. siteID is
// baked in so each site's media-service only serves its own emoji set —
// clients target the room's origin site.
func EmojiListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.emoji.%s.list", siteID)
}

// EmojiDelete builds the concrete subject for deleting a custom emoji.
func EmojiDelete(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.emoji.%s.delete", account, siteID)
}

// EmojiDeletePattern is the natsrouter pattern for the emoji delete RPC.
func EmojiDeletePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.emoji.%s.delete", siteID)
}

// --- room-service natsrouter pattern builders (siteID baked in) ---

func RoomCreatePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.%s.create", siteID)
}

func MemberRoleUpdatePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.member.role-update", siteID)
}

func MemberRemovePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.member.remove", siteID)
}

func MemberAddPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.member.add", siteID)
}

func MemberListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.member.list", siteID)
}

func MemberStatusesPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.member.statuses", siteID)
}

func MentionableSubscriptionsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.subscription.mentionable", siteID)
}

func OrgMembersPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.orgs.{orgID}.%s.members", siteID)
}

func MessageReadPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.message.read", siteID)
}

func MessageReadReceiptPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.message.read-receipt", siteID)
}

func MessageThreadReadPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.message.thread.read", siteID)
}

func RoomKeyGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.key.get", siteID)
}

func MuteTogglePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.mute.toggle", siteID)
}

func FavoriteTogglePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.favorite.toggle", siteID)
}

func RoomRenamePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.room.rename", siteID)
}

func RoomAppTabsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.app.tabs", siteID)
}

func RoomAppCmdMenuPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.app.cmd-menu", siteID)
}

// --- Microsoft Teams integration ---
//
// TeamsRoomCall + TeamsMeeting are room-scoped (the roomID rides the subject so
// membership can be checked), matching every other room RPC. TeamsUserCall is a
// 1:1 deep-link builder with no room; the target account travels in the body.

// TeamsRoomCall returns the concrete subject for the room-call deep-link RPC.
// Returns an error if account contains a NATS wildcard. Shared pkg/ code must
// not panic on bad input (F12), so this returns the error rather than panicking
// like the older sibling builders (e.g. RoomAppTabs, MsgHistory).
func TeamsRoomCall(account, roomID, siteID string) (string, error) {
	if !isValidAccountToken(account) {
		return "", fmt.Errorf("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.teams.call", account, roomID, siteID), nil
}

// TeamsRoomCallPattern is the natsrouter registration pattern for the room-call RPC.
func TeamsRoomCallPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.teams.call", siteID)
}

// TeamsMeeting returns the concrete subject for the Graph onlineMeeting RPC.
// Returns an error if account contains a NATS wildcard. Shared pkg/ code must
// not panic on bad input (F12).
func TeamsMeeting(account, roomID, siteID string) (string, error) {
	if !isValidAccountToken(account) {
		return "", fmt.Errorf("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.room.%s.%s.teams.meeting", account, roomID, siteID), nil
}

// TeamsMeetingPattern is the natsrouter registration pattern for the meetings RPC.
func TeamsMeetingPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.room.{roomID}.%s.teams.meeting", siteID)
}

// TeamsUserCall returns the concrete subject for the 1:1 user-call deep-link RPC.
// Returns an error if account contains a NATS wildcard. Shared pkg/ code must
// not panic on bad input (F12).
func TeamsUserCall(account, siteID string) (string, error) {
	if !isValidAccountToken(account) {
		return "", fmt.Errorf("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.teams.%s.call.user", account, siteID), nil
}

// TeamsUserCallPattern is the natsrouter registration pattern for the user-call RPC.
func TeamsUserCallPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.teams.%s.call.user", siteID)
}

// isValidAccountToken is the parsers' boundary guard for the account token so
// wildcard semantics never leak into identity parsing.
func isValidAccountToken(token string) bool {
	return IsValidAccountToken(token)
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

// --- user-service builders ---

func UserStatusGetByName(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.getByName", account, siteID)
}

func UserProfileGetByName(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.profile.getByName", account, siteID)
}

func UserStatusSet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.status.set", account, siteID)
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

func UserSubscriptionCount(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.count", account, siteID)
}

func UserAppsList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.apps.list", account, siteID)
}

// UserAppsCategories keeps the legacy panic-on-wildcard style of its User*
// siblings (not the F12 error-return style) for family consistency.
func UserAppsCategories(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.apps.categories", account, siteID)
}

// --- natsrouter pattern builders (siteID baked in, account left as {account} placeholder) ---

func UserStatusGetByNamePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.status.getByName", siteID)
}

func UserProfileGetByNamePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.profile.getByName", siteID)
}

func UserStatusSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.status.set", siteID)
}

func UserSubscriptionGetChannelsPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getChannels", siteID)
}

func UserSubscriptionGetDMPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getDM", siteID)
}

func UserSubscriptionCountPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.count", siteID)
}

func UserAppsListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.apps.list", siteID)
}

func UserAppsCategoriesPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.apps.categories", siteID)
}

// UserMe is the concrete subject for the /me self-info endpoint — a deliberate
// single-token action ("me"), not the {area}.{action} shape of its siblings.
func UserMe(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.me", account, siteID)
}

// UserMePattern is the natsrouter pattern for the /me endpoint.
func UserMePattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.me", siteID)
}

func UserSubscriptionList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.list", account, siteID)
}

func UserSubscriptionListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.list", siteID)
}

func UserSettingsGet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.settings.get", account, siteID)
}

func UserSettingsGetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.get", siteID)
}

func UserSettingsSet(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.settings.set", account, siteID)
}

func UserSettingsSetPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.settings.set", siteID)
}

// UserThreadList is the concrete client-facing subject for the cross-site thread
// inbox RPC. siteID is the CALLER's own home site — the site that holds the
// user's federated subscriptions and runs the aggregator. Pair with
// UserThreadListPattern for user-service's registration.
func UserThreadList(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.thread.list", account, siteID)
}

// UserThreadListPattern is the natsrouter pattern user-service registers for the
// thread inbox RPC (siteID baked in, account left as {account}).
func UserThreadListPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.list", siteID)
}

// UserThreadUnreadSummary is the client-facing subject for the cross-site thread
// unread badge. siteID is the CALLER's own home site. Pair with
// UserThreadUnreadSummaryPattern for user-service's registration.
func UserThreadUnreadSummary(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.thread.unread.summary", account, siteID)
}

// UserThreadUnreadSummaryPattern is the natsrouter pattern user-service registers.
func UserThreadUnreadSummaryPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.unread.summary", siteID)
}

// UserThreadReadAll is the client-facing subject for the cross-site
// clear-all-thread-unread RPC. siteID is the CALLER's own home site — the site
// holding the user's federated thread-subscription replicas and running the
// aggregator. Pair with UserThreadReadAllPattern for user-service registration.
func UserThreadReadAll(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.thread.read.all", account, siteID)
}

// UserThreadReadAllPattern is the natsrouter pattern user-service registers for
// the clear-all-thread-unread RPC (siteID baked in, account left as {account}).
func UserThreadReadAllPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.thread.read.all", siteID)
}

func UserSubscriptionSetAppSubscription(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.setAppSubscription", account, siteID)
}

func UserSubscriptionSetAppSubscriptionPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.setAppSubscription", siteID)
}

func UserSubscriptionGetByRoomID(account, siteID string) string {
	if !isValidAccountToken(account) {
		panic("invalid account token: contains NATS wildcard characters")
	}
	return fmt.Sprintf("chat.user.%s.request.user.%s.subscription.getByRoomID", account, siteID)
}

func UserSubscriptionGetByRoomIDPattern(siteID string) string {
	return fmt.Sprintf("chat.user.{account}.request.user.%s.subscription.getByRoomID", siteID)
}

// ParseUserSubject parses any 8-token subject of the form
//
//	chat.user.{account}.request.user.{siteID}.{area}.{action}
//
// where area is one of "status", "subscription", "profile", "apps", "settings".
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
	case "status", "subscription", "profile", "apps", "settings":
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

// PushNotification is the per-recipient mobile-push subject. Lives under chat.server.* so
// client JWTs cannot subscribe. The stream filter covers the .send leaf and future siblings.
func PushNotification(siteID string) string {
	return fmt.Sprintf("chat.server.notification.push.%s.send", siteID)
}

// PushNotificationFilter is the stream-binding wildcard covering .send and any future siblings.
func PushNotificationFilter(siteID string) string {
	return fmt.Sprintf("chat.server.notification.push.%s.>", siteID)
}

// ServerBroadcastThreadTCount is the core-NATS subject on which message-worker
// publishes thread reply-count badge events. Broadcast-worker queue-subscribes
// using the wildcard ServerBroadcastWildcard so this stays fire-and-forget
// without polluting MESSAGES_CANONICAL (which is reserved for message CRUD).
func ServerBroadcastThreadTCount(siteID string) string {
	return fmt.Sprintf("chat.server.broadcast.%s.thread.tcount", siteID)
}

// ServerBroadcastWildcard is the queue-subscribe subject used by broadcast-worker
// to receive all server-broadcast events for a site.
func ServerBroadcastWildcard(siteID string) string {
	return fmt.Sprintf("chat.server.broadcast.%s.>", siteID)
}

// PresenceSnapshot is the bulk presence RPC subject (request/reply).
func PresenceSnapshot(siteID string) string {
	return fmt.Sprintf("chat.presence.%s.request.snapshot", siteID)
}

// SubscriptionUpdateWildcard matches every subscription.update fanout event.
func SubscriptionUpdateWildcard() string {
	return "chat.user.*.event.subscription.update"
}

// ParseSubscriptionUpdateAccount extracts the account from a subscription.update subject; ok=false on malformed input.
func ParseSubscriptionUpdateAccount(s string) (account string, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return "", false
	}
	if parts[0] != "chat" || parts[1] != "user" || parts[3] != "event" ||
		parts[4] != "subscription" || parts[5] != "update" {
		return "", false
	}
	if !isValidAccountToken(parts[2]) {
		return "", false
	}
	return parts[2], true
}

// MigrationOplog builds the subject for one raw CDC event: chat.migration.oplog.{siteID}.{collection}.{op}. collection is the raw source name (e.g. rocketchat_message), op is insert|update|replace|delete.
func MigrationOplog(siteID, collection, op string) string {
	return fmt.Sprintf("chat.migration.oplog.%s.%s.%s", siteID, collection, op)
}

// MigrationOplogWildcard matches every oplog event for a site — the MIGRATION_OPLOG_{siteID} stream's subjects.
func MigrationOplogWildcard(siteID string) string {
	return fmt.Sprintf("chat.migration.oplog.%s.>", siteID)
}

// OrgSyncEmployeesUpsert is the subject search-sync-worker's spotlight-org
// collection consumes from; hr-syncer publishes on the same subject at the
// central site.
func OrgSyncEmployeesUpsert(centralSiteID string) string {
	return fmt.Sprintf("chat.hr.%s.employees.upsert", centralSiteID)
}
