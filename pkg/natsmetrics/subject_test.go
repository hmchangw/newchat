package natsmetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

// Every consumer of a stream must derive event_type the same way, otherwise the
// campaign cannot join accepted -> persisted -> delivered on the label.
func TestEventTypeFromSubject(t *testing.T) {
	tests := []struct {
		name string
		subj string
		want EventType
	}{
		{name: "canonical created", subj: subject.MsgCanonicalCreated("s1"), want: EventCreated},
		{name: "canonical updated", subj: subject.MsgCanonicalUpdated("s1"), want: EventUpdated},
		{name: "canonical deleted", subj: subject.MsgCanonicalDeleted("s1"), want: EventDeleted},
		{name: "canonical pinned", subj: subject.MsgCanonicalPinned("s1"), want: EventPinned},
		{name: "canonical unpinned", subj: subject.MsgCanonicalUnpinned("s1"), want: EventUnpinned},
		{name: "canonical reacted", subj: subject.MsgCanonicalReacted("s1"), want: EventReacted},
		{name: "teams batch", subj: subject.MsgTeamsCanonicalBatch("s1"), want: EventTeamsBatch},
		{name: "user send", subj: subject.MsgSend("acct", "room", "s1"), want: EventSend},
		{name: "unrecognized tail", subj: "chat.msg.canonical.s1.somethingelse", want: EventUnknown},
		{name: "no dot", subj: "created", want: EventCreated},
		{name: "empty", subj: "", want: EventUnknown},
		{name: "trailing dot", subj: "chat.msg.canonical.s1.", want: EventUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EventTypeFromSubject(tt.subj))
		})
	}
}

func TestRoomEventTypeFromSubject(t *testing.T) {
	tests := []struct {
		name string
		subj string
		want EventType
	}{
		{name: "room create", subj: subject.RoomCanonical("site-a", "create"), want: EventRoomCreate},
		{name: "teams room create", subj: subject.RoomTeamsCanonicalCreate("site-a"), want: EventRoomCreate},
		{name: "member add", subj: subject.RoomCanonical("site-a", "member.add"), want: EventMemberAdd},
		{name: "member remove", subj: subject.RoomCanonical("site-a", "member.remove"), want: EventMemberRemove},
		{name: "room rename", subj: subject.RoomCanonical("site-a", "room.rename"), want: EventRoomRename},
		{name: "member mute invalidation", subj: subject.RoomCanonicalMemberEvent("site-a", "muted"), want: EventMemberMuted},
		{name: "unknown room operation", subj: subject.RoomCanonical("site-a", "unexpected"), want: EventUnknown},
		{name: "message subject is not a room operation", subj: subject.MsgCanonicalCreated("site-a"), want: EventUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RoomEventTypeFromSubject(tt.subj))
		})
	}
}

func TestRequestOperationFromSubject(t *testing.T) {
	teamsMeeting, err := subject.TeamsMeeting("alice", "room-a", "site-a")
	assert.NoError(t, err)
	tests := []struct {
		name string
		subj string
		want Operation
	}{
		// SLO-4 and SLO-5 each need their own rpc.method. They have different
		// bounds because their cost models differ — channel load walks
		// messages_by_room buckets, thread open slices one partition — so a
		// shared label mixes a slow family into a fast one's ratio and a fast
		// family into a slow one's, in opposite directions.
		{name: "channel history is its own method (SLO-4)", subj: subject.MsgHistory("alice", "room-a", "site-a"), want: OperationChannelHistory},
		{name: "thread open is its own method (SLO-5)", subj: "chat.user.alice.request.room.room-a.site-a.msg.thread", want: OperationThreadOpen},
		// Everything else stays history_read. Splitting further would put routes
		// into an SLO denominator that the SLO does not describe: thread.parent is
		// a second handler, not part of the verified "Enter thread" path, and
		// next/surrounding are scroll and jump, not an initial load.
		{name: "thread parent read stays history read", subj: "chat.user.alice.request.room.room-a.site-a.msg.thread.parent", want: OperationHistoryRead},
		{name: "scroll page stays history read", subj: "chat.user.alice.request.room.room-a.site-a.msg.next", want: OperationHistoryRead},
		{name: "jump to message stays history read", subj: "chat.user.alice.request.room.room-a.site-a.msg.surrounding", want: OperationHistoryRead},
		{name: "single message read stays history read", subj: subject.MsgGet("alice", "room-a", "site-a"), want: OperationHistoryRead},
		{name: "batch message read stays history read", subj: subject.MsgGetIDs("alice", "room-a", "site-a"), want: OperationHistoryRead},
		{name: "pinned list stays history read", subj: "chat.user.alice.request.room.room-a.site-a.msg.pinned.list", want: OperationHistoryRead},
		{name: "history mutation", subj: subject.MsgEdit("alice", "room-a", "site-a"), want: OperationHistoryMutation},
		{name: "history migration mutation", subj: subject.MigrationInternalMsgEdit("site-a"), want: OperationHistoryMutation},
		{name: "thread subscription read", subj: subject.ThreadSubscriptionList("site-a"), want: OperationHistoryRead},
		{name: "room read", subj: subject.RoomKeyGet("alice", "room-a", "site-a"), want: OperationRoomRead},
		{name: "room mutation", subj: subject.RoomRename("alice", "room-a", "site-a"), want: OperationRoomMutation},
		{name: "member read", subj: subject.MemberList("alice", "room-a", "site-a"), want: OperationMemberRead},
		{name: "organization member read", subj: subject.OrgMembers("alice", "org-a", "site-a"), want: OperationMemberRead},
		{name: "member mutation", subj: subject.MemberAdd("alice", "room-a", "site-a"), want: OperationMemberMutation},
		{name: "teams account does not hijack member mutation", subj: subject.MemberAdd("teams", "room-a", "site-a"), want: OperationMemberMutation},
		{name: "msg room does not hijack room read", subj: subject.RoomKeyGet("alice", "msg", "site-a"), want: OperationRoomRead},
		{name: "teams room", subj: teamsMeeting, want: OperationTeamsRoom},
		{name: "server room read", subj: subject.RoomsInfoBatchSubscribe("site-a"), want: OperationRoomRead},
		{name: "server room mutation", subj: subject.RoomKeyEnsure("site-a"), want: OperationRoomMutation},
		{name: "dynamic unknown", subj: "chat.user.alice.request.room.room-a.site-a.unbounded.value", want: OperationUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RequestOperationFromSubject(tt.subj))
		})
	}
}

func TestPublishLabelsFromSubject(t *testing.T) {
	tests := []struct {
		name        string
		subj        string
		destination DestinationKind
		operation   Operation
	}{
		{name: "canonical message mutation", subj: subject.MsgCanonicalUpdated("site-a"), destination: DestinationCanonical, operation: OperationCanonicalPublish},
		{name: "room create with member site", subj: subject.RoomCanonical("member", "create"), destination: DestinationRoomCanonical, operation: OperationRoomPublish},
		{name: "room create with member prefix site", subj: subject.RoomCanonical("member_foo", "create"), destination: DestinationRoomCanonical, operation: OperationRoomPublish},
		{name: "canonical member event", subj: subject.RoomCanonical("site-a", "member.add"), destination: DestinationRoomCanonical, operation: OperationMemberPublish},
		{name: "outbox room event", subj: subject.Outbox("site-a", "site-b", "room_renamed"), destination: DestinationOutbox, operation: OperationOutboxPublish},
		{name: "internal member feed", subj: subject.InboxInternal("site-a", "member_added"), destination: DestinationInbox, operation: OperationMemberPublish},
		{name: "internal room feed", subj: subject.InboxInternal("site-a", "room_renamed"), destination: DestinationInbox, operation: OperationRoomPublish},
		{name: "internal room feed with member prefix site", subj: subject.InboxInternal("member_foo", "room_renamed"), destination: DestinationInbox, operation: OperationRoomPublish},
		{name: "room event", subj: subject.RoomEvent("room-a", true), destination: DestinationRoomEvent, operation: OperationRoomPublish},
		{name: "member event", subj: subject.RoomMemberEvent("room-a", false), destination: DestinationMemberEvent, operation: OperationMemberPublish},
		{name: "client room update", subj: subject.UserRoomUpdate("alice"), destination: DestinationRecipientEvent, operation: OperationRoomPublish},
		{name: "client subscription update", subj: subject.SubscriptionUpdate("alice"), destination: DestinationRecipientEvent, operation: OperationMemberPublish},
		{name: "client room key", subj: subject.RoomKeyUpdate("alice"), destination: DestinationRecipientEvent, operation: OperationRoomPublish},
		{name: "async client response", subj: subject.UserResponse("alice", "request-a"), destination: DestinationClientResponse, operation: OperationClientResponse},
		{name: "teams identity upsert", subj: subject.OrgSyncUsersUpsert("site-a"), destination: DestinationUserSync, operation: OperationTeamsUserUpsert},
		{name: "unknown", subj: "chat.dynamic.account.room.site.error-text", destination: DestinationUnknown, operation: OperationUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			destination, operation := PublishLabelsFromSubject(tt.subj)
			assert.Equal(t, tt.destination, destination)
			assert.Equal(t, tt.operation, operation)
		})
	}
}

// Suffix matching must not classify subjects that are not requests at all.
// Every label in this family is derived from fixed token positions precisely so
// a subject from another family cannot mint one.
func TestRequestOperationFromSubject_NonRequestSubjectsAreUnknown(t *testing.T) {
	for _, subj := range []string{
		"chat.dynamic.member.list",
		"chat.room.canonical.site-a.member.add",
		"chat.inbox.site-a.external.create",
		"chat.server.broadcast.site-a.message.read",
	} {
		t.Run(subj, func(t *testing.T) {
			assert.Equal(t, OperationUnknown, RequestOperationFromSubject(subj))
		})
	}
}

// Suffix rules must not cross service families. Every subject below belongs to
// a service other than room-service, and each one ends in a tail that
// room-service also uses. The first is not hypothetical: user-service's
// chatlist.section.create was classified as room_mutation, so its traffic
// landed in room-service's rpc.method series.
//
// Unknown is the right answer for these until the operation vocabulary is
// extended deliberately — a wrong bounded label is worse than an honest
// unknown, because a wrong one is silently aggregated with real traffic.
func TestRequestOperationFromSubject_DoesNotCrossServiceFamilies(t *testing.T) {
	for _, subj := range []string{
		subject.UserChatlistSectionCreate("alice", "site-a"),
		"chat.user.alice.request.user.site-a.chatlist.section.rename",
		"chat.user.alice.request.user.site-a.member.list",
		"chat.user.alice.request.user.site-a.key.get",
		"chat.user.alice.request.search.site-a.messages",
		"chat.user.alice.request.media.site-a.open",
	} {
		t.Run(subj, func(t *testing.T) {
			assert.Equal(t, OperationUnknown, RequestOperationFromSubject(subj))
		})
	}
}
