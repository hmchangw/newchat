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

func TestPublishLabelsFromSubject(t *testing.T) {
	tests := []struct {
		name        string
		subj        string
		destination DestinationKind
		operation   PublishOperation
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
