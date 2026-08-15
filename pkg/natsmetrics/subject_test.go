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
