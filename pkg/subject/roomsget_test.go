package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestRoomsGetSubjectBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"RoomsGet", subject.RoomsGet("alice", "site-a"),
			"chat.user.alice.request.history.site-a.rooms.get"},
		{"RoomsGetPattern", subject.RoomsGetPattern("site-a"),
			"chat.user.{account}.request.history.site-a.rooms.get"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestRoomsGet_PanicsOnWildcardAccount(t *testing.T) {
	for _, bad := range []string{"a.b", "a*", "a>", "a b"} {
		t.Run(bad, func(t *testing.T) {
			assert.Panics(t, func() { subject.RoomsGet(bad, "site-a") })
		})
	}
}
