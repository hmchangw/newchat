package subject_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestThreadListSubjectBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"UserThreadList", subject.UserThreadList("alice", "site-a"),
			"chat.user.alice.request.user.site-a.thread.list"},
		{"UserThreadListPattern", subject.UserThreadListPattern("site-a"),
			"chat.user.{account}.request.user.site-a.thread.list"},
		{"ThreadSubscriptionList", subject.ThreadSubscriptionList("site-a"),
			"chat.server.request.thread.site-a.subscription.list"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// UserThreadList mirrors the other user-scoped builders: it panics on an account
// token carrying NATS wildcard characters rather than emit a malformed subject.
func TestUserThreadList_PanicsOnWildcardAccount(t *testing.T) {
	for _, bad := range []string{"a.b", "a*", "a>", "a b"} {
		t.Run(bad, func(t *testing.T) {
			assert.Panics(t, func() { subject.UserThreadList(bad, "site-a") })
		})
	}
}
