package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// TestStreamConfigs covers single-subject streams where one pattern is the
// full story. Multi-subject streams (currently just Inbox) get their own
// dedicated test so both patterns are asserted explicitly.
func TestStreamConfigs(t *testing.T) {
	siteID := "site-a"

	tests := []struct {
		name     string
		cfg      stream.Config
		wantName string
		wantSubj string
	}{
		{"Messages", stream.Messages(siteID), "MESSAGES_site-a", "chat.user.*.room.*.site-a.msg.>"},
		{"MessagesCanonical", stream.MessagesCanonical(siteID), "MESSAGES_CANONICAL_site-a", "chat.msg.canonical.site-a.>"},
		{"Rooms", stream.Rooms(siteID), "ROOMS_site-a", "chat.room.canonical.site-a.>"},
		{"Outbox", stream.Outbox(siteID), "OUTBOX_site-a", "outbox.site-a.>"},
		{"PushNotifications", stream.PushNotifications(siteID), "PUSH_NOTIFICATIONS_site-a", "chat.server.notification.push.site-a.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantName, tt.cfg.Name)
			require.Len(t, tt.cfg.Subjects, 1)
			assert.Equal(t, tt.wantSubj, tt.cfg.Subjects[0])
		})
	}
}

func TestInboxConfig(t *testing.T) {
	cfg := stream.Inbox("site-a")

	assert.Equal(t, "INBOX_site-a", cfg.Name)
	// Two non-overlapping patterns: local (`*`) and federated (`aggregate.>`).
	assert.Equal(t, []string{
		"chat.inbox.site-a.*",
		"chat.inbox.site-a.aggregate.>",
	}, cfg.Subjects)
}
