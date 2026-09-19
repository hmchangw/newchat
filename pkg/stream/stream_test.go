package stream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

// TestStreamConfigs covers single-subject streams; multi-subject streams get dedicated tests.
func TestStreamConfigs(t *testing.T) {
	siteID := "site-a"

	tests := []struct {
		name     string
		cfg      stream.Config
		wantName string
		wantSubj string
	}{
		{"Messages", stream.Messages(siteID), "MESSAGES-site-a", "chat.user.*.room.*.site-a.msg.>"},
		{"MessagesCanonical", stream.MessagesCanonical(siteID), "MESSAGES-CANONICAL-site-a", "chat.msg.canonical.site-a.>"},
		{"Rooms", stream.Rooms(siteID), "ROOMS-site-a", "chat.room.canonical.site-a.>"},
		{"RoomsTeams", stream.RoomsTeams(siteID), "ROOMS-TEAMS-site-a", "chat.teams.room.canonical.site-a.>"},
		{"Outbox", stream.Outbox(siteID), "OUTBOX-site-a", "chat.outbox.site-a.>"},
		{"PushNotification", stream.PushNotification(siteID), "PUSH-NOTIFICATION-site-a", "chat.server.notification.push.site-a.>"},
		{"OrgSyncStream", stream.OrgSyncStream(siteID), "HR-site-a", "chat.hr.site-a.>"},
		{"BotMessagesCanonical", stream.BotMessagesCanonical(siteID), "BOT-MESSAGES-CANONICAL-site-a", "chat.bot.canonical.site-a.>"},
		{"BotPushNotification", stream.BotPushNotification(siteID), "BOT-PUSH-NOTIFICATION-site-a", "chat.bot.notification.push.site-a.>"},
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

	assert.Equal(t, "INBOX-site-a", cfg.Name)
	// Two non-overlapping patterns: internal (same-site feed) and external (cross-site).
	assert.Equal(t, []string{
		"chat.inbox.site-a.internal.>",
		"chat.inbox.site-a.external.>",
	}, cfg.Subjects)
}

func TestInboxFailover(t *testing.T) {
	c := stream.InboxFailover("site-a")
	assert.Equal(t, "INBOX-FAILOVER-site-a", c.Name)
	assert.Equal(t, []string{"chat.failover.inbox.site-a.external.>"}, c.Subjects)
}

// The stream is named for the ORIGIN site, never the hosting buddy — names are
// unique supercluster-wide, and naming by host would collide if a cluster ever
// buddied for more than one peer.
func TestInboxFailover_NamedForOriginSite(t *testing.T) {
	assert.NotEqual(t, stream.InboxFailover("site-a").Name, stream.InboxFailover("site-b").Name)
	assert.Contains(t, stream.InboxFailover("site-a").Name, "site-a")
}

func TestMessagePathFailoverStreams(t *testing.T) {
	tests := []struct {
		name     string
		got      stream.Config
		wantName string
		wantSubj string
	}{
		{"messages", stream.MessagesFailover("site-a"), "MESSAGES-FAILOVER-site-a",
			"chat.user.*.room.*.site-a.failover.msg.>"},
		{"canonical", stream.MessagesCanonicalFailover("site-a"), "MESSAGES-CANONICAL-FAILOVER-site-a",
			"chat.failover.msg.canonical.site-a.>"},
		{"push", stream.PushNotificationFailover("site-a"), "PUSH-NOTIFICATION-FAILOVER-site-a",
			"chat.failover.push.site-a.>"},
		{"outbox", stream.OutboxFailover("site-a"), "OUTBOX-FAILOVER-site-a",
			"chat.failover.outbox.site-a.>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantName, tt.got.Name)
			assert.Equal(t, []string{tt.wantSubj}, tt.got.Subjects)
		})
	}
}
