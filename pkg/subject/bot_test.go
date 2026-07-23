package subject

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBotSubjectBuilders covers every bot req/reply + stream subject builder.
func TestBotSubjectBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ServerBotMsgRoomSend", ServerBotMsgRoomSend("site-a", "r1"),
			"chat.server.bot.request.room.site-a.r1.msg.send"},
		{"ServerBotDMSend", ServerBotDMSend("site-a", "u1"),
			"chat.server.bot.request.dm.site-a.u1.msg.send"},
		{"ServerBotRoomCreate", ServerBotRoomCreate("site-a"),
			"chat.server.bot.request.room.site-a.create"},
		{"ServerBotRoomMemberAdd", ServerBotRoomMemberAdd("site-a", "r1"),
			"chat.server.bot.request.room.site-a.r1.member.add"},
		{"ServerBotRoomMemberRemove", ServerBotRoomMemberRemove("site-a", "r1"),
			"chat.server.bot.request.room.site-a.r1.member.remove"},

		{"ServerBotRoomGet", ServerBotRoomGet("site-a"),
			"chat.server.bot.request.room.site-a.get"},
		{"ServerBotRoomDMEnsure", ServerBotRoomDMEnsure("site-a"),
			"chat.server.bot.request.room.site-a.dm.ensure"},

		{"ServerBotWildcard", ServerBotWildcard(),
			"chat.server.bot.request.>"},

		{"BotCanonicalCreated", BotCanonicalCreated("site-a"),
			"chat.bot.canonical.site-a.created"},
		{"BotCanonicalWildcard", BotCanonicalWildcard("site-a"),
			"chat.bot.canonical.site-a.>"},
		{"BotPushNotification", BotPushNotification("site-a", "message"),
			"chat.bot.notification.push.site-a.message"},
		{"BotPushNotificationWildcard", BotPushNotificationWildcard("site-a"),
			"chat.bot.notification.push.site-a.>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
