package subject

import "fmt"

// Bot messaging pipeline subjects.
// Req/reply lives under chat.server.bot.request.>; JetStream lives under chat.bot.>.

// ServerBotMsgRoomSend is BP's publish subject for a bot send-in-room request.
func ServerBotMsgRoomSend(siteID, roomID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.%s.msg.send", siteID, roomID)
}

// ServerBotDMSend is BP's publish subject for a bot send-DM request.
// bot-msg-handler derives the roomID from userID via idgen.BuildDMRoomID.
func ServerBotDMSend(siteID, userID string) string {
	return fmt.Sprintf("chat.server.bot.request.dm.%s.%s.msg.send", siteID, userID)
}

// ServerBotRoomCreate is BP's publish subject for a bot channel-room create.
func ServerBotRoomCreate(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.create", siteID)
}

// ServerBotRoomMemberAdd is BP's publish subject for a batch add on a bot-owned room.
func ServerBotRoomMemberAdd(siteID, roomID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.%s.member.add", siteID, roomID)
}

// ServerBotRoomMemberRemove is BP's publish subject for a batch remove on a bot-owned room.
func ServerBotRoomMemberRemove(siteID, roomID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.%s.member.remove", siteID, roomID)
}

// ServerBotRoomGet is the cross-site room-info fetch subject, scoped to the room's origin site.
func ServerBotRoomGet(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.get", siteID)
}

// ServerBotRoomDMEnsure is the subject BP's natsDMEnsurer publishes on to
// materialize a DM room via bot-room-service. Always scoped to the bot's
// site (DM origin lives at the bot's site).
func ServerBotRoomDMEnsure(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.dm.ensure", siteID)
}

// ServerBotMsgRoomSendPattern is the natsrouter pattern paired with ServerBotMsgRoomSend.
func ServerBotMsgRoomSendPattern(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.{roomID}.msg.send", siteID)
}

// ServerBotDMSendPattern is the natsrouter pattern paired with ServerBotDMSend.
func ServerBotDMSendPattern(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.dm.%s.{userID}.msg.send", siteID)
}

// ServerBotRoomMemberAddPattern / RemovePattern are the natsrouter patterns for add/remove.
func ServerBotRoomMemberAddPattern(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.{roomID}.member.add", siteID)
}

func ServerBotRoomMemberRemovePattern(siteID string) string {
	return fmt.Sprintf("chat.server.bot.request.room.%s.{roomID}.member.remove", siteID)
}

// ServerBotWildcard matches every bot req/reply subject; use for account permissions.
func ServerBotWildcard() string {
	return "chat.server.bot.request.>"
}

// BotCanonicalCreated is the subject bot-msg-handler publishes canonical bot messages on.
func BotCanonicalCreated(siteID string) string {
	return fmt.Sprintf("chat.bot.canonical.%s.created", siteID)
}

// BotCanonicalWildcard matches every subject on BOT_MESSAGES_CANONICAL; used as stream pattern.
func BotCanonicalWildcard(siteID string) string {
	return fmt.Sprintf("chat.bot.canonical.%s.>", siteID)
}

// BotPushNotification is bot-notification-worker's publish subject; kind is the push category.
func BotPushNotification(siteID, kind string) string {
	return fmt.Sprintf("chat.bot.notification.push.%s.%s", siteID, kind)
}

// BotPushNotificationWildcard matches every subject on BOT_PUSH_NOTIF.
func BotPushNotificationWildcard(siteID string) string {
	return fmt.Sprintf("chat.bot.notification.push.%s.>", siteID)
}
