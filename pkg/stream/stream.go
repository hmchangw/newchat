package stream

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/subject"
)

// Config holds the JetStream stream configuration parameters.
type Config struct {
	Name     string
	Subjects []string
}

func Messages(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES_%s", siteID),
		Subjects: []string{fmt.Sprintf("chat.user.*.room.*.%s.msg.>", siteID)},
	}
}

func MessagesCanonical(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES_CANONICAL_%s", siteID),
		Subjects: []string{fmt.Sprintf("chat.msg.canonical.%s.>", siteID)},
	}
}

func Rooms(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("ROOMS_%s", siteID),
		Subjects: []string{subject.RoomCanonicalWildcard(siteID)},
	}
}

// PushNotification returns the PUSH_NOTIFICATION_{siteID} stream config; ops-owned in prod.
func PushNotification(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("PUSH_NOTIFICATION_%s", siteID),
		Subjects: []string{subject.PushNotificationFilter(siteID)},
	}
}

// Inbox returns the INBOX_{siteID} stream, with two non-overlapping lanes (internal same-site
// search feed vs external cross-site) — no sourcing/SubjectTransform; remote sites publish the external lane directly.
func Inbox(siteID string) Config {
	return Config{
		Name: fmt.Sprintf("INBOX_%s", siteID),
		Subjects: []string{
			fmt.Sprintf("chat.inbox.%s.internal.>", siteID),
			fmt.Sprintf("chat.inbox.%s.external.>", siteID),
		},
	}
}

// Outbox returns OUTBOX_{siteID}: durable federation-relay lane; outbox-worker owns bootstrap.
func Outbox(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX_%s", siteID),
		Subjects: []string{subject.OutboxWildcard(siteID)},
	}
}

// MigrationOplog returns MIGRATION_OPLOG_{siteID}: raw CDC events from legacy source Mongo.
func MigrationOplog(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MIGRATION_OPLOG_%s", siteID),
		Subjects: []string{subject.MigrationOplogWildcard(siteID)},
	}
}

// BotMessagesCanonical returns BOT_MESSAGES_CANONICAL_{siteID}, published by bot-msg-handler.
// Consumed by bot-msg-worker, bot-broadcast-worker, bot-notification-worker, search-sync-worker.
func BotMessagesCanonical(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("BOT_MESSAGES_CANONICAL_%s", siteID),
		Subjects: []string{subject.BotCanonicalWildcard(siteID)},
	}
}

// BotPushNotif returns BOT_PUSH_NOTIF_{siteID}, isolated from user PUSH_NOTIFICATION so a bot-notification incident cannot touch user push delivery.
func BotPushNotif(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("BOT_PUSH_NOTIF_%s", siteID),
		Subjects: []string{subject.BotPushNotificationWildcard(siteID)},
	}
}

// OrgSyncStream is HR_{centralSiteID}, populated daily by hr-syncer at the central site.
func OrgSyncStream(centralSiteID string) Config {
	return Config{
		Name:     fmt.Sprintf("HR_%s", centralSiteID),
		Subjects: []string{fmt.Sprintf("chat.hr.%s.>", centralSiteID)},
	}
}
