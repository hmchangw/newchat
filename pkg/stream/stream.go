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
		Name:     fmt.Sprintf("MESSAGES-%s", siteID),
		Subjects: []string{fmt.Sprintf("chat.user.*.room.*.%s.msg.>", siteID)},
	}
}

func MessagesCanonical(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-CANONICAL-%s", siteID),
		Subjects: []string{fmt.Sprintf("chat.msg.canonical.%s.>", siteID)},
	}
}

// MessagesTeams returns MESSAGES-TEAMS-{siteID}: the Teams-migration message-batch
// stream, separate from MESSAGES-CANONICAL so message-worker's teams mode and the
// live default mode each bind their own stream.
func MessagesTeams(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-TEAMS-%s", siteID),
		Subjects: []string{subject.MsgTeamsCanonicalWildcard(siteID)},
	}
}

func Rooms(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("ROOMS-%s", siteID),
		Subjects: []string{subject.RoomCanonicalWildcard(siteID)},
	}
}

// RoomsTeams returns ROOMS-TEAMS-{siteID}, isolating the Teams-migration
// room-create batch on its own stream. room-worker owns bootstrap + consumes;
// teams-room-creation publishes.
func RoomsTeams(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("ROOMS-TEAMS-%s", siteID),
		Subjects: []string{subject.RoomTeamsCanonicalWildcard(siteID)},
	}
}

// PushNotification returns the PUSH-NOTIFICATION-{siteID} stream config; ops-owned in prod.
func PushNotification(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("PUSH-NOTIFICATION-%s", siteID),
		Subjects: []string{subject.PushNotificationFilter(siteID)},
	}
}

// Inbox returns the INBOX-{siteID} stream, with two non-overlapping lanes (internal same-site
// search feed vs external cross-site) — no sourcing/SubjectTransform; remote sites publish the external lane directly.
func Inbox(siteID string) Config {
	return Config{
		Name: fmt.Sprintf("INBOX-%s", siteID),
		Subjects: []string{
			fmt.Sprintf("chat.inbox.%s.internal.>", siteID),
			fmt.Sprintf("chat.inbox.%s.external.>", siteID),
		},
	}
}

// Outbox returns OUTBOX-{siteID}: durable federation-relay lane; outbox-worker owns bootstrap.
func Outbox(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX-%s", siteID),
		Subjects: []string{subject.OutboxWildcard(siteID)},
	}
}

// MigrationOplog returns MIGRATION-OPLOG-{siteID}: raw CDC events from legacy source Mongo.
func MigrationOplog(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MIGRATION-OPLOG-%s", siteID),
		Subjects: []string{subject.MigrationOplogWildcard(siteID)},
	}
}

// BotMessagesCanonical returns BOT-MESSAGES-CANONICAL-{siteID}, published by bot-message-handler.
// Consumed by bot-message-worker, bot-broadcast-worker, bot-notification-worker, search-sync-worker.
func BotMessagesCanonical(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("BOT-MESSAGES-CANONICAL-%s", siteID),
		Subjects: []string{subject.BotCanonicalWildcard(siteID)},
	}
}

// BotPushNotification returns BOT-PUSH-NOTIFICATION-{siteID}, isolated from user PUSH-NOTIFICATION so a bot-notification incident cannot touch user push delivery.
func BotPushNotification(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("BOT-PUSH-NOTIFICATION-%s", siteID),
		Subjects: []string{subject.BotPushNotificationWildcard(siteID)},
	}
}

// OrgSyncStream is HR-{centralSiteID}, populated daily by hr-syncer at the central site.
func OrgSyncStream(centralSiteID string) Config {
	return Config{
		Name:     fmt.Sprintf("HR-%s", centralSiteID),
		Subjects: []string{fmt.Sprintf("chat.hr.%s.>", centralSiteID)},
	}
}
