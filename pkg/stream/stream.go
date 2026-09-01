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

// InboxFailover returns INBOX-FAILOVER-{siteID}: the standby inbound-federation
// lane for a site, hosted on that site's BUDDY cluster so it survives the site's
// own NATS outage. Peers redirect here when the site's primary INBOX is
// unreachable; the site's own inbox-worker consumes it over its buddy connection
// and applies each event to the site's own DB.
//
// Named for the origin site, not the host — stream names are unique across the
// supercluster (one account, one JetStream domain).
//
// Carries only the external.> lane: the internal.> lane is a same-site search
// feed published by services that are idle during the outage.
//
// Placement is ops-owned and MUST name the buddy's cluster; the owning service
// asserts it at startup via CheckPlacement rather than setting it here.
func InboxFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("INBOX-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverInboxExternalAll(siteID)},
	}
}

// Outbox returns OUTBOX-{siteID}: durable federation-relay lane; outbox-worker owns bootstrap.
func Outbox(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX-%s", siteID),
		Subjects: []string{subject.OutboxWildcard(siteID)},
	}
}

// MessagesFailover returns MESSAGES-FAILOVER-{siteID}: the standby ingress lane,
// hosted on the site's buddy cluster. Displaced clients publish here while the
// site's own NATS is down; message-gatekeeper consumes it over its buddy
// connection. Placement is ops-owned and MUST name the buddy's cluster.
func MessagesFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverMsgSendWildcard(siteID)},
	}
}

// MessagesCanonicalFailover returns MESSAGES-CANONICAL-FAILOVER-{siteID}: the
// standby validated-message lane, fan-in for message-worker, broadcast-worker,
// notification-worker and search-sync-worker on the failover path.
func MessagesCanonicalFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MESSAGES-CANONICAL-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverMsgCanonicalWildcard(siteID)},
	}
}

// PushNotificationFailover returns PUSH-NOTIFICATION-FAILOVER-{siteID}: the
// standby push-request lane between notification-worker and
// push-notification-service.
func PushNotificationFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("PUSH-NOTIFICATION-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverPushNotificationFilter(siteID)},
	}
}

// OutboxFailover returns OUTBOX-FAILOVER-{siteID}: the standby origin-side
// federation buffer, so a site keeps federating OUT while its own NATS is down.
// Consumed with the same ConcurrentEventTypes / OrderedEventTypes partition as
// the live OUTBOX — an event type in neither set would sit here unconsumed.
func OutboxFailover(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX-FAILOVER-%s", siteID),
		Subjects: []string{subject.FailoverOutboxWildcard(siteID)},
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
