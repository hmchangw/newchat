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

// PushNotification returns the PUSH_NOTIFICATION_{siteID} stream config.
// Owned by ops in production; notification-worker bootstraps it in dev only.
func PushNotification(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("PUSH_NOTIFICATION_%s", siteID),
		Subjects: []string{subject.PushNotificationFilter(siteID)},
	}
}

// BotDelivery returns the BOT_DELIVERY_{siteID} stream config (the bot platform's
// durable-pull feed). Ops-owned in prod, dev-bootstrapped by room-worker; the
// backend publishes to the subject, never the name, so a prod name can differ.
func BotDelivery(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("BOT_DELIVERY_%s", siteID),
		Subjects: []string{subject.BotDelivery(siteID)},
	}
}

// Inbox returns the canonical config for the `INBOX_{siteID}` stream that
// carries federation events for a site.
//
// The stream declares TWO non-overlapping subject patterns so the
// internal (same-site) vs. external (cross-site) split is explicit in the
// stream schema itself:
//
//   - `chat.inbox.{siteID}.internal.>`
//     Local-origin publishes from same-site services (e.g., room-worker
//     publishing `chat.inbox.{siteID}.internal.member_added`). This is a
//     search-indexing feed only; inbox-worker does NOT consume it because the
//     originating service already applied the change to the local DB.
//
//   - `chat.inbox.{siteID}.external.>`
//     Remote-origin events published directly by a service at another site
//     via a cross-supercluster JetStream publish to
//     `chat.inbox.{siteID}.external.{eventType}`. inbox-worker consumes this
//     lane and applies each event to the local DB.
//
// There is no Sources/SubjectTransform federation wiring: remote sites write
// the external lane directly. Consumers only need Name + Subjects to bind.
func Inbox(siteID string) Config {
	return Config{
		Name: fmt.Sprintf("INBOX_%s", siteID),
		Subjects: []string{
			fmt.Sprintf("chat.inbox.%s.internal.>", siteID),
			fmt.Sprintf("chat.inbox.%s.external.>", siteID),
		},
	}
}

// Outbox returns the OUTBOX_{siteID} stream config: the durable federation-relay
// lane. room-service (and other same-site publishers) write OutboxEvents
// on chat.outbox.{siteID}.{eventType}; outbox-worker consumes the stream and
// forwards each target to the destination site's INBOX. Owned by outbox-worker
// (dev bootstrap; ops/IaC in prod).
func Outbox(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX_%s", siteID),
		Subjects: []string{subject.OutboxWildcard(siteID)},
	}
}

// MigrationOplog returns the MIGRATION_OPLOG_{siteID} stream config: raw CDC events from the legacy source Mongo. Owned by the oplog-connector (dev bootstrap; ops/IaC in prod).
func MigrationOplog(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("MIGRATION_OPLOG_%s", siteID),
		Subjects: []string{subject.MigrationOplogWildcard(siteID)},
	}
}

// OrgSyncStream is the HR_{centralSiteID} stream populated by hr-syncer's
// daily publishes on chat.hr.{centralSiteID}.>. hr-syncer runs at one
// central site; every fab site's search-sync-worker consumes from it.
func OrgSyncStream(centralSiteID string) Config {
	return Config{
		Name:     fmt.Sprintf("HR_%s", centralSiteID),
		Subjects: []string{fmt.Sprintf("chat.hr.%s.>", centralSiteID)},
	}
}
