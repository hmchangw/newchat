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

func Outbox(siteID string) Config {
	return Config{
		Name:     fmt.Sprintf("OUTBOX_%s", siteID),
		Subjects: []string{fmt.Sprintf("outbox.%s.>", siteID)},
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

// Inbox returns the canonical config for the `INBOX_{siteID}` stream that
// carries subscription lifecycle events (member_added, member_removed)
// plus any other aggregated events federated in from other sites.
//
// The stream declares TWO non-overlapping subject patterns so the local vs.
// federated split is explicit in the stream schema itself:
//
//   - `chat.inbox.{siteID}.*`
//     Local direct publishes from same-site services (e.g., room-worker
//     publishing `chat.inbox.{siteID}.member_added`). Single-token suffix,
//     so it matches local event names only.
//
//   - `chat.inbox.{siteID}.aggregate.>`
//     Federated events sourced from remote OUTBOX streams. These land here
//     via a JetStream SubjectTransform that rewrites
//     `outbox.{remote}.to.{siteID}.>` → `chat.inbox.{siteID}.aggregate.>`
//     on the way into this stream. Multi-token-safe so the transform can
//     preserve any event shape.
//
// Together the two patterns carry exactly the four member events consumers
// care about today — local member_added / member_removed and federated
// aggregate.member_added / aggregate.member_removed — without an overly
// broad catch-all that would silently accept typos.
//
// Cross-site Sources + SubjectTransforms themselves are a deployment-time
// concern layered on by the service that owns stream creation, so this
// baseline leaves Sources empty. Consumers only need Name + Subjects to
// bind.
func Inbox(siteID string) Config {
	return Config{
		Name: fmt.Sprintf("INBOX_%s", siteID),
		Subjects: []string{
			fmt.Sprintf("chat.inbox.%s.*", siteID),
			fmt.Sprintf("chat.inbox.%s.aggregate.>", siteID),
		},
	}
}
