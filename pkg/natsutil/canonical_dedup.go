package natsutil

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// CanonicalDedupID returns the Nats-Msg-Id for a MessageEvent published to
// MESSAGES_CANONICAL. The op suffix keeps event keyspaces disjoint within the
// stream's dedup window; the per-op timestamp/discriminator suffix gives each
// distinct occurrence its own key. Unknown event types fall back to the bare
// messageID.
//
//   - EventCreated:  "<messageID>"
//   - EventUpdated:  "<messageID>:updated:<editedAtUnixMilli>"
//   - EventDeleted:  "<messageID>:deleted"
//   - EventPinned:   "<messageID>:pinned:<pinnedAtUnixMilli>"
//   - EventUnpinned: "<messageID>:unpinned:<evt.Timestamp>"
//   - EventReacted:  "<messageID>:reacted:<actor>:<shortcode>:<action>:<timestampMs>"
//
// A nil ReactionDelta on EventReacted is a publisher contract violation; the
// fallback to the bare messageID is intentional so the buggy event collides
// loudly with EventCreated rather than dedup-shadowed under ":reacted".
func CanonicalDedupID(evt *model.MessageEvent) string {
	switch evt.Event {
	case model.EventUpdated:
		return fmt.Sprintf("%s:%s:%d", evt.Message.ID, evt.Event, evt.Message.EditedAt.UnixMilli())
	case model.EventDeleted:
		return evt.Message.ID + ":" + string(evt.Event)
	case model.EventPinned:
		return fmt.Sprintf("%s:%s:%d", evt.Message.ID, evt.Event, evt.Message.PinnedAt.UnixMilli())
	case model.EventUnpinned:
		return fmt.Sprintf("%s:%s:%d", evt.Message.ID, evt.Event, evt.Timestamp)
	case model.EventReacted:
		if evt.ReactionDelta == nil {
			return evt.Message.ID
		}
		return fmt.Sprintf("%s:%s:%s:%s:%s:%d",
			evt.Message.ID, evt.Event,
			evt.ReactionDelta.Actor.Account, evt.ReactionDelta.Shortcode,
			evt.ReactionDelta.Action, evt.Timestamp)
	default:
		return evt.Message.ID
	}
}
