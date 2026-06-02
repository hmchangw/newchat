package natsutil

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// CanonicalDedupID returns the Nats-Msg-Id for a MessageEvent published to
// MESSAGES_CANONICAL. The op suffix keeps event keyspaces disjoint within the
// stream's dedup window; the per-op timestamp suffix gives each distinct
// occurrence (edit, pin, unpin) its own key so legitimate repeats aren't
// silently swallowed. Unknown event types fall back to the bare messageID.
//
//   - EventCreated:  "<messageID>"
//   - EventUpdated:  "<messageID>:updated:<editedAtUnixMilli>"
//   - EventDeleted:  "<messageID>:deleted"
//   - EventPinned:   "<messageID>:pinned:<pinnedAtUnixMilli>"
//   - EventUnpinned: "<messageID>:unpinned:<evt.Timestamp>"
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
	default:
		return evt.Message.ID
	}
}
