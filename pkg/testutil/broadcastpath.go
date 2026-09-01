// This file is deliberately untagged, for the reason env.go gives: every other
// file in this package is //go:build integration, and this table has to be
// importable from the ordinary unit tests of message-gatekeeper and
// broadcast-worker without dragging testcontainers into their build.

package testutil

import (
	"github.com/hmchangw/chat/pkg/broadcastpath"
	"github.com/hmchangw/chat/pkg/model"
)

// SampleThreadParentMessageID is a syntactically valid 20-char base62 message
// id, so a case from this table can be fed to message-gatekeeper's handler,
// which validates the field before it ever classifies it.
const SampleThreadParentMessageID = "0123456789abcdefghij"

// BroadcastPathCase is one row of the shared classification table.
type BroadcastPathCase struct {
	Name                  string
	ThreadParentMessageID string
	// TShow is the normalized value (req.TShow && ThreadParentMessageID != ""),
	// which is what lands on the canonical message.
	TShow    bool
	RoomType model.RoomType
	Want     broadcastpath.Path
}

// BroadcastPathCases is the table that message-gatekeeper's label and
// broadcast-worker's dispatch are both asserted against, case for case.
//
// It lives here rather than in either service because the point is that the two
// agree: SLO-1b's denominator is emitted by the gatekeeper and its numerator by
// broadcast-worker, so two tables that drift apart give a ratio whose halves
// count different messages. A per-service copy is exactly how that happens.
func BroadcastPathCases() []BroadcastPathCase {
	return []BroadcastPathCase{
		{
			Name:     "channel message",
			RoomType: model.RoomTypeChannel,
			Want:     broadcastpath.RoomSubject,
		},
		{
			Name:     "dm",
			RoomType: model.RoomTypeDM,
			Want:     broadcastpath.DM,
		},
		{
			Name:     "bot dm",
			RoomType: model.RoomTypeBotDM,
			Want:     broadcastpath.DM,
		},
		{
			Name:                  "hidden thread reply in a channel room",
			ThreadParentMessageID: SampleThreadParentMessageID,
			RoomType:              model.RoomTypeChannel,
			Want:                  broadcastpath.Thread,
		},
		{
			Name:                  "hidden thread reply in a dm",
			ThreadParentMessageID: SampleThreadParentMessageID,
			RoomType:              model.RoomTypeDM,
			Want:                  broadcastpath.Thread,
		},
		{
			// tshow asks the reply to also appear in the room timeline, so the
			// worker sends it down the room-subject path, not the thread path.
			Name:                  "thread reply with tshow in a channel room",
			ThreadParentMessageID: SampleThreadParentMessageID,
			TShow:                 true,
			RoomType:              model.RoomTypeChannel,
			Want:                  broadcastpath.RoomSubject,
		},
		{
			Name:                  "thread reply with tshow in a dm",
			ThreadParentMessageID: SampleThreadParentMessageID,
			TShow:                 true,
			RoomType:              model.RoomTypeDM,
			Want:                  broadcastpath.DM,
		},
		{
			// The normalized tshow is false here, so this is not a thread reply
			// at all — it is an ordinary channel message whose TShow the
			// gatekeeper ignored. Passing req.TShow instead of the normalized
			// value is what this row catches.
			Name:     "tshow with no thread parent",
			TShow:    false,
			RoomType: model.RoomTypeChannel,
			Want:     broadcastpath.RoomSubject,
		},
		{
			// How a failed room-meta lookup arrives: no type to switch on.
			Name:     "unresolved room type",
			RoomType: "",
			Want:     broadcastpath.Unknown,
		},
		{
			Name:     "unrecognised room type",
			RoomType: "something-new",
			Want:     broadcastpath.Unknown,
		},
	}
}
