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
//
// The two tshow fields are separate on purpose. A case is consumed at two
// different points on the same message's path — message-gatekeeper sees the
// client's request, broadcast-worker sees the canonical message — and the
// gatekeeper normalizes between them. Carrying one field and feeding it to both
// would silently drop the only case where they differ: a client that sets tshow
// on a send with no thread parent. TestCasesNormalizeConsistently asserts the
// two stand in the gatekeeper's normalization relation, so a row cannot describe
// a message the gatekeeper could never produce.
type BroadcastPathCase struct {
	Name                  string
	ThreadParentMessageID string
	// RequestTShow is what the client sent — model.SendMessageRequest.TShow, the
	// input to message-gatekeeper.
	RequestTShow bool
	// TShow is the normalized value (RequestTShow && ThreadParentMessageID != "")
	// that lands on the canonical message, and so the input to broadcast-worker.
	TShow    bool
	RoomType model.RoomType
	Want     broadcastpath.Path
}

// NormalizedTShow is the gatekeeper's rule, exposed so a test can assert the
// table's two tshow fields agree with it rather than restating it.
func (c BroadcastPathCase) NormalizedTShow() bool {
	return c.RequestTShow && c.ThreadParentMessageID != ""
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
			RequestTShow:          true,
			TShow:                 true,
			RoomType:              model.RoomTypeChannel,
			Want:                  broadcastpath.RoomSubject,
		},
		{
			Name:                  "thread reply with tshow in a dm",
			ThreadParentMessageID: SampleThreadParentMessageID,
			RequestTShow:          true,
			TShow:                 true,
			RoomType:              model.RoomTypeDM,
			Want:                  broadcastpath.DM,
		},
		{
			// The client asked for tshow on a send that carries no thread parent.
			// The gatekeeper normalizes that away, so the canonical message
			// carries TShow=false and this is an ordinary channel message — the
			// one row where the request and the canonical message disagree, and
			// the reason the two fields are separate.
			Name:         "tshow with no thread parent",
			RequestTShow: true,
			TShow:        false,
			RoomType:     model.RoomTypeChannel,
			Want:         broadcastpath.RoomSubject,
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
