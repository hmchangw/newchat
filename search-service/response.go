package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// rawHit is generic over the source type so both message and room
// results can share the response-envelope shape.
type rawHit[T any] struct {
	Source T `json:"_source"`
}

type rawResponse[T any] struct {
	Hits struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
		Hits []rawHit[T] `json:"hits"`
	} `json:"hits"`
}

// messageSearchHit is the internal staging type produced by parseMessagesResponse.
// Fields mirror the ES messages-* index; the public reply type
// (model.SearchMessage) is a projection of this struct with `UserID` dropped.
//
// TODO(searchMessages-editedAt-updatedAt): add `EditedAt *time.Time` and
// `UpdatedAt *time.Time` once the upstream wiring lands (model.Message +
// MessageSearchIndex in search-sync-worker). See pkg/model/search.go's
// SearchMessage doc comment and spec follow-up #5.
type messageSearchHit struct {
	MessageID             string     `json:"messageId"`
	RoomID                string     `json:"roomId"`
	SiteID                string     `json:"siteId"`
	UserID                string     `json:"userId"`
	UserAccount           string     `json:"userAccount"`
	Content               string     `json:"content"`
	CreatedAt             time.Time  `json:"createdAt"`
	ThreadParentID        string     `json:"threadParentMessageId,omitempty"`
	ThreadParentCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty"`
}

// roomSearchHit is the ES `_source` shape for a spotlight hit used
// during the subscription search flow. Only `roomId` is extracted; the other
// fields are present in the index but unused after the Mongo hydration step.
type roomSearchHit struct {
	RoomID string `json:"roomId"`
}

func parseMessagesResponse(raw json.RawMessage) ([]messageSearchHit, int64, error) {
	var rr rawResponse[messageSearchHit]
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, 0, fmt.Errorf("parse messages response: %w", err)
	}

	out := make([]messageSearchHit, 0, len(rr.Hits.Hits))
	for i := range rr.Hits.Hits {
		out = append(out, rr.Hits.Hits[i].Source)
	}
	return out, rr.Hits.Total.Value, nil
}

// toSearchMessage projects an internal messageSearchHit into the public
// model.SearchMessage wire type. Display enrichment (user name, room name)
// is the client's responsibility — resolve via user-service lookups (or
// cache locally) using the UserAccount and RoomID returned here.
func toSearchMessage(hit *messageSearchHit) model.SearchMessage {
	return model.SearchMessage{
		MessageID:                    hit.MessageID,
		RoomID:                       hit.RoomID,
		SiteID:                       hit.SiteID,
		UserAccount:                  hit.UserAccount,
		Content:                      hit.Content,
		CreatedAt:                    hit.CreatedAt,
		ThreadParentMessageID:        hit.ThreadParentID,
		ThreadParentMessageCreatedAt: hit.ThreadParentCreatedAt,
	}
}

// parseRoomIDs extracts the ordered list of room IDs from a
// spotlight ES response. The caller passes these IDs to HydrateRooms
// for Mongo enrichment.
func parseRoomIDs(raw json.RawMessage) ([]string, error) {
	var rr rawResponse[roomSearchHit]
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, fmt.Errorf("parse subscription room IDs: %w", err)
	}

	ids := make([]string, 0, len(rr.Hits.Hits))
	for i := range rr.Hits.Hits {
		ids = append(ids, rr.Hits.Hits[i].Source.RoomID)
	}
	return ids, nil
}
