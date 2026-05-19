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
type messageSearchHit struct {
	MessageID             string     `json:"messageId"`
	RoomID                string     `json:"roomId"`
	SiteID                string     `json:"siteId"`
	UserID                string     `json:"userId"`
	UserAccount           string     `json:"userAccount"`
	Content               string     `json:"content"`
	CreatedAt             time.Time  `json:"createdAt"`
	EditedAt              *time.Time `json:"editedAt,omitempty"`
	UpdatedAt             *time.Time `json:"updatedAt,omitempty"`
	ThreadParentID        string     `json:"threadParentMessageId,omitempty"`
	ThreadParentCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty"`
}

// roomSearchHit is the spotlight ES `_source` shape for a room
// typeahead hit. search.rooms is served directly from this index
// (one doc per (account, room)); the fields map onto model.SearchRoom.
type roomSearchHit struct {
	RoomID   string `json:"roomId"`
	RoomName string `json:"roomName"`
	RoomType string `json:"roomType"`
	SiteID   string `json:"siteId"`
}

func toSearchRoom(h roomSearchHit) model.SearchRoom {
	return model.SearchRoom{
		RoomID:   h.RoomID,
		Name:     h.RoomName,
		RoomType: h.RoomType,
		SiteID:   h.SiteID,
	}
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
		EditedAt:                     hit.EditedAt,
		UpdatedAt:                    hit.UpdatedAt,
		ThreadParentMessageID:        hit.ThreadParentID,
		ThreadParentMessageCreatedAt: hit.ThreadParentCreatedAt,
	}
}

// parseRooms extracts the ordered list of rooms from a spotlight ES
// response, preserving ES relevance order.
func parseRooms(raw json.RawMessage) ([]model.SearchRoom, error) {
	var rr rawResponse[roomSearchHit]
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, fmt.Errorf("parse spotlight rooms response: %w", err)
	}

	rooms := make([]model.SearchRoom, 0, len(rr.Hits.Hits))
	for i := range rr.Hits.Hits {
		rooms = append(rooms, toSearchRoom(rr.Hits.Hits[i].Source))
	}
	return rooms, nil
}
