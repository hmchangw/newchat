package model

import "time"

// SearchMessagesRequest is the NATS payload for `chat.user.{account}.request.search.messages`.
//
// RoomIDs, when empty, means global search across all rooms the user has
// access to. When set, the search is scoped to the listed rooms; the service
// enforces access using the per-user restricted-rooms map.
type SearchMessagesRequest struct {
	SearchText string   `json:"searchText"`
	RoomIDs    []string `json:"roomIds,omitempty"`
	Size       int      `json:"size,omitempty"`
	Offset     int      `json:"offset,omitempty"`
}

// SearchMessagesResponse is the NATS reply for `search.messages`.
type SearchMessagesResponse struct {
	Total   int64              `json:"total"`
	Results []MessageSearchHit `json:"results"`
}

// MessageSearchHit is a single message result in SearchMessagesResponse.
// Fields mirror the `messages-*` ES index document written by search-sync-worker.
type MessageSearchHit struct {
	MessageID             string     `json:"messageId"`
	RoomID                string     `json:"roomId"`
	SiteID                string     `json:"siteId"`
	UserID                string     `json:"userId"`
	UserAccount           string     `json:"userAccount"`
	Content               string     `json:"content"`
	CreatedAt             time.Time  `json:"createdAt"`
	ThreadParentMessageID string     `json:"threadParentMessageId,omitempty"`
	ThreadParentCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty"`
}

// SearchRoomsRequest is the NATS payload for `chat.user.{account}.request.search.rooms`.
//
// Scope values: "all" (default), "channel" (roomType=p), "dm" (roomType=d).
// "app" is reserved and currently rejected as unsupported in MVP.
type SearchRoomsRequest struct {
	SearchText string `json:"searchText"`
	Scope      string `json:"scope,omitempty"`
	Size       int    `json:"size,omitempty"`
	Offset     int    `json:"offset,omitempty"`
}

// SearchRoomsResponse is the NATS reply for `search.rooms`.
type SearchRoomsResponse struct {
	Total   int64           `json:"total"`
	Results []RoomSearchHit `json:"results"`
}

// RoomSearchHit is a single room result in SearchRoomsResponse.
// Fields mirror the `spotlight` ES index document written by search-sync-worker.
type RoomSearchHit struct {
	RoomID      string    `json:"roomId"`
	RoomName    string    `json:"roomName"`
	RoomType    string    `json:"roomType"`
	UserAccount string    `json:"userAccount"`
	SiteID      string    `json:"siteId"`
	JoinedAt    time.Time `json:"joinedAt"`
}
