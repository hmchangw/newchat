package model

import "time"

// SearchMessagesRequest is the NATS payload for `chat.user.{account}.request.search.messages`.
//
// RoomIDs, when empty, means global search across all rooms the user has
// access to. When set, the search is scoped to the listed rooms; the service
// enforces access using the per-user restricted-rooms map.
type SearchMessagesRequest struct {
	Query   string   `json:"query"`
	RoomIDs []string `json:"roomIds,omitempty"`
	Size    int      `json:"size,omitempty"`
	Offset  int      `json:"offset,omitempty"`
}

// SearchMessagesResponse is the NATS reply for `search.messages`.
// Breaking change from the prior shape ({total, results}): the field
// containing hits is now "messages" and the type is SearchMessage (an
// enriched projection) rather than the former MessageSearchHit.
type SearchMessagesResponse struct {
	Messages []SearchMessage `json:"messages"`
	Total    int64           `json:"total"`
}

// SearchMessage is the per-hit projection returned by search.messages.
// Every field is sourced directly from the ES messages-* index — no
// Mongo enrichment occurs server-side. Display fields (user name, room
// name) are the client's responsibility; resolve via the user-service
// lookups or a local subscription cache.
//
// TODO(searchMessages-editedAt-updatedAt): add `EditedAt *time.Time`
// and `UpdatedAt *time.Time` once they propagate through:
//  1. pkg/model.Message (add the fields + populate in edit/update flow)
//  2. search-sync-worker/messages.go MessageSearchIndex (index them)
//  3. search-service/response.go messageSearchHit (decode them)
//  4. search-service/response.go toSearchMessage (copy them)
//
// See spec follow-up #5 in
// docs/superpowers/specs/2026-05-13-search-service-nats-migrations-design.md.
type SearchMessage struct {
	MessageID                    string     `json:"messageId"`
	RoomID                       string     `json:"roomId"`
	SiteID                       string     `json:"siteId"`
	UserAccount                  string     `json:"userAccount"`
	Content                      string     `json:"content"`
	CreatedAt                    time.Time  `json:"createdAt"`
	ThreadParentMessageID        string     `json:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty"`
}

// SearchRoomsRequest is the NATS payload for
// `chat.user.{account}.request.search.rooms`.
//
// Query is a non-empty substring match on room name (case-insensitive prefix).
// RoomType filters by subscription type: "all" (default, same as empty),
// "channel", or "dm". The value "app" and any other value are rejected with
// ErrBadRequest.
type SearchRoomsRequest struct {
	Query    string `json:"query"`
	RoomType string `json:"roomType,omitempty"`
	Size     int    `json:"size,omitempty"`
	Offset   int    `json:"offset,omitempty"`
}

// SearchRoomsResponse is the NATS reply for `search.rooms`.
// Rooms is always non-nil (empty slice marshals as []).
type SearchRoomsResponse struct {
	Rooms []SearchRoom `json:"rooms"`
}

// SearchRoom is the per-user-room projection returned by
// search.rooms. Field list mirrors the legacy HTTP shape for
// the /rooms endpoint — fill in additional fields per the legacy
// response during implementation.
type SearchRoom struct {
	RoomID   string `json:"roomId"             bson:"roomId"`
	Name     string `json:"name"               bson:"name"`
	RoomType string `json:"roomType,omitempty" bson:"roomType,omitempty"`
}

// SearchAppsRequest is the NATS payload for `chat.user.{account}.request.search.apps`.
//
// Query is a non-empty substring match (case-insensitive). AssistantEnabled is a
// strict equality filter on `app.assistant.enabled` when non-nil; nil means no filter.
type SearchAppsRequest struct {
	Query            string `json:"query"`
	AssistantEnabled *bool  `json:"assistantEnabled,omitempty"`
	Size             int    `json:"size,omitempty"`
	Offset           int    `json:"offset,omitempty"`
}

// SearchAppsResponse is the NATS reply for `search.apps`. Apps is always
// non-nil (empty slice marshals as []), and is scoped to apps the caller
// has subscribed to (enforced by the pipeline's $lookup against the
// subscriptions collection).
type SearchAppsResponse struct {
	Apps []App `json:"apps"`
}

// SearchUsersRequest is the NATS payload for `chat.user.{account}.request.search.users`.
//
// No pagination — the third-party HR endpoint hardcodes offset=0, limit=25.
type SearchUsersRequest struct {
	Query string `json:"query"`
}

// SearchUser is a single user result returned by the `search.users` RPC.
// Fields mirror the legacy GET /api/v3/users HTTP response shape.
//
// TODO(searchUsers-thirdparty): copy the exact field list from the legacy
// HTTP response struct when wiring the real third-party endpoint.
// The placeholder fields below cover the known subset; add or remove as
// needed to match the actual wire shape.
type SearchUser struct {
	Account     string `json:"account"`
	EngName     string `json:"engName,omitempty"`
	ChineseName string `json:"chineseName,omitempty"`
	// ... more fields per the legacy shape — add here and in the roundTrip test above
}
