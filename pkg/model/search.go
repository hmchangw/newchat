package model

import "time"

// SearchMessagesRequest is the NATS payload for `chat.user.{account}.request.search.{siteID}.messages`.
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
type SearchMessage struct {
	MessageID                    string     `json:"messageId"`
	RoomID                       string     `json:"roomId"`
	SiteID                       string     `json:"siteId"`
	UserAccount                  string     `json:"userAccount"`
	Content                      string     `json:"content"`
	CreatedAt                    time.Time  `json:"createdAt"`
	EditedAt                     *time.Time `json:"editedAt,omitempty"`
	UpdatedAt                    *time.Time `json:"updatedAt,omitempty"`
	ThreadParentMessageID        string     `json:"threadParentMessageId,omitempty"`
	ThreadParentMessageCreatedAt *time.Time `json:"threadParentMessageCreatedAt,omitempty"`

	// Render payloads mirrored as-is from the message (same wire shape as
	// history reads) so the client can render hits without a second lookup.
	Attachments []Attachment `json:"attachments,omitempty"`
	Card        *Card        `json:"card,omitempty"`
}

// SearchRoomsRequest is the NATS payload for
// `chat.user.{account}.request.search.{siteID}.rooms`.
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

// SearchRoom is the per-user-room projection returned by search.rooms,
// built directly from the spotlight ES index hit (one doc per
// (account, room)). SiteID is the room's home site, carried on the
// spotlight doc by search-sync-worker.
type SearchRoom struct {
	RoomID   string `json:"roomId"`
	Name     string `json:"name"`
	RoomType string `json:"roomType,omitempty"`
	SiteID   string `json:"siteId"`
}

// SearchAppsRequest is the NATS payload for `chat.user.{account}.request.search.{siteID}.apps`.
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

// SearchUsersRequest is the NATS payload for `chat.user.{account}.request.search.{siteID}.users`.
// Offset/Limit page the third-party HR endpoint; Limit defaults to 25 when 0.
type SearchUsersRequest struct {
	Query  string `json:"query"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
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

// SearchOrgsRequest is the NATS payload for `chat.user.{account}.request.search.{siteID}.orgs`.
//
// Query is a non-empty prefix match on the organization name fields
// (section/department names + division id). Whitespace-only is rejected.
// The org index is company-wide, so results are NOT scoped to the caller.
type SearchOrgsRequest struct {
	Query  string `json:"query"`
	Size   int    `json:"size,omitempty"`
	Offset int    `json:"offset,omitempty"`
}

// SearchOrgsResponse is the NATS reply for `search.orgs`. Orgs is always
// non-nil (empty slice marshals as []).
type SearchOrgsResponse struct {
	Orgs []SearchOrg `json:"orgs"`
}

// SearchOrg is a single organization result returned by the `search.orgs`
// RPC, projected directly from the spotlight-org ES index (one document per
// section, keyed by sectId). Fields mirror the index document maintained by
// search-sync-worker; optional fields are omitted when the source doc is
// partial.
type SearchOrg struct {
	SectID          string `json:"sectId"`
	SectName        string `json:"sectName,omitempty"`
	SectTCName      string `json:"sectTCName,omitempty"`
	SectDescription string `json:"sectDescription,omitempty"`
	DeptID          string `json:"deptId,omitempty"`
	DeptName        string `json:"deptName,omitempty"`
	DeptTCName      string `json:"deptTCName,omitempty"`
	DeptDescription string `json:"deptDescription,omitempty"`
	DivisionID      string `json:"divisionId,omitempty"`
}
