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

	// Senders filters to messages from any of these userAccounts (multi-select From).
	Senders []string `json:"senders,omitempty"`
	// DateRange filters createdAt to [Start, End]; either bound may be zero to leave it open.
	DateRange *DateRange `json:"dateRange,omitempty"`
	// HasAttachment, set true, filters to messages carrying at least one attachment.
	HasAttachment *bool `json:"hasAttachment,omitempty"`
	// MentionedMe, set true, filters to messages that mention the requesting account.
	MentionedMe *bool `json:"mentionedMe,omitempty"`
	// FileTypes filters to messages with an attachment in any of these categories
	// (image/pdf/excel/powerpoint/word/zip/others). This is also how file search
	// folds into this RPC — no separate subject.
	FileTypes []string `json:"fileTypes,omitempty"`
}

// DateRange bounds a createdAt filter; presets (today/thisWeek/…) are resolved
// client-side into a concrete range before the request is sent.
type DateRange struct {
	Start time.Time `json:"start,omitempty"`
	End   time.Time `json:"end,omitempty"`
}

// SearchMessagesResponse is the NATS reply for `search.messages`.
// Breaking change from the prior shape ({total, results}): the field
// containing hits is now "messages" and the type is SearchMessage (an
// enriched projection) rather than the former MessageSearchHit.
type SearchMessagesResponse struct {
	Messages []SearchMessage `json:"messages"`
	Total    int64           `json:"total"`
}

// SearchMessage is the per-hit projection returned by search.messages, with
// room/sender enrichment resolved server-side (see MessageRoom / MessageSender).
// The base fields are sourced from the ES messages-* index; the enrichment
// fields are best-effort and omitted when they could not be resolved.
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
	// UserName is the sender's pre-composed display name (Message.UserDisplayName),
	// indexed so free-text query can match on sender name as well as body.
	UserName string `json:"userName,omitempty"`

	// Render payloads mirrored as-is from the message (same wire shape as
	// history reads) so the client can render hits without a second lookup.
	Attachments []Attachment `json:"attachments,omitempty"`
	Card        *Card        `json:"card,omitempty"`

	// Enrichment resolved server-side by search-service (best-effort; a field is
	// omitted when it could not be resolved).
	TShow  bool           `json:"tshow,omitempty"`
	Room   *MessageRoom   `json:"room,omitempty"`
	Sender *MessageSender `json:"sender,omitempty"`
}

// MessageRoom is the enriched room object attached to a SearchMessage.
// Type is the room type from the caller's subscription. HRInfo is set only
// for dm rooms; AppInfo only for botDM rooms. Name is the app name (botDM),
// the counterpart's display name (dm), or the canonical room name (channel/
// discussion, from the RoomsInfoBatch RPC).
type MessageRoom struct {
	ID      string          `json:"id"`
	Name    string          `json:"name,omitempty"`
	Type    RoomType        `json:"type,omitempty"`
	HRInfo  *MessageHRInfo  `json:"hrInfo,omitempty"`
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"`
}

// MessageSender is the enriched author object attached to a SearchMessage.
// HR is set for human senders, AppInfo for bot senders; both are omitted
// when the lookup missed — the client renders the display name.
type MessageSender struct {
	Account string          `json:"account"`
	HR      *MessageHRInfo  `json:"hr,omitempty"`
	AppInfo *MessageAppInfo `json:"appInfo,omitempty"`
}

// MessageHRInfo is the compact HR record on search sender/room objects.
type MessageHRInfo struct {
	Account     string `json:"account"`
	ChineseName string `json:"chineseName,omitempty"`
	EngName     string `json:"engName,omitempty"`
}

// MessageAppInfo is the compact app record on search sender/room objects.
// IsSubscribed is set only on room.appInfo (botDM) — explicit true/false from
// the caller's subscription row — and stays nil (absent) on sender.appInfo.
type MessageAppInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	AssistantName string `json:"assistantName"`
	IsSubscribed  *bool  `json:"isSubscribed,omitempty"`
}

// SearchRoomsRequest is the NATS payload for
// `chat.user.{account}.request.search.{siteID}.rooms`.
//
// Query is a substring match on room name (case-insensitive prefix); it may be
// empty only when Members is set — at least one of the two is required.
// RoomType filters by subscription type: "all" (default, same as empty),
// "channel", or "dm". The value "app" and any other value are rejected with
// ErrBadRequest.
type SearchRoomsRequest struct {
	Query    string `json:"query"`
	RoomType string `json:"roomType,omitempty"`
	Size     int    `json:"size,omitempty"`
	Offset   int    `json:"offset,omitempty"`
	// Members filters to channels that contain all of these accounts (plus the
	// requester), resolved via user-service subscription.getChannels. Query may
	// be empty when Members is set — at least one of the two is required.
	Members []string `json:"members,omitempty"`
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
