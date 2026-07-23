package model

import "encoding/json"

// ThreadListItem is one row of a user's cross-site thread inbox: a thread the
// user is subscribed to, enriched with its parent and last message plus the
// owning room's name/type. All fields are resolved on the thread's owning site.
type ThreadListItem struct {
	SiteID          string   `json:"siteId"          bson:"siteId"`
	RoomID          string   `json:"roomId"          bson:"roomId"`
	RoomName        string   `json:"roomName"        bson:"roomName"`
	RoomType        RoomType `json:"roomType"        bson:"roomType"`
	ThreadRoomID    string   `json:"threadRoomId"    bson:"threadRoomId"`
	ParentMessageID string   `json:"parentMessageId" bson:"parentMessageId"`

	// Subscription state for this user.
	LastSeenAt *int64 `json:"lastSeenAt,omitempty" bson:"lastSeenAt,omitempty"` // UTC ms
	HasMention bool   `json:"hasMention"           bson:"hasMention"`
	Unread     bool   `json:"unread"               bson:"unread"` // lastMsgAt > lastSeenAt

	// LastMsgAt is the thread's last-activity time (UTC ms) and the global sort
	// key for the inbox. Reply count rides on ParentMessage.TCount.
	LastMsgAt int64 `json:"lastMsgAt" bson:"lastMsgAt"`

	// Hydrated message bodies, subject to the thread access window. Carried opaque:
	// history-service emits them pre-marshaled from *cassandra.Message and user-service
	// forwards them to the client verbatim, never decoding the Message (avoids parsing
	// Reactions, whose struct-keyed map has no JSON decoder).
	ParentMessage json.RawMessage `json:"parentMessage,omitempty" bson:"parentMessage,omitempty"`
	LastMessage   json.RawMessage `json:"lastMessage,omitempty"   bson:"lastMessage,omitempty"`

	// HRInfo carries the DM counterpart's HR-directory record (native + English
	// name). The user-service aggregator resolves it from RoomName (which holds the
	// counterpart account for DM rooms); present on DM rows only.
	HRInfo *SubscriptionHRInfo `json:"hrInfo,omitempty" bson:"hrInfo,omitempty"`
}

// ThreadSubscriptionListRequest is the server-to-server leaf request the
// user-service aggregator sends to each site's history-service. The cursor is a
// value position on the global (lastMsgAt, threadRoomId) sort key; nil
// CursorLastMsgAt requests the first page.
type ThreadSubscriptionListRequest struct {
	Account            string `json:"account"`
	CursorLastMsgAt    *int64 `json:"cursorLastMsgAt,omitempty"` // UTC ms
	CursorThreadRoomID string `json:"cursorThreadRoomId,omitempty"`
	Limit              int    `json:"limit"`
}

// ThreadSubscriptionListResponse is a single site's page: items sorted
// (lastMsgAt, threadRoomId) DESC, capped at the requested limit. HasMore reports
// whether the site holds further items beyond this page.
type ThreadSubscriptionListResponse struct {
	Items   []ThreadListItem `json:"items"`
	HasMore bool             `json:"hasMore"`
}

// ThreadListRequest is the client-facing aggregator request. Cursor is the
// opaque base64 composite position returned by a previous response; omit it for
// the first page.
type ThreadListRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// ThreadListResponse is the merged, globally-ordered page returned to the
// client. UnavailableSites lists sites that failed to respond for this page;
// their threads may appear on a later page once they recover.
type ThreadListResponse struct {
	Items            []ThreadListItem `json:"items"`
	NextCursor       string           `json:"nextCursor,omitempty"`
	HasNext          bool             `json:"hasNext"`
	UnavailableSites []string         `json:"unavailableSites,omitempty"`
}
