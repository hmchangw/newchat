package cassandra

// RoomType is the room classification shared by Mongo room/subscription
// documents and enrichment carriers. Canonical constants (RoomTypeChannel,
// RoomTypeDM, RoomTypeBotDM, RoomTypeDiscussion) live in pkg/model/room.go;
// the type itself lives here so ForwardedMessage.RoomType can reference it
// without an import cycle (pkg/model imports this package).
type RoomType string

// MessageRoom is the enriched room object attached to a SearchMessage, built
// per-read by search-service. Type is the room type. HRInfo is set only for dm
// rooms; AppInfo only for botDM rooms. Name is the app name (botDM), the
// counterpart's display name (dm), or the canonical room name
// (channel/discussion). Forwarded snapshots do NOT use this type — they carry
// an inline immutable RoomType instead of a resolved-on-read room object.
type MessageRoom struct {
	ID      string          `json:"id"`
	Name    string          `json:"name,omitempty"`
	Type    RoomType        `json:"type,omitempty"`
	HRInfo  *MessageHRInfo  `json:"hrInfo,omitempty"`
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
