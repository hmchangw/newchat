package cassandra

// RoomType is the room classification shared by Mongo room/subscription
// documents and enrichment carriers. Canonical constants (RoomTypeChannel,
// RoomTypeDM, RoomTypeBotDM, RoomTypeDiscussion) live in pkg/model/room.go;
// the type itself lives here so ForwardedMessage.Room can reference it
// without an import cycle (pkg/model imports this package).
type RoomType string

// MessageRoom is the enriched room object attached to a SearchMessage and to
// a ForwardedMessage snapshot. Type is the room type. HRInfo is set only for
// dm rooms; AppInfo only for botDM rooms (search enrichment only — forwarded
// snapshots never set either). Name is the app name (botDM), the
// counterpart's display name (dm), or the canonical room name
// (channel/discussion). On a forwarded snapshot, dm/botDM sources carry only
// ID and Type.
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
