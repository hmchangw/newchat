package model

// App is a read-only view of the apps collection (provisioning is upstream).
type App struct {
	ID          string         `json:"id"                    bson:"_id"`
	Name        string         `json:"name"                  bson:"name"`
	Description string         `json:"description,omitempty" bson:"description,omitempty"`
	AvatarURL   string         `json:"avatarUrl,omitempty"   bson:"avatarUrl,omitempty"`
	Assistant   *AppAssistant  `json:"assistant,omitempty"   bson:"assistant,omitempty"`
	ChannelTab  *AppChannelTab `json:"channelTab,omitempty"  bson:"channelTab,omitempty"`
	Sponsors    []AppSponsor   `json:"sponsors,omitempty"    bson:"sponsors,omitempty"`
}

// AppAssistant: Name is the bot user account (".bot" suffix); botDM requires Enabled==true.
type AppAssistant struct {
	Enabled     bool   `json:"enabled"               bson:"enabled"`
	Name        string `json:"name"                  bson:"name"`
	SettingsURL string `json:"settingsUrl,omitempty" bson:"settingsUrl,omitempty"`
}

// AppChannelTab describes a tab that can be embedded into channel rooms.
// Default==true marks tabs that appear by default in every channel.
type AppChannelTab struct {
	Enabled bool             `json:"enabled" bson:"enabled"`
	Default bool             `json:"default" bson:"default"`
	Name    string           `json:"name"    bson:"name"`
	URL     AppChannelTabURL `json:"url"     bson:"url"`
}

// AppChannelTabURL holds the URL template. Default is the canonical form
// with literal ${roomId} / ${siteId} placeholders that room-service
// substitutes when building per-room tab URLs.
type AppChannelTabURL struct {
	Default string `json:"default" bson:"default"`
}

type AppSponsor struct {
	Name  string `json:"name"  bson:"name"`
	Phone string `json:"phone" bson:"phone"`
}

// RoomApp is a single entry in GetRoomAppTabsResponse.Apps — derived
// from an apps document with the per-room tabUrl substituted in.
type RoomApp struct {
	ID        string        `json:"id"                  bson:"-"`
	Name      string        `json:"name"                bson:"-"` // = apps.channelTab.name
	TabURL    string        `json:"tabUrl"              bson:"-"` // computed (scheme+host+path-prefix from SITE_URL, ${roomId}/${siteId} substituted)
	Assistant *AppAssistant `json:"assistant,omitempty" bson:"-"`
	AvatarURL string        `json:"avatarUrl,omitempty" bson:"-"`
}

// GetRoomAppTabsResponse is the response body for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.tabs RPC.
type GetRoomAppTabsResponse struct {
	Apps []RoomApp `json:"apps" bson:"-"`
}

// RoomAppAssistant is a single entry in
// GetRoomAppCommandMenuResponse.AppAssistants.
type RoomAppAssistant struct {
	AppName   string     `json:"appName"            bson:"-"` // = apps.name
	Name      string     `json:"name"               bson:"-"` // = apps.assistant.name (bot account)
	CmdBlocks []CmdBlock `json:"cmdBlocks,omitempty" bson:"-"`
}

// GetRoomAppCommandMenuResponse is the response body for the
// chat.user.{account}.request.room.{roomID}.{siteID}.app.cmd-menu RPC.
type GetRoomAppCommandMenuResponse struct {
	AppAssistants []RoomAppAssistant `json:"appAssistants" bson:"-"`
}
