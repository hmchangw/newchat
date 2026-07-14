package model

// App is a read-only view of the apps collection (provisioning is upstream).
type App struct {
	ID            string            `json:"id"                      bson:"_id"`
	Name          string            `json:"name"                    bson:"name"`
	Description   string            `json:"description,omitempty"   bson:"description,omitempty"`
	AppViewURL    map[string]string `json:"appViewUrl,omitempty"    bson:"appViewUrl,omitempty"`
	ReportURL     string            `json:"reportUrl,omitempty"     bson:"reportUrl,omitempty"`
	ForumURL      string            `json:"forumUrl,omitempty"      bson:"forumUrl,omitempty"`
	UserManualURL string            `json:"userManualUrl,omitempty" bson:"userManualUrl,omitempty"`
	Version       string            `json:"version,omitempty"       bson:"version,omitempty"`
	Assistant     *AppAssistant     `json:"assistant,omitempty"     bson:"assistant,omitempty"`
	ChannelTab    *AppChannelTab    `json:"channelTab,omitempty"    bson:"channelTab,omitempty"`
	Sponsors      []AppSponsor      `json:"sponsors,omitempty"      bson:"sponsors,omitempty"`
}

// AppAssistant: Name is the bot user account (".bot" suffix); botDM requires Enabled==true.
type AppAssistant struct {
	Enabled     bool   `json:"enabled"               bson:"enabled"`
	Name        string `json:"name"                  bson:"name"`
	Username    string `json:"username,omitempty"    bson:"username,omitempty"`
	SettingsURL string `json:"settingsUrl,omitempty" bson:"settingsUrl,omitempty"`
}

// AppSubscription is the app-metadata object nested under the `app` key on a
// botDM subscription row. Wire-only — never decoded from Mongo (bson:"-"). Its
// Name is the app's display name (the base Subscription.Name carries it too).
type AppSubscription struct {
	AppID         string            `json:"appId,omitempty"         bson:"-"` // = App.ID
	Name          string            `json:"name,omitempty"          bson:"-"` // = App.Name
	Description   string            `json:"description,omitempty"   bson:"-"`
	Assistant     *AppAssistant     `json:"assistant,omitempty"     bson:"-"`
	AppViewURL    map[string]string `json:"appViewUrl,omitempty"    bson:"-"`
	ReportURL     string            `json:"reportUrl,omitempty"     bson:"-"`
	ForumURL      string            `json:"forumUrl,omitempty"      bson:"-"`
	UserManualURL string            `json:"userManualUrl,omitempty" bson:"-"`
	Version       string            `json:"version,omitempty"       bson:"-"`
	Sponsors      []AppSponsor      `json:"sponsors,omitempty"      bson:"-"`
}

// AppSubscriptionFromApp builds the botDM `app` object from a full app document (AppID=a.ID).
func AppSubscriptionFromApp(a *App) *AppSubscription {
	return &AppSubscription{
		AppID:         a.ID,
		Name:          a.Name,
		Description:   a.Description,
		Assistant:     a.Assistant,
		AppViewURL:    a.AppViewURL,
		ReportURL:     a.ReportURL,
		ForumURL:      a.ForumURL,
		UserManualURL: a.UserManualURL,
		Version:       a.Version,
		Sponsors:      a.Sponsors,
	}
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
