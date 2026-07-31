package model

// Enum values for the two string-enum settings. The server rejects any other
// value on write; an unset field falls back to the client default.
const (
	ThemePreferenceSystem = "system"
	ThemePreferenceLight  = "light"
	ThemePreferenceDark   = "dark"

	InitialChatScrollLastRead = "lastRead"
	InitialChatScrollNewest   = "newest"
)

// UserSettings is the per-user client-preferences sub-document on the user
// record. Every field is optional: absent means the client applies its own
// default — the server stores only what the user explicitly set and never
// injects defaults.
type UserSettings struct {
	FullWidth                               *bool   `json:"fullWidth,omitempty"                               bson:"fullWidth,omitempty"`
	ThemePreference                         *string `json:"themePreference,omitempty"                         bson:"themePreference,omitempty"`
	TranslateMessageInto                    *string `json:"translateMessageInto,omitempty"                    bson:"translateMessageInto,omitempty"`
	MessagePreviewEnabled                   *bool   `json:"messagePreviewEnabled,omitempty"                   bson:"messagePreviewEnabled,omitempty"`
	MuteAllNotifications                    *bool   `json:"muteAllNotifications,omitempty"                    bson:"muteAllNotifications,omitempty"`
	ShowMessagesAndPreviewsInNotifications  *bool   `json:"showMessagesAndPreviewsInNotifications,omitempty"  bson:"showMessagesAndPreviewsInNotifications,omitempty"`
	ShowNotificationsDuringCallsAndMeetings *bool   `json:"showNotificationsDuringCallsAndMeetings,omitempty" bson:"showNotificationsDuringCallsAndMeetings,omitempty"`
	InitialChatScrollPosition               *string `json:"initialChatScrollPosition,omitempty"               bson:"initialChatScrollPosition,omitempty"`
}

// IsEmpty reports whether no field is set — the "nothing to write" guard for
// partial updates.
func (s *UserSettings) IsEmpty() bool {
	return s.FullWidth == nil && s.ThemePreference == nil && s.TranslateMessageInto == nil &&
		s.MessagePreviewEnabled == nil && s.MuteAllNotifications == nil &&
		s.ShowMessagesAndPreviewsInNotifications == nil &&
		s.ShowNotificationsDuringCallsAndMeetings == nil && s.InitialChatScrollPosition == nil
}

// SettingsUpdateEvent is the client-facing settings.update fanout payload.
// It carries the full post-update settings so other devices replace their
// copy rather than merge deltas. Timestamp is the publish time (ms).
type SettingsUpdateEvent struct {
	Timestamp int64        `json:"timestamp"`
	Settings  UserSettings `json:"settings"`
}
