package model

// UserSettings is the per-user client-preferences sub-document on the user
// record. Every field is optional: absent means the client applies its own
// default — the server stores only what the user explicitly set and never
// injects defaults.
type UserSettings struct {
	FullWidth                               *bool   `json:"fullWidth,omitempty"                               bson:"fullWidth,omitempty"`
	TranslateMessageInto                    *string `json:"translateMessageInto,omitempty"                    bson:"translateMessageInto,omitempty"`
	ShowMessagePreviewInSidebarList         *bool   `json:"showMessagePreviewInSidebarList,omitempty"         bson:"showMessagePreviewInSidebarList,omitempty"`
	MuteAllNotifications                    *bool   `json:"muteAllNotifications,omitempty"                    bson:"muteAllNotifications,omitempty"`
	ShowMessagesAndPreviewsInNotifications  *bool   `json:"showMessagesAndPreviewsInNotifications,omitempty"  bson:"showMessagesAndPreviewsInNotifications,omitempty"`
	ShowNotificationsDuringCallsAndMeetings *bool   `json:"showNotificationsDuringCallsAndMeetings,omitempty" bson:"showNotificationsDuringCallsAndMeetings,omitempty"`
	ScrollToBottomInChat                    *bool   `json:"scrollToBottomInChat,omitempty"                    bson:"scrollToBottomInChat,omitempty"`
}

// IsEmpty reports whether no field is set — the "nothing to write" guard for
// partial updates.
func (s *UserSettings) IsEmpty() bool {
	return s.FullWidth == nil && s.TranslateMessageInto == nil &&
		s.ShowMessagePreviewInSidebarList == nil && s.MuteAllNotifications == nil &&
		s.ShowMessagesAndPreviewsInNotifications == nil &&
		s.ShowNotificationsDuringCallsAndMeetings == nil && s.ScrollToBottomInChat == nil
}

// SettingsUpdateEvent is the client-facing settings.update fanout payload.
// It carries the full post-update settings so other devices replace their
// copy rather than merge deltas. Timestamp is the publish time (ms).
type SettingsUpdateEvent struct {
	Timestamp int64        `json:"timestamp"`
	Settings  UserSettings `json:"settings"`
}
