package models

import "github.com/hmchangw/chat/pkg/model"

// SettingsSetRequest is the body of settings.set — a partial update: only the
// fields present in the request are written; at least one is required.
// The embedded UserSettings inlines the seven optional fields.
type SettingsSetRequest struct {
	model.UserSettings
}
