package models

import "github.com/hmchangw/chat/pkg/model"

// SettingsSetRequest is the body of settings.set — a partial update: only the
// fields present in the request are written; at least one is required.
// The embedded UserSettings inlines the seven optional fields.
type SettingsSetRequest struct {
	model.UserSettings
}

// SettingsGetResponse is the settings.get reply: the user's settings plus the evaluated
// admin-managed permissions. The permissions field is read-only on this surface — it is
// not part of UserSettings, so settings.set structurally cannot touch it.
type SettingsGetResponse struct {
	model.UserSettings
	Permissions map[model.PermissionKey]bool `json:"permissions"`
}
