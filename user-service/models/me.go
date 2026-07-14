package models

import "github.com/hmchangw/chat/pkg/model"

// MeResponse is the /me response: the caller's UserStatusView (embedded, flat) plus presence.
type MeResponse struct {
	UserStatusView
	Presence model.PresenceStatus `json:"presence"`
}
