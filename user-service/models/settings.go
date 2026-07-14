package models

import (
	"encoding/json"
	"time"
)

// GetUserSettingsRequest is the body of user.settings.get.
type GetUserSettingsRequest struct{}

// SetUserSettingsRequest is the body of user.settings.set.
type SetUserSettingsRequest struct {
	Data      json.RawMessage `json:"data"`
	IfVersion *int64          `json:"ifVersion,omitempty"`
}

// UserSettingsView is the response of user.settings.get and user.settings.set.
type UserSettingsView struct {
	Account   string          `json:"account"`
	SiteID    string          `json:"siteId"`
	Data      json.RawMessage `json:"data"`
	Version   int64           `json:"version"`
	UpdatedAt time.Time       `json:"updatedAt"`
}
