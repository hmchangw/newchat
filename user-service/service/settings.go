package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/user-service/models"
)

const defaultMaxSettingsBytes = 64 * 1024

// GetUserSettings returns the caller's settings for this service's site.
func (s *UserService) GetUserSettings(c *natsrouter.Context) (*models.UserSettingsView, error) {
	account := c.Param("account")
	if account == "" {
		return nil, errcode.Forbidden("invalid account")
	}
	c.WithLogValues("account", account)

	settings, err := s.settings.GetUserSettings(c, account, s.siteID)
	if err != nil {
		return nil, wrapSettingsError("get user settings", err)
	}
	if settings == nil {
		return nil, errcode.NotFound("user settings not found")
	}
	return userSettingsView(settings), nil
}

// SetUserSettings validates and persists the caller's opaque JSON settings.
func (s *UserService) SetUserSettings(c *natsrouter.Context, req models.SetUserSettingsRequest) (*models.UserSettingsView, error) {
	account := c.Param("account")
	if account == "" {
		return nil, errcode.Forbidden("invalid account")
	}
	c.WithLogValues("account", account)

	if len(req.Data) > s.maxSettingsBytes {
		return nil, errcode.BadRequest("data too large")
	}
	if !isJSONObject(req.Data) {
		return nil, errcode.BadRequest("data must be a JSON object")
	}

	settings, err := s.settings.SetUserSettings(c, account, s.siteID, req.Data, req.IfVersion)
	if err != nil {
		return nil, wrapSettingsError("set user settings", err)
	}
	if settings == nil {
		return nil, fmt.Errorf("set user settings: repository returned nil settings")
	}
	return userSettingsView(settings), nil
}

func isJSONObject(data json.RawMessage) bool {
	var object map[string]json.RawMessage
	return len(data) > 0 && json.Unmarshal(data, &object) == nil && object != nil
}

func userSettingsView(settings *model.UserSettings) *models.UserSettingsView {
	return &models.UserSettingsView{
		Account:   settings.Account,
		SiteID:    settings.SiteID,
		Data:      settings.Data,
		Version:   settings.Version,
		UpdatedAt: settings.UpdatedAt,
	}
}

func wrapSettingsError(_ string, err error) error {
	var coded *errcode.Error
	if errors.As(err, &coded) {
		return err
	}
	return err
}
