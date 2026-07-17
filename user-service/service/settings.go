package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

// translateTagRe is a permissive language-tag shape (BCP-47-ish): hyphenated
// letter/digit subtags, leading subtag letters-only. No whitelist by design.
var translateTagRe = regexp.MustCompile(`^[A-Za-z]+(-[A-Za-z0-9]+)*$`)

// GetSettings returns exactly the stored settings sub-document; {} when never
// set — the server never injects defaults (absent = client-defined default).
func (s *UserService) GetSettings(c *natsrouter.Context) (*model.UserSettings, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserSettings(c, account)
	if err != nil {
		return nil, fmt.Errorf("get settings: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	if u.Settings == nil {
		return &model.UserSettings{}, nil
	}
	return u.Settings, nil
}

// SetSettings partially updates the caller's settings — only the fields sent
// are written — then fans out settings.update with the full post-update
// settings so the caller's other devices sync live.
func (s *UserService) SetSettings(c *natsrouter.Context, req models.SettingsSetRequest) (*model.UserSettings, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if err := validateSettings(&req.UserSettings); err != nil {
		return nil, err
	}
	u, err := s.users.UpdateUserSettings(c, account, &req.UserSettings)
	if err != nil {
		return nil, fmt.Errorf("set settings: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	settings := u.Settings
	if settings == nil {
		// Unreachable after a non-empty $set; keep the reply shape total.
		settings = &model.UserSettings{}
	}
	s.publishSettingsUpdate(c, account, settings)
	return settings, nil
}

// validateSettings rejects an empty request (nothing to write) and a
// malformed translateMessageInto.
func validateSettings(set *model.UserSettings) error {
	if set.IsEmpty() {
		return errcode.BadRequest("no settings provided")
	}
	if set.TranslateMessageInto != nil && !translateTagRe.MatchString(*set.TranslateMessageInto) {
		return errcode.BadRequest("invalid translateMessageInto language tag")
	}
	return nil
}

// publishSettingsUpdate fans out the per-user settings.update event over core
// NATS (ephemeral client delivery, like subscription.update); best-effort —
// errors are logged, the next set re-broadcasts the full settings.
func (s *UserService) publishSettingsUpdate(c *natsrouter.Context, account string, settings *model.UserSettings) {
	data, _ := json.Marshal(model.SettingsUpdateEvent{
		Timestamp: time.Now().UTC().UnixMilli(),
		Settings:  *settings,
	}) // UserSettings is all primitives — Marshal cannot fail
	if err := s.clientPub.Publish(c, subject.SettingsUpdate(account), data); err != nil {
		slog.WarnContext(c, "publish settings update event", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
	}
}
