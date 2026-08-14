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

// GetSettings returns exactly the stored settings sub-document — {} when never set, the
// server never injects defaults (absent = client-defined default) — plus the evaluated
// admin-managed permissions: every known key, always present; no snapshot means false.
func (s *UserService) GetSettings(c *natsrouter.Context) (*models.SettingsGetResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserSettings(c, account)
	if err != nil {
		return nil, fmt.Errorf("get settings: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	var settings model.UserSettings
	if u.Settings != nil {
		settings = *u.Settings
	}
	return &models.SettingsGetResponse{
		UserSettings: settings,
		Permissions:  u.Permissions.Evaluated(time.Now().UTC()),
	}, nil
}

// SetSettings partially updates the caller's settings — only the fields sent
// are written — then fans out settings.update with the full post-update
// settings so the caller's other devices sync live.
//
// hugeParam below: req crossed gocritic's size threshold only because
// model.UserSettings grew a PriorityContacts field this handler never reads,
// and natsrouter.Register requires a value (non-pointer) request type, as
// used by every other handler in this file.
//
//nolint:gocritic
func (s *UserService) SetSettings(c *natsrouter.Context, req models.SettingsSetRequest) (*model.UserSettings, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if err := validateSettings(&req.UserSettings); err != nil {
		return nil, err
	}
	// One timestamp for the write and both fanouts, so the stored high-water mark,
	// the client event, and the cross-site replica all agree on ordering.
	now := time.Now().UTC().UnixMilli()
	u, err := s.users.UpdateUserSettings(c, account, &req.UserSettings, time.UnixMilli(now).UTC())
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
	s.publishSettingsFanouts(c, account, settings, now)
	return settings, nil
}

// validateSettings rejects an empty request (nothing to write), a malformed
// translateMessageInto, and an out-of-enum themePreference / initialChatScrollPosition.
func validateSettings(set *model.UserSettings) error {
	if set.IsEmpty() {
		return errcode.BadRequest("no settings provided")
	}
	// "" is valid: it explicitly stores "translation off" (an unset would fall
	// back to the client default, erasing the user's choice).
	if set.TranslateMessageInto != nil && *set.TranslateMessageInto != "" && !translateTagRe.MatchString(*set.TranslateMessageInto) {
		return errcode.BadRequest("invalid translateMessageInto language tag")
	}
	if set.ThemePreference != nil &&
		*set.ThemePreference != model.ThemePreferenceSystem &&
		*set.ThemePreference != model.ThemePreferenceLight &&
		*set.ThemePreference != model.ThemePreferenceDark {
		return errcode.BadRequest("invalid themePreference")
	}
	if set.InitialChatScrollPosition != nil &&
		*set.InitialChatScrollPosition != model.InitialChatScrollLastRead &&
		*set.InitialChatScrollPosition != model.InitialChatScrollNewest {
		return errcode.BadRequest("invalid initialChatScrollPosition")
	}
	return nil
}

// publishSettingsFanouts emits both settings fanouts off one timestamp: the client
// event for the caller's other devices, and the cross-site inbox replica that every
// site's notification-worker reads locally. Both carry the FULL settings
// sub-document — the cross-site apply is a whole-object $set, so a partial payload
// wipes unrelated settings at remote sites. Every settings mutation must call this,
// not just publishSettingsUpdate: a client-only fanout leaves remote sites stale.
func (s *UserService) publishSettingsFanouts(c *natsrouter.Context, account string, settings *model.UserSettings, now int64) {
	s.publishSettingsUpdate(c, account, settings, now)
	s.publishSettingsInbox(c, account, settings, now)
}

// publishSettingsUpdate fans out the per-user settings.update event over core
// NATS (ephemeral client delivery, like subscription.update); best-effort —
// errors are logged, the next set re-broadcasts the full settings.
func (s *UserService) publishSettingsUpdate(c *natsrouter.Context, account string, settings *model.UserSettings, now int64) {
	data, _ := json.Marshal(model.SettingsUpdateEvent{
		Timestamp: now,
		Settings:  *settings,
	}) // UserSettings is all primitives — Marshal cannot fail
	if err := s.clientPub.Publish(c, subject.SettingsUpdate(account), data); err != nil {
		slog.WarnContext(c, "publish settings update event", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
	}
}

// publishSettingsInbox replicates the full post-update settings to every other
// site's external INBOX lane, so each site's notification worker can decide
// locally whether to push to this user. Mirrors publishStatus; errors logged.
func (s *UserService) publishSettingsInbox(c *natsrouter.Context, account string, settings *model.UserSettings, now int64) {
	payload, _ := json.Marshal(model.UserSettingsUpdated{
		Account:   account,
		Settings:  *settings,
		Timestamp: now,
	}) // UserSettings is all primitives — Marshal cannot fail
	for _, dest := range s.allSiteIDs {
		if dest == "" || dest == s.siteID {
			continue
		}
		evt := model.InboxEvent{
			Type:       model.InboxUserSettingsUpdated,
			SiteID:     s.siteID,
			DestSiteID: dest,
			Payload:    payload,
			Timestamp:  now,
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.WarnContext(c, "marshal settings inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
			continue
		}
		if err := s.pub.Publish(c, subject.InboxExternal(dest, model.InboxUserSettingsUpdated), data); err != nil {
			// Non-fatal: settings are last-write-wins, the next set re-broadcasts.
			slog.WarnContext(c, "publish settings inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
		}
	}
}
