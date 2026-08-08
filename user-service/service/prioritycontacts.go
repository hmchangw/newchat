package service

import (
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/user-service/models"
)

// maxPriorityContacts caps the stored list. The service owns the value; the repo
// takes it as a parameter and enforces it inside the update filter.
//
//nolint:unused // wired into AddPriorityContact by the add handler (Task 8)
const maxPriorityContacts = 30

// GetPriorityContacts returns the caller's priority-contact list, enriched for display.
func (s *UserService) GetPriorityContacts(c *natsrouter.Context) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserPriorityContacts(c, account)
	if err != nil {
		return nil, fmt.Errorf("get priority contacts: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return &models.PriorityContactsResponse{
		Contacts: s.enrichPriorityContacts(c, storedPriorityContacts(u)),
	}, nil
}

// storedPriorityContacts reads the list off a projected user doc, normalising a
// missing settings sub-document or absent field to an empty slice.
func storedPriorityContacts(u *model.User) []string {
	if u == nil || u.Settings == nil {
		return []string{}
	}
	return u.Settings.PriorityContacts
}

// enrichPriorityContacts builds display rows in stored order. Both lookups degrade
// like the thread-list enrichment: a failure logs a warn and yields a nil map, so
// rows come back with account+type and no detail object rather than failing the call.
func (s *UserService) enrichPriorityContacts(c *natsrouter.Context, contacts []string) []models.PriorityContactItem {
	out := make([]models.PriorityContactItem, 0, len(contacts))
	var userAccounts, botAccounts []string
	for _, a := range contacts {
		if model.IsBot(a) {
			botAccounts = append(botAccounts, a)
		} else {
			userAccounts = append(userAccounts, a)
		}
	}
	// Sequential, not parallel: two queries over at most maxPriorityContacts accounts.
	users := s.lookupPriorityContactUsers(c, userAccounts)
	apps := s.lookupPriorityContactApps(c, botAccounts)

	for _, a := range contacts {
		item := models.PriorityContactItem{Account: a, Type: models.PriorityContactTypeUser}
		if model.IsBot(a) {
			item.Type = models.PriorityContactTypeBot
			if app, ok := apps[a]; ok && app != nil {
				item.App = &models.PriorityContactApp{Name: app.Name}
			}
		} else if cu, ok := users[a]; ok && cu != nil {
			item.User = cu
		}
		out = append(out, item)
	}
	return out
}

// lookupPriorityContactUsers fetches display fields for the regular-user contacts;
// a failure or empty set degrades to nil (rows render without a user object).
func (s *UserService) lookupPriorityContactUsers(c *natsrouter.Context, accounts []string) map[string]*models.PriorityContactUser {
	if len(accounts) == 0 {
		return nil
	}
	got, err := s.users.GetPriorityContactUsers(c, accounts)
	if err != nil {
		slog.WarnContext(c, "priority contact user lookup degraded", "account", c.Param("account"),
			"request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return got
}

// lookupPriorityContactApps fetches app docs for the bot contacts; a failure or empty
// set degrades to nil (rows render without an app object).
func (s *UserService) lookupPriorityContactApps(c *natsrouter.Context, bots []string) map[string]*model.App {
	if len(bots) == 0 {
		return nil
	}
	got, err := s.apps.GetAppsByAssistants(c, bots)
	if err != nil {
		slog.WarnContext(c, "priority contact app lookup degraded", "account", c.Param("account"),
			"request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return got
}
