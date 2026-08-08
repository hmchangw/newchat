package service

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/user-service/models"
)

// maxPriorityContacts caps the stored list. The service owns the value; the repo
// takes it as a parameter and enforces it inside the update filter.
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

// AddPriorityContact adds one contact to the caller's list, enforcing the cap inside
// the write, then fans the full post-update settings to the caller's other devices
// and to every other site.
func (s *UserService) AddPriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contact := req.ContactAccount
	if contact == "" {
		return nil, errcode.BadRequest("contactAccount is required")
	}
	if contact == account {
		return nil, errcode.BadRequest("cannot add yourself as a priority contact")
	}

	exists, err := s.priorityContactExists(c, contact)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errcode.NotFound("priority contact not found",
			errcode.WithReason(errcode.UserPriorityContactNotFound))
	}

	// One timestamp for the write and both fanouts.
	now := time.Now().UTC().UnixMilli()
	u, err := s.users.AddPriorityContact(c, account, contact, maxPriorityContacts, time.UnixMilli(now).UTC())
	if err != nil {
		return nil, fmt.Errorf("add priority contact: %w", err)
	}
	if u == nil {
		// The filter rejects "no such user" and "already at cap" alike.
		return s.resolveAddPriorityContactMiss(c, account, contact)
	}
	return s.respondPriorityContacts(c, account, u, now), nil
}

// resolveAddPriorityContactMiss disambiguates a no-match add with one re-read, on
// the failure path only. A duplicate add at exactly the cap is a no-op, not a
// violation, so it succeeds with the unchanged list.
func (s *UserService) resolveAddPriorityContactMiss(c *natsrouter.Context, account, contact string) (*models.PriorityContactsResponse, error) {
	u, err := s.users.GetUserPriorityContacts(c, account)
	if err != nil {
		return nil, fmt.Errorf("resolve priority contact add: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	current := storedPriorityContacts(u)
	for _, a := range current {
		if a == contact {
			return &models.PriorityContactsResponse{Contacts: s.enrichPriorityContacts(c, current)}, nil
		}
	}
	if len(current) >= maxPriorityContacts {
		return nil, errcode.Forbidden("priority contact limit reached",
			errcode.WithReason(errcode.UserPriorityContactLimit))
	}
	// Reachable: the write missed because the list was at cap, then a concurrent
	// RemovePriorityContact for the same account dropped some other contact before
	// this re-read, so we now see the caller's doc, no match, and a list under cap.
	// The add itself is stale, not invalid, so tell the client to retry rather than
	// falsely reporting the caller as not found.
	return nil, errcode.Conflict("priority contacts changed concurrently, retry")
}

// priorityContactExists reports whether contact names something that can send
// messages: an app for a ".bot" account, an active user otherwise. The app need only
// exist, not be Enabled — a disabled assistant makes the entry inert, not wrong, and
// requiring Enabled would break re-adding whenever an admin disables an app.
func (s *UserService) priorityContactExists(c *natsrouter.Context, contact string) (bool, error) {
	if model.IsBot(contact) {
		apps, err := s.apps.GetAppsByAssistants(c, []string{contact})
		if err != nil {
			return false, fmt.Errorf("look up priority contact app: %w", err)
		}
		return apps[contact] != nil, nil
	}
	exists, err := s.users.UserExists(c, contact)
	if err != nil {
		return false, fmt.Errorf("look up priority contact user: %w", err)
	}
	return exists, nil
}

// RemovePriorityContact removes one contact from the caller's list and fans the full
// post-update settings out. No existence check: removing an account that no longer
// exists is exactly the cleanup case to permit.
func (s *UserService) RemovePriorityContact(c *natsrouter.Context, req models.PriorityContactMutateRequest) (*models.PriorityContactsResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	contact := req.ContactAccount
	if contact == "" {
		return nil, errcode.BadRequest("contactAccount is required")
	}

	now := time.Now().UTC().UnixMilli()
	u, err := s.users.RemovePriorityContact(c, account, contact, time.UnixMilli(now).UTC())
	if err != nil {
		return nil, fmt.Errorf("remove priority contact: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	return s.respondPriorityContacts(c, account, u, now), nil
}

// respondPriorityContacts publishes both settings fanouts off the shared timestamp
// and builds the enriched reply. Shared by add and remove so neither can drift into
// publishing only the client event.
func (s *UserService) respondPriorityContacts(c *natsrouter.Context, account string, u *model.User, now int64) *models.PriorityContactsResponse {
	settings := u.Settings
	if settings == nil {
		// Reachable: $pull is a no-op on a missing path, so a matched user whose
		// document has no settings sub-document lands here. Publishing an empty
		// UserSettings would make inbox-worker's whole-object $set clear settings
		// at every remote site — there is nothing to replicate, so do not fan out.
		return &models.PriorityContactsResponse{Contacts: []models.PriorityContactItem{}}
	}
	s.publishSettingsFanouts(c, account, settings, now)
	return &models.PriorityContactsResponse{
		Contacts: s.enrichPriorityContacts(c, settings.PriorityContacts),
	}
}
