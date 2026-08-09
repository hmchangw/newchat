package service

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/user-service/models"
)

// sectionNameAllowedPunct is the non-alphanumeric punctuation a section name may
// contain, beyond letters/digits (any script) and single spaces.
const sectionNameAllowedPunct = "-_./()"

// GetChatlist returns the caller's stored section definitions, or the built-in
// default when the user has never customized it.
func (s *UserService) GetChatlist(c *natsrouter.Context) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	u, err := s.users.GetUserChatlist(c, account)
	if err != nil {
		return nil, fmt.Errorf("get chatlist: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	if u.Chatlist == nil {
		return model.DefaultChatlistState(), nil
	}
	return u.Chatlist, nil
}

// CreateChatlistSection adds a custom section (name validated + unique), placing
// it directly above the built-in "chats" section.
func (s *UserService) CreateChatlistSection(c *natsrouter.Context, req models.ChatlistSectionCreateRequest) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	name := strings.TrimSpace(req.Name)
	if err := validateSectionName(name); err != nil {
		return nil, err
	}
	sortMode := req.SortMode
	if sortMode == "" {
		sortMode = model.SortModeCustom
	}
	if err := validateSortMode(sortMode); err != nil {
		return nil, err
	}
	return s.mutateChatlist(c, account, func(st *model.ChatlistState) error {
		if hasCustomName(st, name, "") {
			return errcode.Conflict("section name already exists", errcode.WithReason(errcode.UserChatlistDuplicateName))
		}
		id := idgen.GenerateUUIDv7()
		st.Sections = append(st.Sections, model.ChatlistSection{
			ID: id, Name: name, BuiltIn: false, SortMode: sortMode,
		})
		insertAboveChats(st, id)
		return nil
	})
}

// DeleteChatlistSection removes a custom section; chats in it fall back to their
// derived built-in placement (orphan-tolerant — no membership cascade).
func (s *UserService) DeleteChatlistSection(c *natsrouter.Context, req models.ChatlistSectionDeleteRequest) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	return s.mutateChatlist(c, account, func(st *model.ChatlistState) error {
		sec := findSection(st, req.SectionID)
		if sec == nil {
			return errcode.NotFound("section not found", errcode.WithReason(errcode.UserChatlistSectionNotFound))
		}
		if sec.BuiltIn {
			return errcode.BadRequest("built-in sections cannot be removed", errcode.WithReason(errcode.UserChatlistBuiltinImmutable))
		}
		removeSection(st, req.SectionID)
		return nil
	})
}

// RenameChatlistSection renames a custom section (new name validated + unique).
func (s *UserService) RenameChatlistSection(c *natsrouter.Context, req models.ChatlistSectionRenameRequest) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	name := strings.TrimSpace(req.Name)
	if err := validateSectionName(name); err != nil {
		return nil, err
	}
	return s.mutateChatlist(c, account, func(st *model.ChatlistState) error {
		sec := findSection(st, req.SectionID)
		if sec == nil {
			return errcode.NotFound("section not found", errcode.WithReason(errcode.UserChatlistSectionNotFound))
		}
		if sec.BuiltIn {
			return errcode.BadRequest("built-in sections cannot be renamed", errcode.WithReason(errcode.UserChatlistBuiltinImmutable))
		}
		if hasCustomName(st, name, req.SectionID) {
			return errcode.Conflict("section name already exists", errcode.WithReason(errcode.UserChatlistDuplicateName))
		}
		sec.Name = name
		return nil
	})
}

// ReorderChatlistSections replaces the section display order. The request must be
// a permutation of every current section id (built-in + custom).
func (s *UserService) ReorderChatlistSections(c *natsrouter.Context, req models.ChatlistSectionReorderRequest) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	return s.mutateChatlist(c, account, func(st *model.ChatlistState) error {
		if !isPermutation(st.SectionOrder, req.SectionOrder) {
			return errcode.BadRequest("sectionOrder must list every section exactly once", errcode.WithReason(errcode.UserChatlistInvalidOrder))
		}
		st.SectionOrder = req.SectionOrder
		return nil
	})
}

// SetChatlistSectionSortMode sets one section's sortMode (custom|mostRecent).
// Works on any section — a built-in can be flipped to custom so the client sorts
// it by subscription.sectionOrder; only membership is off-limits on built-ins.
func (s *UserService) SetChatlistSectionSortMode(c *natsrouter.Context, req models.ChatlistSectionSetSortModeRequest) (*model.ChatlistState, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if err := validateSortMode(req.SortMode); err != nil {
		return nil, err
	}
	return s.mutateChatlist(c, account, func(st *model.ChatlistState) error {
		sec := findSection(st, req.SectionID)
		if sec == nil {
			return errcode.NotFound("section not found", errcode.WithReason(errcode.UserChatlistSectionNotFound))
		}
		sec.SortMode = req.SortMode
		return nil
	})
}

// mutateChatlist reads the caller's chatlist (default when never set), applies
// apply (which validates + mutates), persists the whole object, and fans the full
// post-update state out to the caller's other devices and other sites.
// read-modify-write is whole-object last-write-wins — matches the
// cross-site replica semantics; two concurrent same-user device edits race (rare).
// Add an optimistic version guard if that ever bites.
func (s *UserService) mutateChatlist(c *natsrouter.Context, account string, apply func(*model.ChatlistState) error) (*model.ChatlistState, error) {
	u, err := s.users.GetUserChatlist(c, account)
	if err != nil {
		return nil, fmt.Errorf("get chatlist: %w", err)
	}
	if u == nil {
		return nil, errcode.NotFound("user not found")
	}
	state := u.Chatlist
	if state == nil {
		state = model.DefaultChatlistState()
	}
	if err := apply(state); err != nil {
		return nil, err
	}
	// One timestamp for the stored HWM and both fanouts so the client event and the
	// cross-site replica agree on ordering.
	now := time.Now().UTC().UnixMilli()
	state.LastUpdatedAt = now
	updated, err := s.users.UpdateUserChatlist(c, account, state)
	if err != nil {
		return nil, fmt.Errorf("update chatlist: %w", err)
	}
	if updated == nil {
		return nil, errcode.NotFound("user not found")
	}
	result := updated.Chatlist
	if result == nil {
		result = state // unreachable after a $set of the whole object
	}
	s.publishChatlistUpdate(c, account, result, now)
	s.publishChatlistInbox(c, account, result, now)
	return result, nil
}

// publishChatlistUpdate fans the per-user chatlist.update event over core NATS
// (ephemeral client delivery, like settings.update); best-effort.
func (s *UserService) publishChatlistUpdate(c *natsrouter.Context, account string, state *model.ChatlistState, now int64) {
	data, _ := json.Marshal(model.ChatlistUpdateEvent{Timestamp: now, Chatlist: *state}) // string fields only — Marshal cannot fail
	if err := s.clientPub.Publish(c, subject.ChatlistUpdate(account), data); err != nil {
		slog.WarnContext(c, "publish chatlist update event", "error", err, "account", account, "request_id", natsutil.RequestIDFromContext(c))
	}
}

// publishChatlistInbox replicates the full post-update state to every other site's
// external INBOX lane, HWM-guarded on the replica side. Mirrors
// publishSettingsInbox; errors logged.
func (s *UserService) publishChatlistInbox(c *natsrouter.Context, account string, state *model.ChatlistState, now int64) {
	payload, _ := json.Marshal(model.UserChatlistUpdated{Account: account, Chatlist: *state, Timestamp: now}) // string fields only — Marshal cannot fail
	for _, dest := range s.allSiteIDs {
		if dest == "" || dest == s.siteID {
			continue
		}
		evt := model.InboxEvent{
			Type:       model.InboxUserChatlistUpdated,
			SiteID:     s.siteID,
			DestSiteID: dest,
			Payload:    payload,
			Timestamp:  now,
		}
		data, err := json.Marshal(evt)
		if err != nil {
			slog.WarnContext(c, "marshal chatlist inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
			continue
		}
		if err := s.pub.Publish(c, subject.InboxExternal(dest, model.InboxUserChatlistUpdated), data); err != nil {
			// Non-fatal: whole-object last-write-wins, the next mutation re-broadcasts.
			slog.WarnContext(c, "publish chatlist inbox event", "error", err, "site", s.siteID, "dest", dest, "account", account, "request_id", natsutil.RequestIDFromContext(c))
		}
	}
}

// validateSectionName enforces: 1–50 runes (after the caller trims), no
// consecutive spaces, and only letters/digits (any script), spaces, or -_./()
func validateSectionName(name string) error {
	n := utf8.RuneCountInString(name)
	if n < 1 || n > model.MaxSectionName {
		return errcode.BadRequest("section name must be 1-50 characters", errcode.WithReason(errcode.UserChatlistInvalidName))
	}
	if strings.Contains(name, "  ") {
		return errcode.BadRequest("section name has consecutive spaces", errcode.WithReason(errcode.UserChatlistInvalidName))
	}
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == ' ' || strings.ContainsRune(sectionNameAllowedPunct, r) {
			continue
		}
		return errcode.BadRequest("section name has a disallowed character", errcode.WithReason(errcode.UserChatlistInvalidName))
	}
	return nil
}

// validateSortMode rejects anything but the custom|mostRecent enum.
func validateSortMode(mode string) error {
	if mode != model.SortModeCustom && mode != model.SortModeMostRecent {
		return errcode.BadRequest("invalid sortMode", errcode.WithReason(errcode.UserChatlistInvalidSortMode))
	}
	return nil
}

// findSection returns a pointer to the section with id (mutable), or nil.
func findSection(st *model.ChatlistState, id string) *model.ChatlistSection {
	for i := range st.Sections {
		if st.Sections[i].ID == id {
			return &st.Sections[i]
		}
	}
	return nil
}

// hasCustomName reports whether a custom section other than excludeID already uses
// name (case-sensitive).
func hasCustomName(st *model.ChatlistState, name, excludeID string) bool {
	for i := range st.Sections {
		sec := &st.Sections[i]
		if !sec.BuiltIn && sec.ID != excludeID && sec.Name == name {
			return true
		}
	}
	return false
}

// insertAboveChats places id immediately before the built-in "chats" section in
// SectionOrder (appends if "chats" is somehow absent).
func insertAboveChats(st *model.ChatlistState, id string) {
	for i, sid := range st.SectionOrder {
		if sid == model.SectionChats {
			st.SectionOrder = append(st.SectionOrder[:i:i], append([]string{id}, st.SectionOrder[i:]...)...)
			return
		}
	}
	st.SectionOrder = append(st.SectionOrder, id)
}

// removeSection drops the section from both Sections and SectionOrder.
func removeSection(st *model.ChatlistState, id string) {
	secs := st.Sections[:0]
	for _, sec := range st.Sections {
		if sec.ID != id {
			secs = append(secs, sec)
		}
	}
	st.Sections = secs
	order := st.SectionOrder[:0]
	for _, sid := range st.SectionOrder {
		if sid != id {
			order = append(order, sid)
		}
	}
	st.SectionOrder = order
}

// isPermutation reports whether want lists exactly the ids in have, once each.
func isPermutation(have, want []string) bool {
	if len(have) != len(want) {
		return false
	}
	counts := make(map[string]int, len(have))
	for _, id := range have {
		counts[id]++
	}
	for _, id := range want {
		if counts[id] == 0 {
			return false
		}
		counts[id]--
	}
	return true
}
