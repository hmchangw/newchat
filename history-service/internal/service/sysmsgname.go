package service

import (
	"context"
	"log/slog"
	"strings"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/displayfmt"
)

const (
	// legacyMembersRemovedType is the migrated system-message type (plural
	// "members_"), distinct from the modern model.MessageTypeMemberRemoved.
	// Rows of this type carry a raw account in their text, not in sysMsgData.
	legacyMembersRemovedType = "members_removed"
	// removedFromChannelSuffix must match exactly, trailing period included: a
	// user can type the same sentence without it, and the period plus the type
	// gate is what keeps an ordinary message from being rewritten.
	removedFromChannelSuffix = " has been removed from the channel."
)

// extractRemovedAccount returns the account baked into a legacy members_removed
// row's text. Reports false for every other row, leaving it untouched.
func extractRemovedAccount(m *models.Message) (string, bool) {
	if m == nil || m.Type != legacyMembersRemovedType {
		return "", false
	}
	account, found := strings.CutSuffix(m.Msg, removedFromChannelSuffix)
	if !found || account == "" {
		return "", false
	}
	return account, true
}

// resolveRemovedMemberNames rewrites every legacy members_removed row in msgs,
// swapping the raw account in its text for the user's display name.
//
// One batched read serves the whole page however many rows qualify, and a page
// with none — the overwhelmingly common case — returns before touching Mongo.
//
// Best-effort throughout: a failed lookup or an account with no user document
// leaves the row reading exactly as it does today. A display name is never worth
// failing a history load over.
func (s *HistoryService) resolveRemovedMemberNames(ctx context.Context, msgs []models.Message) {
	if len(msgs) == 0 || s.users == nil {
		return
	}

	accounts := make([]string, 0, len(msgs))
	seen := make(map[string]struct{}, len(msgs))
	for i := range msgs {
		account, ok := extractRemovedAccount(&msgs[i])
		if !ok {
			continue
		}
		if _, dup := seen[account]; dup {
			continue
		}
		seen[account] = struct{}{}
		accounts = append(accounts, account)
	}
	if len(accounts) == 0 {
		return
	}

	users, err := s.users.FindUsersByAccounts(ctx, accounts)
	if err != nil {
		slog.WarnContext(ctx, "resolving removed-member display names, leaving raw accounts",
			"accounts", len(accounts), "error", err)
		return
	}

	names := make(map[string]string, len(users))
	for i := range users {
		u := &users[i]
		names[u.Account] = displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account)
	}

	for i := range msgs {
		account, ok := extractRemovedAccount(&msgs[i])
		if !ok {
			continue
		}
		if name, found := names[account]; found {
			msgs[i].Msg = name + removedFromChannelSuffix
		}
	}
}

// resolveRemovedMemberName is the one-message form, for the handlers that return
// a single row rather than a page.
func (s *HistoryService) resolveRemovedMemberName(ctx context.Context, m *models.Message) {
	if m == nil {
		return
	}
	one := []models.Message{*m}
	s.resolveRemovedMemberNames(ctx, one)
	*m = one[0]
}
