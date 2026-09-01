package service

import (
	"context"
	"log/slog"
	"strings"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

const (
	// legacyMembersRemovedType is the migrated system-message type (plural
	// "members_"), distinct from the modern model.MessageTypeMemberRemoved.
	// Rows of this type carry a raw account in their text, not in sysMsgData.
	legacyMembersRemovedType = "members_removed"
	// legacyMembersLeftType is the migrated self-leave type. Nothing in this repo
	// writes it — only the migration did — so it has no pkg/model constant.
	legacyMembersLeftType = "members_left"
	// removedFromChannelSuffix must match exactly, trailing period included: a
	// user can type the same sentence without it, and the period plus the type
	// gate is what keeps an ordinary message from being rewritten.
	removedFromChannelSuffix = " has been removed from the channel."
)

// legacySysMsgTypes maps each migrated system-message type to the modern one
// clients understand. Keyed by the constants above and valued by pkg/model's, so
// a rename on either side is a compile error rather than silent drift.
var legacySysMsgTypes = map[string]string{
	legacyMembersRemovedType: model.MessageTypeMemberRemoved,
	legacyMembersLeftType:    model.MessageTypeMemberLeft,
}

// normalizeLegacySysMsgTypes rewrites migrated types in place, on the wire only —
// the Cassandra rows keep their legacy form.
//
// Unconditional by design: a members_removed row whose text never matched the
// name-resolution suffix still gets the type its client expects.
func normalizeLegacySysMsgTypes(msgs []models.Message) {
	for i := range msgs {
		if modern, ok := legacySysMsgTypes[msgs[i].Type]; ok {
			msgs[i].Type = modern
		}
	}
}

// extractRemovedAccount returns the account baked into a legacy members_removed
// row's text, which quotes it: `"bob" has been removed from the channel.`
// Reports false for every other row, leaving it untouched.
//
// The quotes are part of the stored sentence, not delimiters we may keep: an
// account carrying them matches no user document, so the row would silently go
// unresolved.
func extractRemovedAccount(m *models.Message) (string, bool) {
	if m == nil || m.Type != legacyMembersRemovedType {
		return "", false
	}
	quoted, found := strings.CutSuffix(m.Msg, removedFromChannelSuffix)
	if !found {
		return "", false
	}
	// Needs both quotes plus at least one character between them; `""` and a
	// lone `"` name nobody. An inner quote belongs to the account itself.
	if len(quoted) < 3 || !strings.HasPrefix(quoted, `"`) || !strings.HasSuffix(quoted, `"`) {
		return "", false
	}
	return quoted[1 : len(quoted)-1], true
}

// quoteRemoved renders the sentence back with name where the account stood,
// keeping the quotes the stored form uses.
func quoteRemoved(name string) string {
	return `"` + name + `"` + removedFromChannelSuffix
}

// removedMemberName renders one account's display name, preferring a bot's
// registered app name over the composed one — the precedence room-worker applies
// on the live path, so a legacy row and a modern one show a bot alike.
//
// An empty return means "nothing resolved": leave the row exactly as stored.
func (s *HistoryService) removedMemberName(ctx context.Context, account string, u *model.User) string {
	var engName, chineseName string
	if u != nil {
		engName, chineseName = u.EngName, u.ChineseName
	}
	if model.IsBot(account) {
		return s.botAwareDisplayName(ctx, engName, chineseName, account)
	}
	if u == nil {
		return ""
	}
	return displayfmt.CombineWithFallback(engName, chineseName, account)
}

// resolveRemovedMemberNames rewrites every legacy members_removed row in msgs,
// swapping the raw account in its text for the member's display name.
//
// One batched read serves the whole page however many rows qualify, and a page
// with none — the overwhelmingly common case — returns before touching Mongo. Bot
// accounts stay in that same batch rather than earning a query of their own: a bot
// may carry a user document, and the batch is issued either way.
//
// Best-effort throughout: a failed lookup, or an account matching neither a user
// nor an app, leaves the row reading exactly as it does today. A display name is
// never worth failing a history load over.
func (s *HistoryService) resolveRemovedMemberNames(ctx context.Context, msgs []models.Message) {
	if len(msgs) == 0 {
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

	// A users failure is logged and fallen through, not returned: it says nothing
	// about `apps`, so a bot on this page can still resolve. Non-bot rows end up
	// untouched either way, which is the same guarantee an early return gave.
	var docs map[string]*model.User
	if s.users != nil {
		users, err := s.users.FindUsersByAccounts(ctx, accounts)
		if err != nil {
			slog.WarnContext(ctx, "resolving removed-member display names, leaving raw accounts",
				"accounts", len(accounts), "error", err)
		}
		docs = make(map[string]*model.User, len(users))
		for i := range users {
			docs[users[i].Account] = &users[i]
		}
	}

	names := make(map[string]string, len(accounts))
	for _, account := range accounts {
		if name := s.removedMemberName(ctx, account, docs[account]); name != "" {
			names[account] = name
		}
	}

	for i := range msgs {
		account, ok := extractRemovedAccount(&msgs[i])
		if !ok {
			continue
		}
		if name, found := names[account]; found {
			msgs[i].Msg = quoteRemoved(name)
		}
	}
}

// normalizeLegacySysMsgs renders migrated system-message rows the way clients
// expect them: display names in the text, modern types on the wire.
//
// The order is load-bearing. extractRemovedAccount gates on the LEGACY plural
// type, so normalizing first would leave every legacy sentence stuck with its raw
// account. Both passes live here so no call site can drift them apart.
func (s *HistoryService) normalizeLegacySysMsgs(ctx context.Context, msgs []models.Message) {
	s.resolveRemovedMemberNames(ctx, msgs)
	normalizeLegacySysMsgTypes(msgs)
}

// normalizeLegacySysMsg is the one-message form, for the handlers that return a
// single row rather than a page.
func (s *HistoryService) normalizeLegacySysMsg(ctx context.Context, m *models.Message) {
	if m == nil {
		return
	}
	one := []models.Message{*m}
	s.normalizeLegacySysMsgs(ctx, one)
	*m = one[0]
}
