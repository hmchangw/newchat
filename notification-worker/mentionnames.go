package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

// defaultMentionNamesTimeout bounds the lookup so a slow users collection cannot
// hold up the fan-out; the push still goes out with raw @tokens.
const defaultMentionNamesTimeout = 2 * time.Second

// MentionNameResolver maps mentioned accounts to their render-ready display
// names for the push body. Errors are advisory — the handler falls open and
// sends the unsubstituted content.
type MentionNameResolver interface {
	Resolve(ctx context.Context, accounts []string) (map[string]string, error)
}

// UserAccountFinder is the userstore surface this resolver needs; both
// userstore.Cache and the raw Mongo store satisfy it.
type UserAccountFinder interface {
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
}

// userMentionNames resolves display names from the users collection, composing
// them with the same engName/chineseName rule message-gatekeeper uses for the
// sender's UserDisplayName.
type userMentionNames struct {
	users   UserAccountFinder
	timeout time.Duration
}

func newUserMentionNames(users UserAccountFinder, timeout time.Duration) *userMentionNames {
	if timeout <= 0 {
		timeout = defaultMentionNamesTimeout
	}
	return &userMentionNames{users: users, timeout: timeout}
}

// Resolve returns display names keyed by lowercased account. A user with
// neither name is omitted rather than mapped to their account, so
// mention.ReplaceAccounts keeps the "@account" marker instead of degrading it to
// bare text. Names collected before an error are still returned, so a partial
// read substitutes what it can.
func (r *userMentionNames) Resolve(ctx context.Context, accounts []string) (map[string]string, error) {
	out := make(map[string]string, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	// Bound this dependency rather than inheriting the consumer's deadline.
	qctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	users, err := r.users.FindUsersByAccounts(qctx, accounts)
	for i := range users {
		// Empty fallback: an unnamed user yields "" and keeps its raw @token.
		name := displayfmt.CombineWithFallback(users[i].EngName, users[i].ChineseName, "")
		if name == "" {
			continue
		}
		out[strings.ToLower(users[i].Account)] = name
	}
	if err != nil {
		return out, fmt.Errorf("find mentioned users: %w", err)
	}
	return out, nil
}
