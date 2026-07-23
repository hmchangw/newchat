package main

import (
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// mentionVisible reports whether a mentionee whose subscription carries
// historySharedSince may see a thread reply with parent createdAt parentCreatedAt.
// nil window = full access; a set window with a missing/older parent = no access.
func mentionVisible(historySharedSince, parentCreatedAt *time.Time) bool {
	if historySharedSince == nil {
		return true
	}
	if parentCreatedAt == nil {
		return false
	}
	return !parentCreatedAt.Before(*historySharedSince)
}

// isBot reports whether account has no live UI client and so must be skipped for
// event fan-out: real ".bot" bots and the "p_tchatadmin_" platform-admin
// pseudo-account. It routes through the model taxonomy so plain "p_" QA test
// accounts — ordinary users with a client — are NOT skipped.
func isBot(account string) bool {
	return model.IsBot(account) || model.IsPlatformAdminAccount(account)
}

// dedupedAccounts prepends sender to mentions, dropping later duplicates.
// Sender comes first so the deduped list shape is stable across the
// no-mention and mention paths regardless of whether the sender is also
// @-mentioned in the content.
func dedupedAccounts(sender string, mentions []string) []string {
	out := make([]string, 0, 1+len(mentions))
	seen := make(map[string]struct{}, 1+len(mentions))
	out = append(out, sender)
	seen[sender] = struct{}{}
	for _, a := range mentions {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}
