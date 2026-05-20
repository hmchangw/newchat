package main

import "strings"

// isBot returns true if account follows the bot naming convention used across
// the codebase (suffix `.bot` or prefix `p_`). Mirrors the predicate in
// message-gatekeeper/helper.go and room-service/helper.go — promoting to a
// shared pkg/botid is a future cleanup; keep these copies in sync if the
// convention changes.
func isBot(account string) bool {
	return strings.HasSuffix(account, ".bot") || strings.HasPrefix(account, "p_")
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
