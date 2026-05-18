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
