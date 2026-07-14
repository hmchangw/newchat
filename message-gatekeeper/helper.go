package main

import (
	"fmt"
	"regexp"
	"strings"
)

// messageLink builds the canonical deep link to a message from trusted inputs.
// Single source of truth for the link format, shared by the authoritative
// history fetch (fetcher_history.go) and the degraded-mode placeholder snapshot
// (handler.go) so the two paths can't drift. baseURL is operator-supplied
// (CHAT_BASE_URL); its trailing slash is trimmed so the link never doubles up.
func messageLink(baseURL, roomID, messageID string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(baseURL, "/"), roomID, messageID)
}

// botPattern matches account names treated as bots. Mirrors
// room-service/helper.go:32. Promotion to a shared pkg/botid is a future
// cleanup — keep both copies in sync if this regex changes here, since the
// other copy is owned by a separate developer.
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

// isBot returns true if an account name matches the bot naming pattern
// (suffix `.bot` or prefix `p_`).
func isBot(account string) bool { return botPattern.MatchString(account) }
