package main

import "regexp"

// botPattern matches account names treated as bots. Mirrors
// room-service/helper.go:32. Promotion to a shared pkg/botid is a future
// cleanup — keep both copies in sync if this regex changes here, since the
// other copy is owned by a separate developer.
var botPattern = regexp.MustCompile(`\.bot$|^p_`)

// isBot returns true if an account name matches the bot naming pattern
// (suffix `.bot` or prefix `p_`).
func isBot(account string) bool { return botPattern.MatchString(account) }
