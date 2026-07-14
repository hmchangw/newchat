package main

import (
	"strings"

	"github.com/hmchangw/chat/pkg/subject"
)

// Presence write subjects are exposed by pkg/subject only as natsrouter
// patterns carrying a literal "{account}" token. The loadgen publishes on
// concrete subjects, so it substitutes the synthetic account into the pattern.
// Building on top of the pattern builders keeps the structure single-sourced.

func concretePresenceSubject(pattern, account string) string {
	return strings.Replace(pattern, "{account}", account, 1)
}

func presenceHelloSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceHelloPattern(siteID), account)
}

func presencePingSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresencePingPattern(siteID), account)
}

func presenceActivitySubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceActivityPattern(siteID), account)
}

func presenceByeSubject(account, siteID string) string {
	return concretePresenceSubject(subject.PresenceByePattern(siteID), account)
}
