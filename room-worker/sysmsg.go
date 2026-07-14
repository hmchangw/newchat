package main

import (
	"strconv"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

func displayName(u *model.User) string {
	return displayfmt.CombineWithFallback(u.EngName, u.ChineseName, u.Account)
}

func displayOrg(name, tcName, orgID string) string {
	return displayfmt.CombineWithFallback(name, tcName, orgID)
}

func quoted(name string) string {
	return "\"" + name + "\""
}

// plural renders "1 singular" when n == 1, otherwise "N pluralForm".
func plural(n int, singular, pluralForm string) string {
	if n == 1 {
		return "1 " + singular
	}
	return strconv.Itoa(n) + " " + pluralForm
}

func formatAddedSingle(requester, added *model.User) string {
	return quoted(displayName(requester)) + " added " + quoted(displayName(added)) + " to the chatroom"
}

// formatAddedCounts renders the count form. At least one of people/orgs is > 0
// (callers skip emitting the message when both are zero).
func formatAddedCounts(requester *model.User, people, orgs int) string {
	who := quoted(displayName(requester))
	switch {
	case people > 0 && orgs > 0:
		return who + " added " + plural(people, "person", "people") +
			" and " + plural(orgs, "organization", "organizations") + " to the chatroom"
	case orgs > 0:
		return who + " added " + plural(orgs, "organization", "organizations") + " to the chatroom"
	default:
		return who + " added " + plural(people, "person", "people") + " to the chatroom"
	}
}

// addedContent renders members_added Content for both add and create: named form
// for a lone individual (no orgs), else counts. lookup returns nil if unresolved.
func addedContent(requester *model.User, individuals, orgs []string, lookup func(string) *model.User) string {
	if len(individuals) == 1 && len(orgs) == 0 {
		if u := lookup(individuals[0]); u != nil {
			return formatAddedSingle(requester, u)
		}
	}
	return formatAddedCounts(requester, len(individuals), len(orgs))
}

// nonNil returns s, or a non-nil empty slice so JSON marshals "[]" not "null".
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// withoutAccount filters account out of accounts — the requester can ride into
// req.Users via channel-ref expansion and must not count in their own sys-msg.
func withoutAccount(accounts []string, account string) []string {
	out := make([]string, 0, len(accounts))
	for _, a := range accounts {
		if a != account {
			out = append(out, a)
		}
	}
	return out
}

func formatRemovedUser(requester, removed *model.User) string {
	return quoted(displayName(requester)) + " removed " + quoted(displayName(removed)) + " from the chatroom"
}

func formatRemovedOrg(requester *model.User, name, tcName, orgID string) string {
	return quoted(displayName(requester)) + " removed " + quoted(displayOrg(name, tcName, orgID)) + " from the chatroom"
}

func formatLeft(user *model.User) string {
	return quoted(displayName(user)) + " left the chatroom"
}
