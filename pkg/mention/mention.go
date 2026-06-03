package mention

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/hmchangw/chat/pkg/model"
)

// mentionRe matches @mention tokens in message content. The @ must appear at
// the start of the content or immediately after whitespace, so email-style
// occurrences (e.g. "bob@example.com", "here@all") are not treated as mentions.
var mentionRe = regexp.MustCompile(`(^|\s)@([0-9a-zA-Z_-]+(\.[0-9a-zA-Z_-]+)*)`)

// ParseResult holds parsed mention data extracted from message content.
type ParseResult struct {
	Accounts   []string // unique mentioned accounts, lowercased, excluding @all
	MentionAll bool     // true if @all was mentioned (case-insensitive)
}

// Parse extracts @mention tokens from content, returning unique accounts and whether @all appears.
func Parse(content string) ParseResult {
	matches := mentionRe.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return ParseResult{}
	}

	var result ParseResult
	seen := make(map[string]struct{}, len(matches))

	for _, m := range matches {
		account := strings.ToLower(m[2])
		if account == "all" {
			result.MentionAll = true
			continue
		}
		if _, exists := seen[account]; !exists {
			seen[account] = struct{}{}
			result.Accounts = append(result.Accounts, account)
		}
	}

	return result
}

// LookupFunc abstracts user-by-account lookup so Resolve is testable without a real DB.
type LookupFunc func(ctx context.Context, accounts []string) ([]model.User, error)

// ResolveResult holds mention resolution output.
type ResolveResult struct {
	Participants []model.Participant // enriched mentioned users + @all entry if present
	MentionAll   bool                // true if @all was mentioned (case-insensitive)
	Accounts     []string            // raw parsed accounts (for caller use outside resolution)
}

// Resolve parses @mentions from content, looks up users via lookupFn,
// and builds Participants. On lookup error, returns partial result
// (MentionAll and Accounts populated, Participants empty) with the error.
func Resolve(ctx context.Context, content string, lookupFn LookupFunc) (*ResolveResult, error) {
	parsed := Parse(content)
	if len(parsed.Accounts) == 0 && !parsed.MentionAll {
		return &ResolveResult{
			Accounts:   parsed.Accounts,
			MentionAll: parsed.MentionAll,
		}, nil
	}

	users := map[string]model.User{}
	if len(parsed.Accounts) > 0 {
		fetched, err := lookupFn(ctx, parsed.Accounts)
		if err != nil {
			return &ResolveResult{
				Accounts:   parsed.Accounts,
				MentionAll: parsed.MentionAll,
			}, fmt.Errorf("find mentioned users: %w", err)
		}
		users = make(map[string]model.User, len(fetched))
		for i := range fetched {
			users[fetched[i].Account] = fetched[i]
		}
	}
	return ResolveFromParsed(parsed, users), nil
}

// ResolveFromParsed builds a ResolveResult from pre-parsed input and a caller-supplied user map.
// Use when the caller has already done the lookup. Unknown accounts are silently omitted.
func ResolveFromParsed(parsed ParseResult, users map[string]model.User) *ResolveResult {
	result := &ResolveResult{
		MentionAll: parsed.MentionAll,
		Accounts:   parsed.Accounts,
	}
	for _, account := range parsed.Accounts {
		u, ok := users[account]
		if !ok {
			continue
		}
		result.Participants = append(result.Participants, model.Participant{
			UserID:      u.ID,
			Account:     u.Account,
			SiteID:      u.SiteID,
			ChineseName: u.ChineseName,
			EngName:     u.EngName,
		})
	}
	if parsed.MentionAll {
		result.Participants = append(result.Participants, model.Participant{
			Account: "all",
			EngName: "all",
		})
	}
	return result
}
