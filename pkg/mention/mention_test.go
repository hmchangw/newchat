package mention

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		accounts   []string
		mentionAll bool
	}{
		{name: "no mentions", content: "hello world", accounts: nil, mentionAll: false},
		{name: "single mention", content: "hello @bob", accounts: []string{"bob"}, mentionAll: false},
		{name: "multiple mentions", content: "@alice check with @bob", accounts: []string{"alice", "bob"}, mentionAll: false},
		{name: "mention at start", content: "@alice hello", accounts: []string{"alice"}, mentionAll: false},
		{name: "mention after newline", content: "line1\n@bob", accounts: []string{"bob"}, mentionAll: false},
		{name: "mention after tab", content: "hi\t@bob", accounts: []string{"bob"}, mentionAll: false},
		{name: "duplicates deduplicated", content: "@bob and @bob again", accounts: []string{"bob"}, mentionAll: false},
		{name: "dots and hyphens", content: "cc @first.last and @my-user", accounts: []string{"first.last", "my-user"}, mentionAll: false},
		{name: "empty content", content: "", accounts: nil, mentionAll: false},
		{name: "trailing period not captured", content: "hey @bob. check this", accounts: []string{"bob"}, mentionAll: false},
		{name: "@all lowercase", content: "hey @all check this", accounts: nil, mentionAll: true},
		{name: "@All uppercase", content: "attention @All everyone", accounts: nil, mentionAll: true},
		{name: "@all at start", content: "@all team", accounts: nil, mentionAll: true},
		{name: "@all and individual", content: "@All and @alice", accounts: []string{"alice"}, mentionAll: true},
		{name: "case-insensitive dedup", content: "@alice @Alice", accounts: []string{"alice"}, mentionAll: false},
		{name: "mixed case lowered", content: "hey @BOB", accounts: []string{"bob"}, mentionAll: false},

		// @ must be at start-of-text or after whitespace.
		{name: "email rejected", content: "contact me at bob@example.com", accounts: nil, mentionAll: false},
		{name: "email plain", content: "email@example.com", accounts: nil, mentionAll: false},
		{name: "no space before @", content: "hello@bob", accounts: nil, mentionAll: false},
		{name: "punctuation before @", content: "hi,@bob", accounts: nil, mentionAll: false},
		{name: "word@all not @all", content: "say hi here@all team", accounts: nil, mentionAll: false},

		// Email-style suffix no longer captured: only the leading @user matches.
		{name: "email-style suffix dropped", content: "ping @user@domain.com", accounts: []string{"user"}, mentionAll: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Parse(tt.content)
			assert.Equal(t, tt.accounts, result.Accounts)
			assert.Equal(t, tt.mentionAll, result.MentionAll)
		})
	}
}

func TestResolve(t *testing.T) {
	bobUser := model.User{ID: "u-bob", Account: "bob", SiteID: "site-b", EngName: "Bob Chen", ChineseName: "鮑勃"}
	aliceUser := model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}

	tests := []struct {
		name           string
		content        string
		lookupUsers    []model.User
		lookupErr      error
		wantAccounts   []string
		wantMentionAll bool
		wantParts      []model.Participant
		wantErr        bool
	}{
		{
			name:    "no mentions",
			content: "hello world",
		},
		{
			name:         "single mention resolved",
			content:      "hey @bob",
			lookupUsers:  []model.User{bobUser},
			wantAccounts: []string{"bob"},
			wantParts: []model.Participant{
				{UserID: "u-bob", Account: "bob", SiteID: "site-b", EngName: "Bob Chen", ChineseName: "鮑勃"},
			},
		},
		{
			name:         "multiple mentions resolved",
			content:      "@alice and @bob",
			lookupUsers:  []model.User{aliceUser, bobUser},
			wantAccounts: []string{"alice", "bob"},
			wantParts: []model.Participant{
				{UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"},
				{UserID: "u-bob", Account: "bob", SiteID: "site-b", EngName: "Bob Chen", ChineseName: "鮑勃"},
			},
		},
		{
			name:           "@all only — no lookup",
			content:        "hello @all",
			wantMentionAll: true,
			wantParts: []model.Participant{
				{Account: "all", EngName: "all"},
			},
		},
		{
			name:           "@all and individual",
			content:        "@all and @bob",
			lookupUsers:    []model.User{bobUser},
			wantAccounts:   []string{"bob"},
			wantMentionAll: true,
			wantParts: []model.Participant{
				{UserID: "u-bob", Account: "bob", SiteID: "site-b", EngName: "Bob Chen", ChineseName: "鮑勃"},
				{Account: "all", EngName: "all"},
			},
		},
		{
			name:         "unresolved account skipped",
			content:      "@alice and @unknown",
			lookupUsers:  []model.User{aliceUser},
			wantAccounts: []string{"alice", "unknown"},
			wantParts: []model.Participant{
				{UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"},
			},
		},
		{
			name:         "lookup error — partial result returned",
			content:      "hey @bob",
			lookupErr:    errors.New("db error"),
			wantAccounts: []string{"bob"},
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookupFn := func(_ context.Context, _ []string) ([]model.User, error) {
				if tt.lookupErr != nil {
					return nil, tt.lookupErr
				}
				return tt.lookupUsers, nil
			}
			result, err := Resolve(context.Background(), tt.content, lookupFn)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.NotNil(t, result)
			assert.Equal(t, tt.wantAccounts, result.Accounts)
			assert.Equal(t, tt.wantMentionAll, result.MentionAll)
			assert.Equal(t, tt.wantParts, result.Participants)
		})
	}
}

func TestResolveFromParsed(t *testing.T) {
	aliceUser := model.User{ID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"}
	bobUser := model.User{ID: "u-bob", Account: "bob", SiteID: "site-a", EngName: "Bob Chen", ChineseName: "鮑勃"}

	tests := []struct {
		name           string
		parsed         ParseResult
		users          map[string]model.User
		wantParts      []model.Participant
		wantAccounts   []string
		wantMentionAll bool
	}{
		{
			name:   "all mentions resolved",
			parsed: ParseResult{Accounts: []string{"alice", "bob"}},
			users:  map[string]model.User{"alice": aliceUser, "bob": bobUser},
			wantParts: []model.Participant{
				{UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"},
				{UserID: "u-bob", Account: "bob", SiteID: "site-a", EngName: "Bob Chen", ChineseName: "鮑勃"},
			},
			wantAccounts: []string{"alice", "bob"},
		},
		{
			name:           "mention all appends synthetic participant",
			parsed:         ParseResult{Accounts: []string{"alice"}, MentionAll: true},
			users:          map[string]model.User{"alice": aliceUser},
			wantMentionAll: true,
			wantAccounts:   []string{"alice"},
			wantParts: []model.Participant{
				{UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"},
				{Account: "all", EngName: "all"},
			},
		},
		{
			name:         "unknown account silently omitted",
			parsed:       ParseResult{Accounts: []string{"alice", "ghost"}},
			users:        map[string]model.User{"alice": aliceUser},
			wantAccounts: []string{"alice", "ghost"},
			wantParts: []model.Participant{
				{UserID: "u-alice", Account: "alice", SiteID: "site-a", EngName: "Alice Wang", ChineseName: "愛麗絲"},
			},
		},
		{
			name:   "no mentions, no MentionAll",
			parsed: ParseResult{},
		},
		{
			name:           "MentionAll only",
			parsed:         ParseResult{MentionAll: true},
			wantMentionAll: true,
			wantParts:      []model.Participant{{Account: "all", EngName: "all"}},
		},
		{
			name:         "nil users map is treated like empty",
			parsed:       ParseResult{Accounts: []string{"alice"}},
			users:        nil,
			wantAccounts: []string{"alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveFromParsed(tt.parsed, tt.users)
			require.NotNil(t, got)
			assert.Equal(t, tt.wantAccounts, got.Accounts)
			assert.Equal(t, tt.wantMentionAll, got.MentionAll)
			assert.Equal(t, tt.wantParts, got.Participants)
		})
	}
}

func TestReplaceAccounts(t *testing.T) {
	names := map[string]string{
		"alice":      "Alice Wang 愛麗絲",
		"bob":        "Bob Chen",
		"first.last": "First Last",
	}

	tests := []struct {
		name    string
		content string
		names   map[string]string
		want    string
	}{
		{name: "no mentions", content: "hello world", names: names, want: "hello world"},
		{name: "empty content", content: "", names: names, want: ""},
		{name: "single known mention", content: "hello @alice", names: names, want: "hello Alice Wang 愛麗絲"},
		{name: "mention at start", content: "@alice hello", names: names, want: "Alice Wang 愛麗絲 hello"},
		{name: "multiple known mentions", content: "@alice check with @bob", names: names, want: "Alice Wang 愛麗絲 check with Bob Chen"},
		{name: "duplicate mention replaced every occurrence", content: "@bob and @bob again", names: names, want: "Bob Chen and Bob Chen again"},
		{name: "unknown account kept verbatim", content: "hey @nobody", names: names, want: "hey @nobody"},
		{name: "mixed known and unknown", content: "@alice and @nobody", names: names, want: "Alice Wang 愛麗絲 and @nobody"},
		{name: "token casing folded for lookup", content: "hey @ALICE", names: names, want: "hey Alice Wang 愛麗絲"},
		{name: "dotted account", content: "cc @first.last ok", names: names, want: "cc First Last ok"},
		{name: "trailing punctuation preserved", content: "hey @bob. check", names: names, want: "hey Bob Chen. check"},
		{name: "mention after newline", content: "line1\n@bob", names: names, want: "line1\nBob Chen"},
		{name: "mention after tab", content: "hi\t@bob", names: names, want: "hi\tBob Chen"},
		{name: "email address is not a mention", content: "mail bob@example.com now", names: names, want: "mail bob@example.com now"},
		{name: "second @ without separator untouched", content: "@alice@bob", names: names, want: "Alice Wang 愛麗絲@bob"},
		{name: "at all becomes All", content: "hey @all now", names: names, want: "hey All now"},
		{name: "at all casing folded", content: "@All and @ALL", names: names, want: "All and All"},
		{name: "at here becomes here", content: "hey @here now", names: names, want: "hey here now"},
		{name: "at here casing folded", content: "@Here now", names: names, want: "here now"},
		{name: "literals resolve without a names map", content: "@all @here", names: nil, want: "All here"},
		{name: "nil names leaves accounts verbatim", content: "hey @alice", names: nil, want: "hey @alice"},
		{name: "empty names leaves accounts verbatim", content: "hey @alice", names: map[string]string{}, want: "hey @alice"},
		{name: "empty display name falls back to raw token", content: "hey @alice", names: map[string]string{"alice": ""}, want: "hey @alice"},
		{name: "literals win over a same-named account", content: "@all", names: map[string]string{"all": "Should Not Win"}, want: "All"},
		{name: "everything at once", content: "@all ping @alice and @nobody cc @here", names: names, want: "All ping Alice Wang 愛麗絲 and @nobody cc here"},
		// Substituted text is never rescanned: a display name that looks like a
		// mention passes through inert. Pins the ReplaceAllStringFunc choice.
		{name: "display name containing a mention is not rescanned", content: "@alice @bob", names: map[string]string{"alice": "@bob", "bob": "Bob Chen"}, want: "@bob Bob Chen"},
		{name: "display name containing at-all is not rescanned", content: "@alice", names: map[string]string{"alice": "@all"}, want: "@all"},
		// Regexp expansion metachars in a display name must survive verbatim; they
		// would be eaten by a ReplaceAllString refactor.
		{name: "dollar metachars in display name survive", content: "@eve hi", names: map[string]string{"eve": "Eve $1 $name &"}, want: "Eve $1 $name & hi"},
		{name: "backslash in display name survives", content: "@eve hi", names: map[string]string{"eve": `Eve \\1`}, want: `Eve \\1 hi`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ReplaceAccounts(tt.content, tt.names))
		})
	}
}

func TestReplaceAccounts_DoesNotMutateInput(t *testing.T) {
	names := map[string]string{"alice": "Alice Wang"}
	content := "hey @alice"

	got := ReplaceAccounts(content, names)

	assert.Equal(t, "hey @alice", content, "input content must not be mutated")
	assert.Equal(t, "hey Alice Wang", got)
	assert.Len(t, names, 1, "names map must not be mutated")
}

func TestLookupAccountsFromParsed(t *testing.T) {
	tests := []struct {
		name   string
		parsed ParseResult
		want   []string
	}{
		{name: "empty result", parsed: ParseResult{}, want: nil},
		{name: "mention all only", parsed: ParseResult{MentionAll: true}, want: nil},
		{name: "here is filtered out", parsed: ParseResult{Accounts: []string{"here"}}, want: nil},
		{name: "accounts kept", parsed: ParseResult{Accounts: []string{"alice", "bob"}}, want: []string{"alice", "bob"}},
		{name: "literals stripped from accounts", parsed: ParseResult{Accounts: []string{"here", "alice", "all"}}, want: []string{"alice"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, LookupAccountsFromParsed(tt.parsed))
		})
	}
}

// TestResolve_PartialHitsSurviveLookupError pins the outage contract: a store
// that answers some accounts and then fails must not lose the ones it answered.
// pkg/userstore.Cache returns exactly this shape (L1 hits + error) when Mongo is
// down, and dropping it means a mention that WAS resolvable is persisted as
// plain text forever.
func TestResolve_PartialHitsSurviveLookupError(t *testing.T) {
	partial := []model.User{{ID: "u1", Account: "alice", EngName: "Alice"}}
	lookup := func(context.Context, []string) ([]model.User, error) {
		return partial, errors.New("mongo down")
	}

	got, err := Resolve(context.Background(), "hi @alice and @bob", lookup)
	require.Error(t, err, "the caller must still learn the lookup degraded")
	require.NotNil(t, got)

	require.Len(t, got.Participants, 1, "the resolved half must survive")
	assert.Equal(t, "alice", got.Participants[0].Account)
	assert.Equal(t, "u1", got.Participants[0].UserID)
	assert.ElementsMatch(t, []string{"alice", "bob"}, got.Accounts, "raw accounts stay intact")
}

func TestResolve_MentionAllSurvivesLookupError(t *testing.T) {
	lookup := func(context.Context, []string) ([]model.User, error) {
		return nil, errors.New("mongo down")
	}

	got, err := Resolve(context.Background(), "@all and @alice", lookup)
	require.Error(t, err)
	require.NotNil(t, got)
	assert.True(t, got.MentionAll)
	// @all needs no lookup, so it must resolve even when every user lookup fails.
	require.Len(t, got.Participants, 1)
	assert.Equal(t, "all", got.Participants[0].Account)
}
