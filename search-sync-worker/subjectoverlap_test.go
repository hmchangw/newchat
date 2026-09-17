package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStream satisfies cachedStreamInfo, standing in for either the plain or the
// o11y-wrapped stream handle.
type fakeStream struct{ info *jetstream.StreamInfo }

func (f fakeStream) CachedInfo() *jetstream.StreamInfo { return f.info }

func TestLiveStreamSubjects(t *testing.T) {
	t.Run("returns the deployed subjects", func(t *testing.T) {
		lookup := liveStreamSubjects(func(context.Context, string) (fakeStream, error) {
			return fakeStream{info: &jetstream.StreamInfo{
				Config: jetstream.StreamConfig{Subjects: []string{"chat.bot.canonical.site-a.>"}},
			}}, nil
		})

		subjects, err := lookup(context.Background(), "BOT-MESSAGES-CANONICAL-site-a")

		require.NoError(t, err)
		assert.Equal(t, []string{"chat.bot.canonical.site-a.>"}, subjects)
	})

	t.Run("wraps a lookup failure with the stream name", func(t *testing.T) {
		lookup := liveStreamSubjects(func(context.Context, string) (fakeStream, error) {
			return fakeStream{}, errors.New("stream not found")
		})

		_, err := lookup(context.Background(), "MISSING-site-a")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "MISSING-site-a")
		assert.Contains(t, err.Error(), "stream not found")
	})

	t.Run("absent cached info is an error, not a panic", func(t *testing.T) {
		lookup := liveStreamSubjects(func(context.Context, string) (fakeStream, error) {
			return fakeStream{info: nil}, nil
		})

		_, err := lookup(context.Background(), "BOT-MESSAGES-CANONICAL-site-a")

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no cached info")
	})
}

// The bug this guards: the service's own StreamConfig is a declaration, not the
// deployed truth. For INBOX and HR — streams owned by other services — ops can
// narrow the real subjects and the local copy never notices.
func TestPreflightFilters(t *testing.T) {
	const (
		streamName   = "BOT-MESSAGES-CANONICAL-site-a"
		consumerName = "bot-message-sync"
	)
	declared := []string{"chat.bot.canonical.site-a.>"}

	lookupReturning := func(subjects []string) streamSubjectsFunc {
		return func(context.Context, string) ([]string, error) { return subjects, nil }
	}

	tests := []struct {
		name    string
		lookup  streamSubjectsFunc
		filters []string
		wantErr string
	}{
		{
			name:    "live subjects disjoint from filter is rejected even though the declaration overlaps",
			lookup:  lookupReturning([]string{"chat.bot.canonical.other-site.>"}),
			filters: []string{"chat.bot.canonical.site-a.*"},
			wantErr: "shares no subject",
		},
		{
			name:    "live subjects overlapping the filter pass",
			lookup:  lookupReturning([]string{"chat.bot.canonical.site-a.>"}),
			filters: []string{"chat.bot.canonical.site-a.*"},
		},
		{
			name:    "live narrowing that still overlaps is a legitimate config",
			lookup:  lookupReturning([]string{"chat.bot.canonical.site-a.created"}),
			filters: []string{"chat.bot.canonical.site-a.*"},
		},
		{
			name:    "lookup failure falls back to the declared subjects",
			lookup:  func(context.Context, string) ([]string, error) { return nil, errors.New("stream not found") },
			filters: []string{"chat.bot.canonical.site-a.*"},
		},
		{
			name:    "empty live subjects fall back to the declared subjects",
			lookup:  lookupReturning(nil),
			filters: []string{"chat.bot.canonical.site-a.*"},
		},
		{
			name:    "fallback still rejects a filter the declaration cannot carry",
			lookup:  func(context.Context, string) ([]string, error) { return nil, errors.New("stream not found") },
			filters: []string{"chat.msg.canonical.site-a.*"},
			wantErr: "shares no subject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := preflightFilters(context.Background(), tt.lookup, streamName, declared, tt.filters, consumerName)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Contains(t, err.Error(), consumerName, "error must name the consumer")
			assert.Contains(t, err.Error(), streamName, "error must name the stream")
		})
	}
}

// validSubjectPatterns returns every subject pattern of up to maxTokens tokens
// over {a, b, *, >}, keeping only those NATS considers valid.
//
// Two distinct literals plus both wildcards is the whole behaviour space: subject
// matching asks only whether two tokens are equal or wildcards and never looks at
// their characters, so real tokens (`chat`, a site id) would add cases without
// adding coverage.
func validSubjectPatterns(maxTokens int) []string {
	alphabet := []string{"a", "b", "*", ">"}
	var out []string
	var build func(prefix []string)
	build = func(prefix []string) {
		if len(prefix) > 0 {
			if s := strings.Join(prefix, "."); server.IsValidSubject(s) {
				out = append(out, s)
			}
		}
		if len(prefix) == maxTokens {
			return
		}
		for _, tok := range alphabet {
			build(append(prefix, tok))
		}
	}
	build(nil)
	return out
}

// TestSubjectsIntersect_MatchesServerSemantics checks subjectsIntersect against
// nats-server's own SubjectsCollide — the function the broker uses to answer this
// exact question — instead of against expectations written out by hand.
//
// Hand-written expectations are how the bug this file exists for shipped in the
// first place: messages_test.go asserted the bot collection's filter WAS the
// disjoint one, so the defect had a passing test describing it as intended. An
// oracle cannot enshrine a misunderstanding, because no expected value is ever
// stated — the reference supplies every answer.
//
// nats-server is a test-only import here (nineteen other _test.go files already
// use it). The production path keeps its own matcher so the server package never
// links into the service binary.
func TestSubjectsIntersect_MatchesServerSemantics(t *testing.T) {
	patterns := validSubjectPatterns(4)
	require.Greater(t, len(patterns), 100, "corpus too small to be meaningful")

	var examples []string
	mismatches := 0
	for _, a := range patterns {
		for _, b := range patterns {
			got, want := subjectsIntersect(a, b), server.SubjectsCollide(a, b)
			if got == want {
				continue
			}
			mismatches++
			if len(examples) < 10 {
				examples = append(examples, fmt.Sprintf("  %q vs %q: got %v, server says %v", a, b, got, want))
			}
		}
	}
	if mismatches > 0 {
		t.Fatalf("disagreed with nats-server on %d of %d pairs:\n%s",
			mismatches, len(patterns)*len(patterns), strings.Join(examples, "\n"))
	}
}

// TestSubjectsIntersect_Intent pins the cases that carry meaning rather than
// coverage: the bug, the review finding that reshaped the predicate, and the
// shape every shipped collection uses. Correctness is owned by the exhaustive
// check above; these say why the function exists.
func TestSubjectsIntersect_Intent(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{
			name: "the bug: a filter on another subject tree shares nothing with the stream",
			a:    "chat.bot.canonical.s.>", b: "chat.msg.canonical.s.*", want: false,
		},
		{
			// Containment answered false here, so the guard meant to catch a filter
			// that selects nothing would instead have refused to start a working
			// consumer, reporting the opposite of the truth.
			name: "overlap, not containment: a wildcard filter meets a literal stream leaf",
			a:    "chat.events.created", b: "chat.events.*", want: true,
		},
		{
			name: "the shape every shipped collection uses: a `*` filter under a `>` stream",
			a:    "chat.bot.canonical.s.>", b: "chat.bot.canonical.s.*", want: true,
		},
		{
			name: "a filter scoped to another site shares nothing",
			a:    "chat.bot.canonical.s.>", b: "chat.bot.canonical.other.*", want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, subjectsIntersect(tt.a, tt.b))
			assert.Equal(t, tt.want, subjectsIntersect(tt.b, tt.a), "intersection must be symmetric")
		})
	}
}

// TestSubjectsIntersect_InvalidSubjects covers what the oracle cannot: its corpus
// is filtered by server.IsValidSubject, so malformed shapes never reach it. A
// non-final `>`, an empty subject and a trailing empty token are all rejected by
// nats-server, which means SubjectsCollide's answer for them is unreachable in
// practice and this table is the only thing pinning ours.
//
// They matter because subjectsIntersect runs BEFORE consumer creation: a `>` that
// swallowed the comparison would return "these overlap" for a filter that is
// disjoint, which is exactly the silent failure the guard exists to prevent.
func TestSubjectsIntersect_InvalidSubjects(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"non-final > does not match a longer subject", "a.>.b", "a.x.y", false},
		{"non-final > does not match its literal prefix", "a.>.b", "a.b", false},
		{"an empty subject never intersects", "chat.a.>", "", false},
		{"both empty do not intersect", "", "", false},
		{"trailing empty token is not a wildcard", "a.", "a.b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, subjectsIntersect(tt.a, tt.b))
			assert.Equal(t, tt.want, subjectsIntersect(tt.b, tt.a), "must be symmetric")
		})
	}
}

func TestCheckFilterSubjects(t *testing.T) {
	tests := []struct {
		name     string
		subjects []string
		filters  []string
		wantErr  bool
	}{
		{"a covered filter passes", []string{"chat.bot.canonical.s.>"}, []string{"chat.bot.canonical.s.*"}, false},
		{"no filter at all passes (consumes the whole stream)", []string{"chat.bot.canonical.s.>"}, nil, false},
		{"every filter must be covered", []string{"chat.bot.canonical.s.>"},
			[]string{"chat.bot.canonical.s.*", "chat.msg.canonical.s.*"}, true},
		{"the shipped bug is rejected", []string{"chat.bot.canonical.s.>"}, []string{"chat.msg.canonical.s.*"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkFilterSubjects("BOT-MESSAGES-CANONICAL-s", tt.subjects, tt.filters, "bot-message-sync")
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "would consume nothing")
				return
			}
			assert.NoError(t, err)
		})
	}
}
