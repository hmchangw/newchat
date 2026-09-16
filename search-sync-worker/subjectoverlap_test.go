package main

import (
	"context"
	"errors"
	"testing"

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

// `>` is legal only as the final token. subjectsIntersect documents that but did
// not enforce it, so a malformed hand-written filter passed the startup guard.
func TestSubjectsIntersect_MalformedAndBoundaryShapes(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{"non-final > does not match a longer subject", "a.>.b", "a.x.y", false},
		{"non-final > does not match its literal prefix", "a.>.b", "a.b", false},
		{"bare > matches any single token", ">", "a", true},
		{"bare > matches a multi-token subject", ">", "a.b.c", true},
		{"> against > overlaps", "a.>", "a.>", true},
		{"> needs at least one token", "a.b", "a.b.>", false},
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
