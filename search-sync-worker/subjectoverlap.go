package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
)

// subjectsIntersect reports whether two NATS subject patterns share at least one
// concrete subject.
//
// Both sides may carry wildcards, because a stream binds
// `chat.bot.canonical.{site}.>` while a consumer filters
// `chat.bot.canonical.{site}.*`. Standard NATS semantics — `*` is exactly one
// token, `>` is one-or-more trailing tokens and may only be last.
//
// Intersection, not containment: a consumer consumes a stream as long as the two
// patterns overlap anywhere. A filter of `chat.events.*` against a stream binding
// the literal `chat.events.created` is not contained by it, yet delivers that
// subject perfectly well — so containment would reject a working config.
func subjectsIntersect(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	at := strings.Split(a, ".")
	bt := strings.Split(b, ".")

	for i := 0; ; i++ {
		aEnd, bEnd := i >= len(at), i >= len(bt)
		if aEnd && bEnd {
			return true
		}
		if aEnd || bEnd {
			// One pattern requires a token the other cannot supply. A `>` would
			// already have returned below, so nothing here can absorb the tail.
			return false
		}
		ta, tb := at[i], bt[i]
		// `>` absorbs the rest only where it is legal — as the final token. A `>`
		// anywhere else is a malformed subject the server would reject at create
		// time, so it matches nothing here rather than swallowing the comparison.
		if (ta == ">" && i == len(at)-1) || (tb == ">" && i == len(bt)-1) {
			return true
		}
		if ta == "*" || tb == "*" {
			continue
		}
		if ta != tb {
			return false
		}
	}
}

// anySubjectIntersects reports whether any of a stream's subjects overlaps filter.
func anySubjectIntersects(patterns []string, filter string) bool {
	for _, p := range patterns {
		if subjectsIntersect(p, filter) {
			return true
		}
	}
	return false
}

// checkFilterSubjects returns an error if a consumer filter selects nothing on
// the stream it binds.
//
// JetStream accepts such a consumer without complaint: it is created, reports
// healthy, and delivers nothing — forever. The only symptom is an index quietly
// missing a whole class of document, which surfaces as "search doesn't find
// that" months later rather than as an error anyone can act on. Failing at
// startup turns a silent correctness bug into a config fix.
//
// It answers only "disjoint or not". A filter that overlaps its stream partially
// is a legitimate narrowing and passes.
func checkFilterSubjects(streamName string, streamSubjects, filters []string, consumerName string) error {
	for _, f := range filters {
		if !anySubjectIntersects(streamSubjects, f) {
			return fmt.Errorf(
				"consumer %q filters on %q, which shares no subject with stream %q (%v): it would consume nothing",
				consumerName, f, streamName, streamSubjects)
		}
	}
	return nil
}

// streamSubjectsFunc reads a stream's deployed subjects. A func rather than the
// jetstream.Stream interface so the preflight is exercisable without a live server.
type streamSubjectsFunc func(ctx context.Context, name string) ([]string, error)

// cachedStreamInfo is the one method the plain and o11y-wrapped stream handles
// share; each returns its own Stream type, so the adapter is generic over it.
type cachedStreamInfo interface {
	CachedInfo() *jetstream.StreamInfo
}

// liveStreamSubjects adapts a JetStream handle's Stream method to
// streamSubjectsFunc. Pass the method value: liveStreamSubjects(js.Stream).
func liveStreamSubjects[S cachedStreamInfo](lookup func(context.Context, string) (S, error)) streamSubjectsFunc {
	return func(ctx context.Context, name string) ([]string, error) {
		s, err := lookup(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("look up stream %s: %w", name, err)
		}
		info := s.CachedInfo()
		if info == nil {
			return nil, fmt.Errorf("look up stream %s: no cached info", name)
		}
		return info.Config.Subjects, nil
	}
}

// preflightFilters checks a consumer's filters against the stream's DEPLOYED
// subjects rather than this service's declaration of them.
//
// The distinction is the whole point: for streams another service owns (INBOX,
// HR) the local StreamConfig is a belief that nothing reconciles, so checking it
// against filters built from the same constants can only restate a compile-time
// fact. Ops narrowing the real subjects is the one drift that happens in
// production, and only a live read can see it.
//
// An unreadable stream is therefore a failed check, not a reason to fall back to
// the declaration. Substituting it would have passed exactly when the answer was
// unknown — and the declaration always matches, because the filters are built
// from the same constants — so a transient lookup failure against a stream that
// HAS drifted would wave through the inert consumer this guard exists to stop.
// Startup must not report a filter as verified when nothing verified it.
//
// Failing here costs nothing the caller was not already paying: main.go creates
// the consumer immediately after, exiting on error, so a stream that is absent
// aborts startup either way — this just names the real reason. The case the
// fallback actually covered was the one where the stream exists but its info is
// unreadable (denied credentials, a blip), and that is precisely the case where
// the local declaration is evidence of nothing.
func preflightFilters(ctx context.Context, lookup streamSubjectsFunc, streamName string, filters []string, consumerName string) error {
	subjects, err := lookup(ctx, streamName)
	if err != nil {
		return fmt.Errorf("consumer %q: read deployed subjects for stream %q: %w", consumerName, streamName, err)
	}
	if len(subjects) == 0 {
		return fmt.Errorf("consumer %q: stream %q reports no deployed subjects", consumerName, streamName)
	}
	return checkFilterSubjects(streamName, subjects, filters, consumerName)
}
