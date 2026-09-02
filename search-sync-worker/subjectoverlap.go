package main

import (
	"fmt"
	"strings"
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
		if ta == ">" || tb == ">" {
			// Legal only as the final token, and the other side still has at
			// least this one token for it to absorb.
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

// checkFilterSubjects reports a consumer filter that selects nothing on the
// stream it binds.
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
