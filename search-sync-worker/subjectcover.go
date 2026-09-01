package main

import (
	"fmt"
	"strings"
)

// subjectCovers reports whether every subject a consumer filter selects is also
// captured by pattern, a stream's subject.
//
// This is pattern-against-pattern containment, not literal matching: both sides
// may carry NATS wildcards, because a stream binds `chat.bot.canonical.{site}.>`
// while a consumer filters `chat.bot.canonical.{site}.*`. Standard NATS
// semantics — `*` is exactly one token, `>` is one-or-more trailing tokens and
// may only be last.
//
// It exists to make one specific mistake impossible to ship silently: a
// consumer whose filter selects nothing on its own stream. JetStream accepts
// that configuration, so the consumer is created, reports healthy and delivers
// nothing, forever.
func subjectCovers(pattern, filter string) bool {
	if pattern == "" || filter == "" {
		return false
	}
	p := strings.Split(pattern, ".")
	f := strings.Split(filter, ".")

	for i, pt := range p {
		if pt == ">" {
			// A trailing `>` absorbs one or more remaining filter tokens. It is
			// only legal as the final token, so anything after it makes the
			// pattern malformed rather than more permissive.
			return i == len(p)-1 && len(f) > i
		}
		if i >= len(f) {
			return false // filter is shorter than the pattern
		}
		if pt == "*" {
			// One token against one token. A filter token of `>` would select a
			// whole subtree, which a single-token wildcard does not cover.
			if f[i] == ">" {
				return false
			}
			continue
		}
		if pt != f[i] {
			return false
		}
	}
	// No `>` in the pattern, so the filter must not reach past it.
	return len(f) == len(p)
}

// anySubjectCovers reports whether any of a stream's subjects covers filter.
func anySubjectCovers(patterns []string, filter string) bool {
	for _, p := range patterns {
		if subjectCovers(p, filter) {
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
func checkFilterSubjects(streamName string, streamSubjects, filters []string, consumerName string) error {
	for _, f := range filters {
		if !anySubjectCovers(streamSubjects, f) {
			return fmt.Errorf(
				"consumer %q filters on %q, which no subject of stream %q (%v) captures: it would consume nothing",
				consumerName, f, streamName, streamSubjects)
		}
	}
	return nil
}
