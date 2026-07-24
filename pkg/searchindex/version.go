// Package searchindex provides helpers shared by services that interact
// with Elasticsearch indices managed by search-sync-worker.
package searchindex

import (
	"regexp"
	"strconv"
)

var versionSuffix = regexp.MustCompile(`-v(\d+)$`)

// StripVersion splits "<base>-v<N>" into its base and integer version.
// Returns ok=false for unversioned names (e.g. user-room indices) so
// callers can treat the value as a literal index name.
func StripVersion(name string) (base string, version int, ok bool) {
	m := versionSuffix.FindStringSubmatchIndex(name)
	if m == nil {
		return name, 0, false
	}
	v, _ := strconv.Atoi(name[m[2]:m[3]])
	return name[:m[0]], v, true
}

// StripVersionBase returns just the base from StripVersion, discarding
// the version number and ok flag.
func StripVersionBase(name string) string {
	base, _, _ := StripVersion(name)
	return base
}

// IndexPattern returns the wildcard covering every index of prefix's base, e.g.
// "messages-a-v2" → "messages-a-*"; template and mapping push share it to avoid drift.
func IndexPattern(prefix string) string {
	return StripVersionBase(prefix) + "-*"
}
