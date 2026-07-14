package main

import "strings"

// mediaTypeFilter decides whether an uploaded MIME type is allowed: blacklist
// first (deny wins), then whitelist (when non-empty, the type must match). Each
// list is split into an exact-match set (O(1)) and a slice of wildcard patterns.
type mediaTypeFilter struct {
	whitelistExact    map[string]struct{}
	whitelistWildcard []string
	blacklistExact    map[string]struct{}
	blacklistWildcard []string
}

func newMediaTypeFilter(whitelist, blacklist string) *mediaTypeFilter {
	we, ww := parseMediaTypes(whitelist)
	be, bw := parseMediaTypes(blacklist)
	return &mediaTypeFilter{
		whitelistExact:    we,
		whitelistWildcard: ww,
		blacklistExact:    be,
		blacklistWildcard: bw,
	}
}

// parseMediaTypes splits a CSV into an exact-match set and a wildcard slice
// ("type/*" or bare "*"/"*/*"). Entries are normalized; blanks are dropped.
func parseMediaTypes(csv string) (exact map[string]struct{}, wildcard []string) {
	exact = make(map[string]struct{})
	for _, p := range strings.Split(csv, ",") {
		p = normalizeMediaType(p)
		if p == "" {
			continue
		}
		if p == "*" || p == "*/*" || strings.HasSuffix(p, "/*") {
			wildcard = append(wildcard, p)
			continue
		}
		exact[p] = struct{}{}
	}
	return exact, wildcard
}

func (f *mediaTypeFilter) allowed(mime string) bool {
	m := normalizeMediaType(mime)
	if matchSet(f.blacklistExact, f.blacklistWildcard, m) {
		return false
	}
	if len(f.whitelistExact) == 0 && len(f.whitelistWildcard) == 0 {
		return true
	}
	return matchSet(f.whitelistExact, f.whitelistWildcard, m)
}

// matchSet returns true if mime is in the exact set (O(1)) or matches any
// wildcard pattern in the slice.
func matchSet(exact map[string]struct{}, wildcard []string, mime string) bool {
	if _, ok := exact[mime]; ok {
		return true
	}
	for _, w := range wildcard {
		if matchMediaType(w, mime) {
			return true
		}
	}
	return false
}

// matchMediaType supports "type/*" prefix wildcard and bare "*".
func matchMediaType(pattern, mime string) bool {
	if pattern == "*" || pattern == "*/*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		return strings.HasPrefix(mime, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == mime
}

// normalizeMediaType lowercases, trims, and drops any parameters after the first
// ";" (e.g. "Image/SVG+XML; charset=utf-8" → "image/svg+xml") so a parameterized
// Content-Type can't slip past an exact-match allow/deny rule.
func normalizeMediaType(v string) string {
	if base, _, ok := strings.Cut(v, ";"); ok {
		v = base
	}
	return strings.ToLower(strings.TrimSpace(v))
}
