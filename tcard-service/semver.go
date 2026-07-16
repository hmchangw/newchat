package main

import (
	"strconv"
	"strings"
)

// semver is a parsed a.b.c version.
type semver struct{ major, minor, patch int }

// parseSemver parses "a.b.c" (three all-digit parts); ok is false otherwise.
func parseSemver(s string) (semver, bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semver{}, false
	}
	var n [3]int
	for i, p := range parts {
		// Reject empties, signs/non-digits, and leading zeros (strict semver).
		if p == "" || !allDigits(p) || (len(p) > 1 && p[0] == '0') {
			return semver{}, false
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return semver{}, false
		}
		n[i] = v
	}
	return semver{n[0], n[1], n[2]}, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// greater reports whether s > o in semver order.
func (s semver) greater(o semver) bool {
	if s.major != o.major {
		return s.major > o.major
	}
	if s.minor != o.minor {
		return s.minor > o.minor
	}
	return s.patch > o.patch
}

// isHighest reports whether v is strictly greater than every well-formed
// version in existing; non-semver existing versions are ignored.
func isHighest(v string, existing []string) bool {
	nv, ok := parseSemver(v)
	if !ok {
		return false
	}
	for _, e := range existing {
		if ev, ok := parseSemver(e); ok && !nv.greater(ev) {
			return false
		}
	}
	return true
}
