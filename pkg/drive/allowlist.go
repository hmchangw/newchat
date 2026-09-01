package drive

import (
	"errors"
	"strings"
)

// ErrHostNotAllowed reports a Drive host that is not one of the configured
// base URLs. Callers that accept a host from a client map it to a 400 rather
// than surfacing it as an upstream failure — the request is malformed, not the
// dependency broken.
var ErrHostNotAllowed = errors.New("drive host is not a configured Drive base URL")

// hostAllowed reports whether host is one of the Drive base URLs this client is
// configured for: the default URL, plus every non-empty entry of the
// room-origin base URL map.
//
// This exists because a client-supplied host reaches fetchPresignedURL, which
// attaches the Drive api-token to it — so without an allow-list the caller
// chooses which server receives the credential. The check lives here, beside
// the code that attaches the token, rather than in the HTTP handler: any future
// method that takes a host is then covered by construction.
//
// Matching is exact after trimming trailing slashes, deliberately. An allow-list
// that matched on prefix, suffix or "contains" would admit
// https://evil.example/?u=https://drive.site-a.example and its many cousins;
// the only safe comparison against an attacker-chosen string is equality with a
// value an operator configured. The cost is that a host must be spelled exactly
// as configured, which is the correct trade for a credential boundary.
//
// An empty BaseURLMap — what LoadBaseURLs falls back to on a missing or
// malformed config file — leaves the default URL allowed and nothing else, so a
// single-site deployment keeps working and a misconfigured one fails closed.
func (c *Client) hostAllowed(host string) bool {
	h := normalizeDriveHost(host)
	if h == "" {
		return false
	}
	if h == normalizeDriveHost(c.baseURL) {
		return true
	}
	for _, u := range c.baseURLMap {
		// Skip empty entries: they normalize to "" and would otherwise make the
		// empty host allowed, which the h == "" guard above already refuses.
		if u == "" {
			continue
		}
		if h == normalizeDriveHost(u) {
			return true
		}
	}
	return false
}

// normalizeDriveHost trims trailing slashes so a configured
// "https://drive.example/" and a supplied "https://drive.example" compare equal.
// It does no other rewriting — anything more would start reinterpreting an
// attacker-supplied string rather than comparing it.
func normalizeDriveHost(host string) string {
	return strings.TrimRight(strings.TrimSpace(host), "/")
}
