package natsrouter

import (
	"fmt"
	"strings"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/subject"
)

// Params holds named tokens extracted from a NATS subject at request time.
// Values are populated by matching the incoming subject against the registered
// pattern's {name} placeholders.
type Params struct {
	values map[string]string
}

// NewParams creates Params from a map of key-value pairs.
// Useful for testing handlers that accept Params without a real NATS subject.
func NewParams(values map[string]string) Params {
	return Params{values: values}
}

// Get returns the value of a named param, or empty string if not found.
func (p Params) Get(key string) string {
	return p.values[key]
}

// MustGet returns the value of a named param. Panics if the key is not found.
// Use only when the param is guaranteed by the pattern — a panic here means
// the pattern doesn't contain the requested param (developer error).
func (p Params) MustGet(key string) string {
	v, ok := p.values[key]
	if !ok {
		panic(fmt.Sprintf("natsrouter: param %q not found in subject", key))
	}
	return v
}

// Require returns the value of a named param or an error if not found/empty.
func (p Params) Require(key string) (string, error) {
	v, ok := p.values[key]
	if !ok || v == "" {
		return "", errcode.BadRequest("missing required param: " + key)
	}
	return v, nil
}

// route is created once at registration time from a pattern.
// It holds the converted NATS wildcard subject and the position-to-name
// mapping for param extraction.
type route struct {
	natsSubject string         // "chat.user.*.request.room.*.*.msg.history"
	params      map[int]string // {2: "account", 5: "roomID", 6: "siteID"}
}

// parsePattern converts a pattern with {name} placeholders into a route.
// Each {name} token becomes a * in the NATS subject and its position is
// recorded in the params map for extraction at request time.
//
// Example:
//
//	parsePattern("chat.user.{account}.request.room.{roomID}.{siteID}.msg.history")
//	→ route{
//	    natsSubject: "chat.user.*.request.room.*.*.msg.history",
//	    params:      map[int]string{2: "account", 5: "roomID", 6: "siteID"},
//	  }
func parsePattern(pattern string) route {
	parts := strings.Split(pattern, ".")
	params := make(map[int]string)
	nats := make([]string, len(parts))

	for i, part := range parts {
		if len(part) > 2 && part[0] == '{' && part[len(part)-1] == '}' {
			name := part[1 : len(part)-1]
			params[i] = name
			nats[i] = "*"
		} else {
			nats[i] = part
		}
	}

	return route{
		natsSubject: strings.Join(nats, "."),
		params:      params,
	}
}

// extractParams scans the subject by "." positions and pulls values
// from the positions recorded in the route's params map.
// Uses index scanning instead of strings.Split to avoid a []string allocation.
//
// The "account" param arrives in NATS transport form — a ".bot" bot's account
// is minted into its JWT with dots replaced by underscores (weather.bot →
// weather_bot), so the subject token is the encoded form. It is decoded back to
// the requester's real account (subject.DecodeAccount) so every router-registered
// handler gets the true identity / data key. A no-op for every non-bot account;
// this one choke point covers all router RPCs. Other params are never decoded.
func (r route) extractParams(subj string) Params {
	if len(r.params) == 0 {
		return Params{}
	}
	values := make(map[string]string, len(r.params))
	pos := 0
	tokenStart := 0
	for i := 0; i <= len(subj); i++ {
		if i == len(subj) || subj[i] == '.' {
			if name, ok := r.params[pos]; ok {
				val := subj[tokenStart:i]
				if name == "account" {
					val = subject.DecodeAccount(val)
				}
				values[name] = val
			}
			pos++
			tokenStart = i + 1
		}
	}
	return Params{values: values}
}
