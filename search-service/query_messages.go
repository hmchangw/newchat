package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

// MessageIndexPattern is the comma-joined list of indices searched for
// messages. The local prefix (`messages-*`) is required because `*:` only
// matches configured remote clusters — without it, a user on site-a would
// miss their own site's messages. When no remote clusters are configured,
// `*:messages-*` resolves to zero matches and the query proceeds against
// local `messages-*` only (courtesy of `ignore_unavailable=true`).
var MessageIndexPattern = []string{"messages-*", "*:messages-*"}

// buildMessageQuery composes the ES `_search` body for a single message
// search request. `restricted` is the caller's cached `restrictedRooms`
// map (rid → historySharedSince millis); pass nil or empty for
// unrestricted-only users.
//
// For global search (req.RoomIDs == nil), the unrestricted clause uses an
// ES terms-lookup against the user-room doc so the service doesn't need
// to send the full list over the wire. For scoped search
// (req.RoomIDs != nil), the inline terms clause is STILL gated by the
// terms-lookup so a caller can't reach rooms they don't belong to by
// passing arbitrary roomIds.
func buildMessageQuery(req model.SearchMessagesRequest, account string, restricted map[string]int64, recentWindow time.Duration, userRoomIndex string) (json.RawMessage, error) {
	userRoomIndex = resolveUserRoomIndex(userRoomIndex)
	clauses := roomAccessClauses(req.RoomIDs, account, restricted, userRoomIndex)

	body := map[string]any{
		"from":             req.Offset,
		"size":             req.Size,
		"track_total_hits": true,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"multi_match": map[string]any{
							"query":    req.SearchText,
							"type":     "bool_prefix",
							"operator": "AND",
							"fields":   []string{"content"},
						},
					},
				},
				"filter": []any{
					map[string]any{
						"range": map[string]any{
							"createdAt": map[string]any{
								"gte": fmt.Sprintf("now-%s", recentWindowToGte(recentWindow)),
							},
						},
					},
					map[string]any{
						"bool": map[string]any{
							"should":               clauses,
							"minimum_should_match": 1,
						},
					},
				},
			},
		},
		"sort": []any{
			"_score",
			map[string]any{"createdAt": map[string]any{"order": "desc"}},
		},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal message query: %w", err)
	}
	return data, nil
}

func roomAccessClauses(roomIDs []string, account string, restricted map[string]int64, userRoomIndex string) []any {
	if len(roomIDs) == 0 {
		return globalAccessClauses(account, restricted, userRoomIndex)
	}
	return scopedAccessClauses(roomIDs, account, restricted, userRoomIndex)
}

func globalAccessClauses(account string, restricted map[string]int64, userRoomIndex string) []any {
	clauses := []any{termsLookupClause(account, userRoomIndex)}
	for _, rid := range sortedRIDs(restricted) {
		iso := hssToISO(restricted[rid])
		clauses = append(clauses,
			restrictedRoomClauseA(rid, iso),
			restrictedRoomClauseB(rid, iso),
		)
	}
	return clauses
}

func scopedAccessClauses(roomIDs []string, account string, restricted map[string]int64, userRoomIndex string) []any {
	var unrestricted []string
	var restrictedSubset []string
	for _, rid := range roomIDs {
		if _, isRestricted := restricted[rid]; isRestricted {
			restrictedSubset = append(restrictedSubset, rid)
		} else {
			unrestricted = append(unrestricted, rid)
		}
	}

	clauses := make([]any, 0, 1+2*len(restrictedSubset))
	if len(unrestricted) > 0 {
		// AND inline terms with the user-room lookup so a caller can't
		// reach rooms they don't belong to by passing arbitrary roomIds.
		// The restricted subset is already safe — Clause A/B only fire
		// for rids present in the caller's cached restrictedRooms map.
		clauses = append(clauses, map[string]any{
			"bool": map[string]any{
				"filter": []any{
					termsInlineClause(unrestricted),
					termsLookupClause(account, userRoomIndex),
				},
			},
		})
	}
	for _, rid := range restrictedSubset {
		iso := hssToISO(restricted[rid])
		clauses = append(clauses,
			restrictedRoomClauseA(rid, iso),
			restrictedRoomClauseB(rid, iso),
		)
	}
	return clauses
}

// termsLookupClause resolves the user's allowed rooms via ES terms-lookup
// instead of shipping the rooms[] array on every query. Passing an empty
// userRoomIndex would produce an invalid index name, so callers must
// resolve it (e.g. via resolveUserRoomIndex) before calling.
func termsLookupClause(account, userRoomIndex string) map[string]any {
	return map[string]any{
		"terms": map[string]any{
			"roomId": map[string]any{
				"index": userRoomIndex,
				"id":    account,
				"path":  "rooms",
			},
		},
	}
}

func termsInlineClause(roomIDs []string) map[string]any {
	return map[string]any{
		"terms": map[string]any{
			"roomId": roomIDs,
		},
	}
}

// restrictedRoomBaseMust is the shared scaffolding between Clause A and
// Clause B — every restricted-room clause gates on (roomId == rid) AND
// (createdAt >= hss). Clause A adds must_not exists threadParent; Clause
// B adds exists threadParent plus the B1/B2 inner OR.
func restrictedRoomBaseMust(rid, hssISO string) []any {
	return []any{
		map[string]any{"term": map[string]any{"roomId": rid}},
		map[string]any{
			"range": map[string]any{
				"createdAt": map[string]any{"gte": hssISO},
			},
		},
	}
}

// Clause A matches parent messages (or regular non-thread messages)
// posted after the user's HSS bound for this room. Thread replies are
// explicitly excluded via must_not so Clause B remains the sole gate
// for thread replies in restricted rooms.
func restrictedRoomClauseA(rid, hssISO string) map[string]any {
	return map[string]any{
		"bool": map[string]any{
			"must": restrictedRoomBaseMust(rid, hssISO),
			"must_not": []any{
				map[string]any{"exists": map[string]any{"field": "threadParentMessageId"}},
			},
		},
	}
}

// Clause B matches thread replies in this restricted room. Two gates
// must BOTH hold:
//  1. The reply itself is at or after HSS (via restrictedRoomBaseMust's
//     createdAt range). Without this outer gate, a pre-HSS reply flagged
//     tshow=true would leak restricted-room history the user never had
//     access to.
//  2. Either the reply is also shown in the channel (B1: tshow=true) OR
//     the parent message is at or after HSS (B2).
func restrictedRoomClauseB(rid, hssISO string) map[string]any {
	must := restrictedRoomBaseMust(rid, hssISO)
	must = append(must,
		map[string]any{"exists": map[string]any{"field": "threadParentMessageId"}},
		map[string]any{
			"bool": map[string]any{
				"should": []any{
					map[string]any{"term": map[string]any{"tshow": true}},
					map[string]any{
						"range": map[string]any{
							"threadParentMessageCreatedAt": map[string]any{"gte": hssISO},
						},
					},
				},
				"minimum_should_match": 1,
			},
		},
	)
	return map[string]any{"bool": map[string]any{"must": must}}
}

func hssToISO(hss int64) string {
	return time.UnixMilli(hss).UTC().Format(time.RFC3339Nano)
}

// recentWindowToGte converts a Go Duration into an ES date-math fragment.
// ES date-math accepts a single `<N><unit>` token per operator, NOT
// compound forms like `8760h0m0s` that Go's Duration.String produces —
// compound forms fail to parse and ES rejects the whole query.
func recentWindowToGte(d time.Duration) string {
	if d <= 0 {
		// Zero / negative durations would emit `now-0s` which ES interprets
		// as "strictly after now" — effectively no matches. Bias to a
		// 1-year default so a misconfigured value degrades to the intended
		// behavior rather than an empty result set.
		d = 365 * 24 * time.Hour
	}
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	default:
		// ES date-math has no sub-second unit — supported set is y/M/w/d/h/m/s.
		// Round UP so a misconfigured sub-second value widens the window
		// rather than collapsing it (which would silently drop matches).
		return fmt.Sprintf("%ds", int64((d+time.Second-1)/time.Second))
	}
}

// sortedRIDs returns map keys in ascending order. Sort is load-bearing
// for golden-file tests and for ES query-plan cacheability — do not
// remove without replacing both guarantees.
func sortedRIDs(m map[string]int64) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
