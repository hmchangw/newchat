package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
)

// buildOrgQuery composes the ES `_search` body for an organization search over
// the company-wide spotlight-org index. Unlike buildRoomQuery it applies no
// access filter and sorts on `_score` only — no org field has doc-values to
// tiebreak on, and `_id` sorting is avoided (fielddata may be disabled).
func buildOrgQuery(req model.SearchOrgsRequest) (json.RawMessage, error) {
	body := map[string]any{
		"from":             req.Offset,
		"size":             req.Size,
		"track_total_hits": true,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{
						"multi_match": map[string]any{
							"query":    req.Query,
							"type":     "bool_prefix",
							"operator": "AND",
							"fields":   []string{"sectName", "sectTCName", "deptName", "deptTCName", "divisionId"},
						},
					},
				},
			},
		},
		"sort": []any{"_score"},
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal org query: %w", err)
	}
	return data, nil
}
