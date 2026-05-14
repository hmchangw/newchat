package main

import (
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// buildSearchAppsPipeline builds the Mongo aggregation pipeline for
// `search.apps`. The current body is a minimal-viable PROTOTYPE that
// supports name regex + optional assistant.enabled filter + $skip +
// $limit; the full pipeline per the design spec
// (docs/superpowers/specs/2026-05-13-search-service-nats-migrations-design.md §5.1)
// additionally $lookups subscriptions (the user-scope access guard),
// performs a $group, and $lookups rooms before $project. Extend this
// function as the real pipeline gets authored; the function signature
// is the stable contract.
//
// All metacharacters in query are escaped via regexp.QuoteMeta so
// a caller can't inject regex syntax (ReDoS or pattern injection).
// The match is case-insensitive substring (no anchors).
// offset is applied via $skip before $limit.
func buildSearchAppsPipeline(
	query, account string,
	assistantEnabled *bool,
	offset, limit int,
) []bson.M {
	matchStage := bson.M{
		"name": bson.M{
			"$regex":   regexp.QuoteMeta(query),
			"$options": "i",
		},
	}
	if assistantEnabled != nil {
		matchStage["assistant.enabled"] = *assistantEnabled
	}

	// TODO(searchApps-pipeline): replace this prototype body with the
	// full spec'd pipeline:
	//   1. $match (this stage — keep)
	//   2. $lookup against `subscriptions` (the access guard: drop apps
	//      the user `account` has not subscribed to)
	//   3. $group / projection to deduplicate and shape per-app
	//   4. $lookup against `rooms` (decorate with per-user room info if
	//      the response shape requires it; today's spec returns model.App
	//      so room info is consumed inside the pipeline, not projected)
	//   5. $limit (this stage — keep, last)
	//   6. terminal $project matching model.App's bson tags
	_ = account // referenced in the access-guard $lookup once implemented

	return []bson.M{
		{"$match": matchStage},
		{"$skip": offset},
		{"$limit": limit},
	}
}
