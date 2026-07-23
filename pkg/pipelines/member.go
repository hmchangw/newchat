// Package pipelines holds shared MongoDB candidate-resolution helpers used by
// more than one service — the query predicates (MatchCandidatesFilter and its
// direct-bots variant) and the companion read (SubscribedAccounts).
// Centralizing them keeps room-service and room-worker in lock-step on org
// expansion and the already-subscribed subtraction; bot handling deliberately
// differs per caller (capacity/create exclude bots everywhere, member.add
// resolution admits explicitly listed ones — see each predicate's doc).
package pipelines

import (
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
)

// botOrPseudoAccountRegex is the wire-side equivalent of model.IsBot /
// model.IsPlatformAdminAccount: it matches bot (".bot" suffix) and platform-admin
// (prefix QuoteMeta-escaped) accounts. Plain "p_" QA accounts are NOT matched.
func botOrPseudoAccountRegex() string {
	return `(\.bot$|^` + regexp.QuoteMeta(model.PlatformAdminAccountPrefix()) + `)`
}

// MatchCandidatesFilter returns the query predicate selecting the users an
// add/create-member request resolves to: members of any of orgIDs (by sectId or
// deptId) OR any of directAccounts, excluding bot/pseudo accounts and — when
// non-empty — excludeAccount.
//
// It is a plain find/count predicate, not an aggregation stage. Callers resolve
// each candidate's membership state with separate indexed reads (subscriptions
// keyed on (roomId, u.account), room_members on (rid, member.type, member.id))
// rather than a correlated $lookup, which keeps every per-add query a bounded
// indexed lookup instead of an aggregation pipeline.
//
// At least one of orgIDs/directAccounts must be non-empty: an all-empty call
// produces a {$or: []} predicate that MongoDB rejects, so callers guard first.
func MatchCandidatesFilter(orgIDs, directAccounts []string, excludeAccount string) bson.M {
	orFilter := bson.A{}
	if len(orgIDs) > 0 {
		orFilter = append(orFilter, bson.M{"sectId": bson.M{"$in": orgIDs}}, bson.M{"deptId": bson.M{"$in": orgIDs}})
	}
	if len(directAccounts) > 0 {
		orFilter = append(orFilter, bson.M{"account": bson.M{"$in": directAccounts}})
	}
	accountFilter := bson.M{"$not": bson.Regex{Pattern: botOrPseudoAccountRegex(), Options: ""}}
	if excludeAccount != "" {
		accountFilter["$ne"] = excludeAccount
	}
	return bson.M{"$or": orFilter, "account": accountFilter}
}

// MatchCandidatesFilterWithDirectBots narrows the bot exclusion to the org
// arms: directly named accounts may be bots (member.add validated them), while
// org expansion stays bot-free. Same non-empty-input contract.
func MatchCandidatesFilterWithDirectBots(orgIDs, directAccounts []string, excludeAccount string) bson.M {
	notBot := bson.M{"$not": bson.Regex{Pattern: botOrPseudoAccountRegex(), Options: ""}}
	orFilter := bson.A{}
	if len(orgIDs) > 0 {
		orFilter = append(orFilter,
			bson.M{"sectId": bson.M{"$in": orgIDs}, "account": notBot},
			bson.M{"deptId": bson.M{"$in": orgIDs}, "account": notBot},
		)
	}
	if len(directAccounts) > 0 {
		orFilter = append(orFilter, bson.M{"account": bson.M{"$in": directAccounts}})
	}
	filter := bson.M{"$or": orFilter}
	if excludeAccount != "" {
		filter["account"] = bson.M{"$ne": excludeAccount}
	}
	return filter
}
