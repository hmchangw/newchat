// Package pipelines holds shared MongoDB candidate-resolution helpers used by
// more than one service — both the query predicate (MatchCandidatesFilter) and
// the companion read (SubscribedAccounts). Centralizing them keeps room-service
// (capacity check) and room-worker (candidate resolution) in lock-step on org
// expansion, bot exclusion, and the already-subscribed subtraction.
package pipelines

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

// botOrPseudoAccountRegex matches bot (".bot" suffix) and pseudo ("p_" prefix)
// accounts. It is the wire-side equivalent of model.IsBot / model.IsPlatformAdminAccount,
// applied as a residual filter so org-expanded candidates — whose accounts the
// caller does not know up front — are excluded server-side.
const botOrPseudoAccountRegex = `(\.bot$|^p_)`

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
	accountFilter := bson.M{"$not": bson.Regex{Pattern: botOrPseudoAccountRegex, Options: ""}}
	if excludeAccount != "" {
		accountFilter["$ne"] = excludeAccount
	}
	return bson.M{"$or": orFilter, "account": accountFilter}
}
