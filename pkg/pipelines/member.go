// Package pipelines holds shared MongoDB aggregation pipelines used by more
// than one service. Putting them here lets each service append its own
// terminal stage (e.g. $count vs. $group) without duplicating the leading
// stages.
package pipelines

import "go.mongodb.org/mongo-driver/v2/bson"

// GetNewMembersPipeline returns the common stages for finding the unique,
// non-bot, not-already-subscribed users that an add-members request would
// add to roomID, given org IDs and direct account names.
//
// Pipeline target: the users collection.
//
// Stages:
//  1. $match: account in directAccounts OR sectId in orgIDs, AND account
//     does not match the bot regex (`.bot$|^p_`).
//  2. $lookup: existing subscription documents for (account, roomID), with
//     a $limit:1 sub-pipeline so we only need a yes/no answer.
//  3. $match: keep only users where existingSub is the empty array (i.e.,
//     no subscription exists for that account in roomID).
//
// Callers MUST append a terminal stage that fits their need:
//   - room-service: bson.M{"$count": "n"}                                (capacity check)
//   - room-worker:  bson.M{"$group": {"_id": nil, "accounts": {"$addToSet": "$account"}}}
func GetNewMembersPipeline(orgIDs, directAccounts []string, roomID string) bson.A {
	orFilter := bson.A{}
	if len(orgIDs) > 0 {
		orFilter = append(orFilter, bson.M{"sectId": bson.M{"$in": orgIDs}})
	}
	if len(directAccounts) > 0 {
		orFilter = append(orFilter, bson.M{"account": bson.M{"$in": directAccounts}})
	}

	return bson.A{
		bson.M{"$match": bson.M{
			"$or":     orFilter,
			"account": bson.M{"$not": bson.Regex{Pattern: `(\.bot$|^p_)`, Options: ""}},
		}},
		bson.M{"$lookup": bson.M{
			"from": "subscriptions",
			"let":  bson.M{"userAccount": "$account"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{"$expr": bson.M{"$and": bson.A{
					bson.M{"$eq": bson.A{"$roomId", roomID}},
					bson.M{"$eq": bson.A{"$u.account", "$$userAccount"}},
				}}}},
				bson.M{"$limit": 1},
			},
			"as": "existingSub",
		}},
		bson.M{"$match": bson.M{"existingSub": bson.M{"$eq": bson.A{}}}},
	}
}
