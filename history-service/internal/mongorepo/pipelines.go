package mongorepo

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// accessSince gates to threads whose parent was created at or after the user's join time.
func buildBaseThreadMatch(roomID string, accessSince *time.Time) bson.M {
	match := bson.M{"roomId": roomID}
	if accessSince != nil {
		match["threadParentCreatedAt"] = bson.M{"$gte": *accessSince}
	}
	return match
}

func allThreadsPipeline(roomID string, accessSince *time.Time) bson.A {
	return bson.A{
		bson.D{{Key: "$match", Value: buildBaseThreadMatch(roomID, accessSince)}},
		bson.D{{Key: "$sort", Value: threadRoomSort}},
	}
}

func followingThreadsPipeline(roomID, account string, accessSince *time.Time) bson.A {
	match := buildBaseThreadMatch(roomID, accessSince)
	match["replyAccounts"] = account
	return bson.A{
		bson.D{{Key: "$match", Value: match}},
		bson.D{{Key: "$sort", Value: threadRoomSort}},
	}
}

// userThreadSubscriptionsPipeline lists one user's thread subscriptions, newest
// activity first, after the (lastMsgAt, threadRoomId) value cursor. It is driven
// from thread_subscriptions (the per-user filter) and joins thread_rooms for the
// activity/parent fields.
//
// $lookup justification: two joins, none avoidable.
//  1. subscriptions (membership) runs FIRST, on the thread_subscription's own
//     roomId — the room subscription, not the thread subscription, is the source
//     of truth for whether the user still belongs to the room (purged on leave;
//     thread_subscriptions rows are not). For botDM rooms, where unsubscribe is a
//     soft toggle that retains the row, the same join also gates on isSubscribed so
//     an unsubscribed app's threads drop out. Filtering here, before $limit, keeps
//     the page exact; doing it before the thread_rooms join means that join runs
//     only for accessible threads. Indexed point read on (u.account, roomId). This
//     join also carries roomName and roomType: the subscription holds the
//     per-subscriber display name (dm/botDM room docs have none) and the roomType,
//     so the page needs no rooms lookup.
//  2. thread_rooms supplies the inbox sort key (lastMsgAt) and parent/activity
//     fields, which live there rather than on thread_subscriptions, so we must
//     sort and paginate on the looked-up field. Denormalizing lastMsgAt onto every
//     subscription was rejected because it would write-amplify across all
//     subscribers on every reply (see docs/design/user-thread-list.md §5).
func userThreadSubscriptionsPipeline(account string, cursorLastMsgAt *time.Time, cursorThreadRoomID string, limit int) bson.A {
	pipeline := bson.A{
		bson.D{{Key: "$match", Value: bson.M{"userAccount": account}}},
		// Membership filter — join 1 above: keyed on the thread_subscription's own
		// roomId so it runs before the thread_rooms/rooms joins.
		bson.D{{Key: "$lookup", Value: bson.M{
			"from": subscriptionsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.D{{Key: "$match", Value: bson.M{
					"u.account": account,
					"$expr":     bson.M{"$eq": bson.A{"$roomId", "$$rid"}},
					// botDM unsubscribe is a soft toggle (isSubscribed=false, row
					// retained), unlike a room leave that purges the row. Gate botDM
					// rooms on isSubscribed so an unsubscribed app's threads drop out
					// of the inbox; channel/dm/discussion rooms pass through untouched.
					"$or": bson.A{
						bson.M{"roomType": bson.M{"$ne": "botDM"}},
						bson.M{"isSubscribed": true},
					},
				}}},
				bson.D{{Key: "$project", Value: bson.M{"_id": 1, "name": 1, "roomType": 1}}},
			},
			"as": "sub",
		}}},
		// {$ne: []} — $lookup sets "sub" to [] when no subscription matched; non-empty means subscribed.
		bson.D{{Key: "$match", Value: bson.M{"sub": bson.M{"$ne": bson.A{}}}}},
		// roomName and roomType come from the user's own subscription, not a room doc:
		// dm/botDM rooms store an empty room name (the display name is per-subscriber),
		// and the subscription already carries roomType — so no rooms lookup is needed.
		// Lifted to scalars here so the sub array can be dropped before $sort.
		bson.D{{Key: "$set", Value: bson.M{
			"roomName": bson.M{"$arrayElemAt": bson.A{"$sub.name", 0}},
			"roomType": bson.M{"$arrayElemAt": bson.A{"$sub.roomType", 0}},
		}}},
		bson.D{{Key: "$project", Value: bson.M{"sub": 0}}}, // drop before $sort — mirrors unreadThreadsPipeline
		bson.D{{Key: "$lookup", Value: bson.M{
			"from":         threadRoomsCollection,
			"localField":   "threadRoomId",
			"foreignField": "_id",
			"as":           "tr",
			"pipeline": bson.A{
				bson.D{{Key: "$project", Value: bson.M{
					"lastMsgAt": 1, "lastMsgId": 1, "parentMessageId": 1, "roomId": 1, "siteId": 1,
				}}},
			},
		}}},
		bson.D{{Key: "$unwind", Value: "$tr"}},
	}
	// Value cursor: items strictly older than the cursor in (lastMsgAt, threadRoomId) DESC order.
	if cursorLastMsgAt != nil {
		pipeline = append(pipeline, bson.D{{Key: "$match", Value: bson.M{
			"$or": bson.A{
				bson.M{"tr.lastMsgAt": bson.M{"$lt": *cursorLastMsgAt}},
				bson.M{"tr.lastMsgAt": *cursorLastMsgAt, "threadRoomId": bson.M{"$lt": cursorThreadRoomID}},
			},
		}}})
	}
	pipeline = append(pipeline,
		bson.D{{Key: "$sort", Value: bson.D{{Key: "tr.lastMsgAt", Value: -1}, {Key: "threadRoomId", Value: -1}}}},
		bson.D{{Key: "$limit", Value: int64(limit + 1)}},
		// roomName/roomType both ride in on the membership subscription (join 1), so
		// the page needs no rooms lookup.
		bson.D{{Key: "$project", Value: bson.M{
			"_id":             "$threadRoomId",
			"roomId":          "$tr.roomId",
			"siteId":          "$tr.siteId",
			"roomName":        1, // sourced from the subscription above, not a room doc
			"roomType":        1, // sourced from the subscription above, not a room doc
			"parentMessageId": "$tr.parentMessageId",
			"lastMsgId":       "$tr.lastMsgId",
			"lastMsgAt":       "$tr.lastMsgAt",
			"lastSeenAt":      1,
			"hasMention":      1,
		}}},
	)
	return pipeline
}

// Unread = subscribed AND lastMsgAt > lastSeenAt (nil lastSeenAt = never seen = always unread).
func unreadThreadsPipeline(roomID, userAccount string, accessSince *time.Time) bson.A {
	match := buildBaseThreadMatch(roomID, accessSince)
	return bson.A{
		bson.D{{Key: "$match", Value: match}},
		bson.D{{Key: "$lookup", Value: bson.M{
			"from": "thread_subscriptions",
			"let":  bson.M{"tr": "$_id"},
			"pipeline": bson.A{
				bson.D{{Key: "$match", Value: bson.M{
					"$expr":       bson.M{"$eq": bson.A{"$threadRoomId", "$$tr"}},
					"userAccount": userAccount,
				}}},
				bson.D{{Key: "$project", Value: bson.M{"lastSeenAt": 1, "_id": 0}}},
			},
			"as": "sub",
		}}},
		// {$ne: []} — $lookup sets "sub" to [] when no subscription matched; non-empty means subscribed.
		bson.D{{Key: "$match", Value: bson.M{"sub": bson.M{"$ne": bson.A{}}}}},
		// null is the smallest BSON value, so $gt:[lastMsgAt, null] is true for any
		// non-null lastMsgAt — threads with nil lastSeenAt (never seen) are included.
		bson.D{{Key: "$match", Value: bson.M{
			"$expr": bson.M{"$gt": bson.A{"$lastMsgAt", bson.M{"$arrayElemAt": bson.A{"$sub.lastSeenAt", 0}}}},
		}}},
		bson.D{{Key: "$project", Value: bson.M{"sub": 0}}},
		bson.D{{Key: "$sort", Value: threadRoomSort}},
	}
}
