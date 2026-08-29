package pipelines

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

// SubscribedAccounts returns the subset of accounts that already have a
// subscription to roomID, via an indexed point read on (roomId, u.account).
// It is the "subtract already-subscribed" half of candidate resolution, shared
// by room-service (CountNewMembers) and room-worker (ListAddMemberCandidates)
// so it stays in lock-step with MatchCandidatesFilter — exactly the drift this
// package exists to prevent.
func SubscribedAccounts(ctx context.Context, subscriptions *mongo.Collection, roomID string, accounts []string) (map[string]struct{}, error) {
	cursor, err := subscriptions.Find(ctx,
		bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"u.account": 1, "_id": 0}))
	if err != nil {
		return nil, fmt.Errorf("find existing subscriptions for room %q: %w", roomID, err)
	}
	var rows []struct {
		User struct {
			Account string `bson:"account"`
		} `bson:"u"`
	}
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode existing subscriptions: %w", err)
	}
	set := make(map[string]struct{}, len(rows))
	for _, r := range rows {
		set[r.User.Account] = struct{}{}
	}
	return set, nil
}

// botSuffixRegex is the wire-side equivalent of model.IsBot: it matches the
// ".bot" account suffix and nothing else. Unlike botOrPseudoAccountRegex it
// deliberately excludes the platform-admin prefix — a p_admin DM is an ordinary
// DM, not an app room.
const botSuffixRegex = `\.bot$`

// AppRoomFilter matches subscription rows that are app rooms: a botDM facing a
// real ".bot" app. It is the wire-side twin of model.IsAppRoom, and the only
// shape the isSubscribed soft-unsubscribe gate still applies to.
//
// The regex is never the index-driving term — every call site leads with a
// selective u.account match or an (u.account, roomId) point read — so it is
// evaluated only over candidate documents. The returned map is freshly built
// and safe for the caller to mutate.
func AppRoomFilter() bson.M {
	return bson.M{
		"roomType": string(model.RoomTypeBotDM),
		"name":     bson.Regex{Pattern: botSuffixRegex},
	}
}

// UnsubscribedAppFilter matches the one shape any subscriber-facing read must
// hide: an app room the user has unsubscribed from. Unsubscribing is a soft
// toggle (isSubscribed=false, row retained) rather than a row delete, so it
// needs a filter rather than absence.
//
// $ne:true rather than false so a legacy row with no isSubscribed field counts
// as unsubscribed, matching the equality test it replaces. Negate it with $nor
// to express "still accessible": ¬(app ∧ ¬subscribed) ≡ ¬app ∨ subscribed, which
// admits every non-app botDM — the bot's own side of a bot<->human DM, and
// either side of a p_admin DM, both written isSubscribed=false by construction.
func UnsubscribedAppFilter() bson.M {
	f := AppRoomFilter()
	f["isSubscribed"] = bson.M{"$ne": true}
	return f
}
