package roomsubcache

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

// memberFindOpts is built once: the loader runs per room, and rebuilding the
// projection on every call allocates for no benefit.
var memberFindOpts = options.Find().SetProjection(bson.M{
	"u._id":              1,
	"u.account":          1,
	"u.isBot":            1,
	"roomType":           1,
	"muted":              1,
	"historySharedSince": 1,
})

// homeSiteFindOpts projects the users lookup down to the two fields the
// home-site fill needs. Built once for the same reason as memberFindOpts.
var homeSiteFindOpts = options.Find().SetProjection(bson.M{
	"_id": 0, "account": 1, "siteId": 1,
})

// NewMongoLoader returns the canonical Loader over the subscriptions
// collection. It is the only sanctioned production loader: every Member field
// is filled, so the entry it writes is safe for any service reading the shared
// key — including the ones that gate on Muted and HistorySharedSince, and
// notification-worker, which groups by HomeSiteID for the per-site badge RPC.
//
// users is the users collection, needed because HomeSiteID lives there rather
// than on the subscription (see Member.HomeSiteID). It costs one batched $in
// per cache fill. Passing nil skips the fill and leaves HomeSiteID empty —
// acceptable only for a caller that owns the key alone, which no production
// caller does, since the key is shared.
func NewMongoLoader(subscriptions, users *mongo.Collection) Loader {
	return func(ctx context.Context, roomID string) ([]Member, error) {
		cur, err := subscriptions.Find(ctx, bson.M{"roomId": roomID}, memberFindOpts)
		if err != nil {
			return nil, fmt.Errorf("find subscriptions for room %s: %w", roomID, err)
		}
		defer cur.Close(ctx)

		// Capacity hint: this is the cold-fill path for rooms documented as having
		// thousands of members, so an unhinted append reallocates and copies the
		// whole slice ~log2(N) times per fill.
		out := make([]Member, 0, 128)
		for cur.Next(ctx) {
			var doc struct {
				User struct {
					ID      string `bson:"_id"`
					Account string `bson:"account"`
					IsBot   bool   `bson:"isBot"`
				} `bson:"u"`
				RoomType           model.RoomType `bson:"roomType"`
				Muted              bool           `bson:"muted"`
				HistorySharedSince *time.Time     `bson:"historySharedSince"`
			}
			if err := cur.Decode(&doc); err != nil {
				return nil, fmt.Errorf("decode subscription: %w", err)
			}
			var hssMs *int64
			if doc.HistorySharedSince != nil {
				ms := doc.HistorySharedSince.UnixMilli()
				hssMs = &ms
			}
			out = append(out, Member{
				ID:                 doc.User.ID,
				Account:            doc.User.Account,
				RoomType:           doc.RoomType,
				IsBot:              doc.User.IsBot,
				Muted:              doc.Muted,
				HistorySharedSince: hssMs,
			})
		}
		if err := cur.Err(); err != nil {
			return nil, fmt.Errorf("iterate subscriptions: %w", err)
		}
		if err := fillHomeSites(ctx, users, roomID, out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

// fillHomeSites stamps each member's HomeSiteID from the users collection
// ({account $in members}, projected to {account, siteId}). An account missing
// from users leaves HomeSiteID empty — that member degrades to no badge count
// downstream, which is preferable to failing the whole fill. A lookup error
// does fail the fill: a half-stamped list written to the shared key would
// misroute every member it could not resolve, for the full TTL.
func fillHomeSites(ctx context.Context, users *mongo.Collection, roomID string, members []Member) error {
	if users == nil || len(members) == 0 {
		return nil
	}
	accounts := make([]string, 0, len(members))
	for i := range members {
		accounts = append(accounts, members[i].Account)
	}
	cur, err := users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}}, homeSiteFindOpts)
	if err != nil {
		return fmt.Errorf("find home sites for room %s members: %w", roomID, err)
	}
	defer cur.Close(ctx)

	siteByAccount := make(map[string]string, len(accounts))
	for cur.Next(ctx) {
		var doc struct {
			Account string `bson:"account"`
			SiteID  string `bson:"siteId"`
		}
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode user home site: %w", err)
		}
		siteByAccount[doc.Account] = doc.SiteID
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate user home sites: %w", err)
	}
	for i := range members {
		members[i].HomeSiteID = siteByAccount[members[i].Account]
	}
	return nil
}
