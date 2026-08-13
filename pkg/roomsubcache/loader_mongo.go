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

// NewMongoLoader returns the canonical Loader over the subscriptions
// collection. It is the only sanctioned production loader: every Member field
// is filled, so the entry it writes is safe for any service reading the shared
// key — including the ones that gate on Muted and HistorySharedSince.
func NewMongoLoader(subscriptions *mongo.Collection) Loader {
	return func(ctx context.Context, roomID string) ([]Member, error) {
		cur, err := subscriptions.Find(ctx, bson.M{"roomId": roomID}, memberFindOpts)
		if err != nil {
			return nil, fmt.Errorf("find subscriptions for room %s: %w", roomID, err)
		}
		defer cur.Close(ctx)

		var out []Member
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
		return out, nil
	}
}
