package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// Compile-time proof each Mongo type still satisfies the contract in store.go.
var (
	_ MemberStore             = (*mongoMemberStore)(nil)
	_ ThreadFollowerLister    = (*mongoThreadFollowers)(nil)
	_ UserSettingsSnapshotter = (*mongoUserSettings)(nil)
)

// mongoMemberStore loads a room's member list and stamps each member's HOME
// site from the users collection (one batch $in per cache fill). Not
// Subscription.siteId — that is the ROOM's home site, which would misroute
// badge RPCs. This deliberately revises the "no users-collection lookups"
// contract (docs/notification-worker-downstream-contracts.md §3), cache-fill
// time only.
type mongoMemberStore struct {
	col   *mongo.Collection // subscriptions
	users *mongo.Collection // users — home-site (siteId) lookup
}

func (m *mongoMemberStore) ListMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	projection := bson.M{
		"u._id":              1,
		"u.account":          1,
		"u.isBot":            1,
		"roomType":           1,
		"muted":              1,
		"historySharedSince": 1,
	}
	cur, err := m.col.Find(ctx, bson.M{"roomId": roomID}, options.Find().SetProjection(projection))
	if err != nil {
		return nil, fmt.Errorf("find subscriptions for room %s: %w", roomID, err)
	}
	defer cur.Close(ctx)

	var out []roomsubcache.Member
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
		out = append(out, roomsubcache.Member{
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
	if err := m.fillHomeSites(ctx, roomID, out); err != nil {
		return nil, err
	}
	return out, nil
}

// fillHomeSites stamps each member's HomeSiteID from the users collection
// ({account $in members}, projected to {account, siteId}). An account missing
// from users leaves HomeSiteID empty — that member degrades to no badge count
// downstream rather than misrouting the RPC.
func (m *mongoMemberStore) fillHomeSites(ctx context.Context, roomID string, members []roomsubcache.Member) error {
	if len(members) == 0 {
		return nil
	}
	accounts := make([]string, 0, len(members))
	for i := range members {
		accounts = append(accounts, members[i].Account)
	}
	cur, err := m.users.Find(ctx, bson.M{"account": bson.M{"$in": accounts}},
		options.Find().SetProjection(bson.M{"_id": 0, "account": 1, "siteId": 1}))
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

type mongoThreadFollowers struct {
	col *mongo.Collection
}

func newMongoThreadFollowers(col *mongo.Collection) *mongoThreadFollowers {
	return &mongoThreadFollowers{col: col}
}

func (m *mongoThreadFollowers) Lookup(ctx context.Context, parentMessageID string) (ThreadRoomInfo, error) {
	if parentMessageID == "" {
		return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
	}
	var doc struct {
		ReplyAccounts []string `bson:"replyAccounts"`
	}
	opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "_id": 0})
	err := m.col.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return ThreadRoomInfo{Followers: map[string]struct{}{}}, nil
		}
		return ThreadRoomInfo{}, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
	}
	out := make(map[string]struct{}, len(doc.ReplyAccounts))
	for _, a := range doc.ReplyAccounts {
		if a != "" {
			out[a] = struct{}{}
		}
	}
	return ThreadRoomInfo{Followers: out}, nil
}

// userSettingsProjection takes only the four gated fields. Deliberately NOT the
// whole-sub-document {"settings":1} projection user-service's fanouts need —
// nothing here re-publishes the settings object.
var userSettingsProjection = bson.M{
	"_id":                           0,
	"account":                       1,
	"settings.muteAllNotifications": 1,
	"settings.alwaysAllowPriorityNotifications": 1,
	"settings.showNotificationsInCall":          1,
	"settings.priorityContacts":                 1,
}

// mongoUserSettings reads notification settings straight from the users
// collection — one indexed $in per chunk, no cache. Per-user keys make a Valkey
// tier strictly worse than this (one round trip per candidate, and per-account
// keys cross cluster slots), so there is no L2 here by design.
type mongoUserSettings struct {
	col       *mongo.Collection
	batchSize int
	timeout   time.Duration
}

func newMongoUserSettings(col *mongo.Collection, batchSize int, timeout time.Duration) *mongoUserSettings {
	if batchSize <= 0 {
		batchSize = 512
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &mongoUserSettings{col: col, batchSize: batchSize, timeout: timeout}
}

// Snapshot returns settings keyed by account. It never returns an error: a failed
// or timed-out read yields whatever was collected so far, and absent accounts take
// the zero notifSettings, i.e. today's behaviour. See the spec on why this gate
// fails open like its two neighbours rather than silencing the site.
func (m *mongoUserSettings) Snapshot(ctx context.Context, accounts []string) (map[string]notifSettings, error) {
	out := make(map[string]notifSettings, len(accounts))
	if len(accounts) == 0 {
		return out, nil
	}
	// Bound the new dependency rather than inheriting the consumer's deadline.
	qctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()

	// Chunks run sequentially under one shared timeout, unlike bulkPresenceSource's
	// concurrent fan-out — deliberate, since this read is a single indexed $in and
	// not worth a goroutine per chunk. Consequence: if the shared timeout expires
	// mid-loop, chunks nearer the end of a very large recipient list are the ones
	// that never get read and fail open.
	for _, chunk := range chunkStrings(accounts, m.batchSize) {
		if err := m.appendChunk(qctx, chunk, out); err != nil {
			slog.Warn("user settings lookup failed, defaulting to push",
				"error", err, "chunk", len(chunk), "request_id", natsutil.RequestIDFromContext(ctx))
			return out, nil
		}
	}
	return out, nil
}

func (m *mongoUserSettings) appendChunk(ctx context.Context, chunk []string, out map[string]notifSettings) error {
	// active:{$ne:false} matches activeUserFilter in user-service so this read
	// agrees with user-service about what an active user is.
	filter := bson.M{"account": bson.M{"$in": chunk}, "active": bson.M{"$ne": false}}
	cur, err := m.col.Find(ctx, filter, options.Find().SetProjection(userSettingsProjection))
	if err != nil {
		return fmt.Errorf("find user settings: %w", err)
	}
	// Close error intentionally discarded: the cursor is being abandoned either
	// way, and Snapshot is fail-open by contract, so a close failure here
	// changes nothing observable to the caller.
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		var doc struct {
			Account  string              `bson:"account"`
			Settings *model.UserSettings `bson:"settings"`
		}
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode user settings: %w", err)
		}
		if doc.Account == "" {
			continue
		}
		out[doc.Account] = resolveNotifSettings(doc.Settings)
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("iterate user settings: %w", err)
	}
	return nil
}
