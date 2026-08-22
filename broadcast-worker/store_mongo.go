package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/roomsubcache"
	"github.com/hmchangw/chat/pkg/userstore"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// EnsureIndexes creates indexes that back the store's read paths.
// Must be called once at startup; index creation is idempotent when the key
// spec matches.
func (m *mongoStore) EnsureIndexes(ctx context.Context) error {
	if _, err := m.threadRoomCol.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "parentMessageId", Value: 1}, {Key: "siteId", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_rooms (parentMessageId, siteId) index: %w", err)
	}
	return nil
}

type mongoStore struct {
	roomCol       *mongo.Collection
	subCol        *mongo.Collection
	threadRoomCol *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	metaRec       roommetacache.Recorder
	metaOpts      []roommetacache.ReadThroughOption
	members       *roomsubcache.Lookup
	// breaker fences the reads that have no cache tier of their own. Nil is
	// "protection off" — circuitbreaker.Do1 passes through — so tests and a
	// breaker-less config both work without a branch at each call site.
	breaker *circuitbreaker.Breaker
}

func NewMongoStore(roomCol, subCol, threadRoomCol, userCol *mongo.Collection, valkey valkeyutil.Client, metaTTL, subTTL time.Duration, mongoBreaker *circuitbreaker.Breaker) *mongoStore {
	// A nil valkey leaves the Lookup cacheless (straight to Mongo). The loader
	// is always the shared full-projection one: notification-worker reads the
	// same key and gates on Muted/HistorySharedSince, so a partial write here
	// would silently unmute users and widen their history windows. userCol is
	// what lets it stamp HomeSiteID too — without it a cold fill won here hands
	// notification-worker an entry that misroutes its per-site badge RPC.
	var subCache roomsubcache.Cache
	if valkey != nil {
		subCache = roomsubcache.NewValkeyCache(valkey)
	}
	s := &mongoStore{
		roomCol:       roomCol,
		subCol:        subCol,
		threadRoomCol: threadRoomCol,
		valkey:        valkey,
		metaTTL:       metaTTL,
		metaRec:       cachemetrics.For("roommeta", "l2"),
		members: roomsubcache.NewLookup(subCache,
			roomsubcache.GuardLoader(roomsubcache.NewMongoLoader(subCol, userCol), mongoBreaker), subTTL),
		breaker: mongoBreaker,
	}
	if mongoBreaker != nil {
		s.metaOpts = []roommetacache.ReadThroughOption{roommetacache.WithFetchGuard(mongoBreaker.Do)}
	}
	return s
}

// mongoBreakerFailure is the failure predicate this service's single Mongo
// breaker must be built with. It exempts every "healthy absence" sentinel the
// fenced call sites can return — a missing document or a missing user is an
// answer from a working Mongo, not evidence it is unwell. A new fenced call
// site with its own not-found sentinel must be added here rather than given a
// breaker of its own, or it re-splits the failure budget.
var mongoBreakerFailure = circuitbreaker.FailureExcept(mongo.ErrNoDocuments, userstore.ErrUserNotFound)

// GetRoom backs the edit path, which has no cache tier to fall back on. It is
// fenced so a Mongo outage fast-fails instead of spending a server-selection
// timeout on every one of the OutageRetryBudget's redeliveries.
func (m *mongoStore) GetRoom(ctx context.Context, roomID string) (*model.Room, error) {
	return circuitbreaker.Do1(m.breaker, func() (*model.Room, error) {
		filter := bson.M{"_id": roomID}
		var room model.Room
		if err := m.roomCol.FindOne(ctx, filter).Decode(&room); err != nil {
			return nil, fmt.Errorf("find room %s: %w", roomID, err)
		}
		return &room, nil
	})
}

// ListRoomMembers reads through the shared roomsubcache. The Lookup owns the
// Mongo fallback, so during an outage a warm room still fans out from L2.
func (m *mongoStore) ListRoomMembers(ctx context.Context, roomID string) ([]roomsubcache.Member, error) {
	members, err := m.members.GetMembers(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("list members for room %s: %w", roomID, err)
	}
	return members, nil
}

// GetRoomMeta fences only the Mongo fetch, never the L2 read in front of it: an
// open breaker must still serve cached rooms, since during the outage that
// opened it the L2 is the only tier that can answer — and this read gates
// delivery for every message in the room.
func (m *mongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	return roommetacache.ReadThrough(ctx, m.valkey, m.roomCol, roomID, m.metaTTL, m.metaRec, m.metaOpts...)
}

// GetThreadFollowers gates hidden-thread-reply delivery and, like GetRoom, has
// no cache in front of it, so it rides the breaker too. A thread with no
// followers resolves inside the fence to an empty set and a nil error: that is
// Mongo answering, and it must read as a success rather than pressure.
func (m *mongoStore) GetThreadFollowers(ctx context.Context, parentMessageID string) (map[string]struct{}, error) {
	return circuitbreaker.Do1(m.breaker, func() (map[string]struct{}, error) {
		var doc struct {
			ReplyAccounts []string `bson:"replyAccounts"`
		}
		opts := options.FindOne().SetProjection(bson.M{"replyAccounts": 1, "_id": 0})
		err := m.threadRoomCol.FindOne(ctx, bson.M{"parentMessageId": parentMessageID}, opts).Decode(&doc)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return map[string]struct{}{}, nil
			}
			return nil, fmt.Errorf("find thread room by parent %s: %w", parentMessageID, err)
		}
		out := make(map[string]struct{}, len(doc.ReplyAccounts))
		for _, a := range doc.ReplyAccounts {
			if a != "" {
				out[a] = struct{}{}
			}
		}
		return out, nil
	})
}

// GetHistorySharedSince is fenced for the same reason as its siblings. The
// empty-accounts short-circuit stays OUTSIDE the fence: it issues no query, so
// letting it report success would hold the breaker closed on evidence that
// never touched Mongo.
func (m *mongoStore) GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error) {
	if len(accounts) == 0 {
		return map[string]*time.Time{}, nil
	}
	return circuitbreaker.Do1(m.breaker, func() (map[string]*time.Time, error) {
		filter := bson.M{"roomId": roomID, "u.account": bson.M{"$in": accounts}}
		opts := options.Find().SetProjection(bson.M{"u.account": 1, "historySharedSince": 1, "_id": 0})
		cursor, err := m.subCol.Find(ctx, filter, opts)
		if err != nil {
			return nil, fmt.Errorf("query history windows for room %s: %w", roomID, err)
		}
		defer cursor.Close(ctx)
		// Minimal decode shape: the projection returns only u.account + historySharedSince,
		// so decode just those rather than the full model.SubscriptionUser (whose other
		// fields would silently be zero-valued).
		var rows []struct {
			User struct {
				Account string `bson:"account"`
			} `bson:"u"`
			HistorySharedSince *time.Time `bson:"historySharedSince"`
		}
		if err := cursor.All(ctx, &rows); err != nil {
			return nil, fmt.Errorf("decode history windows: %w", err)
		}
		out := make(map[string]*time.Time, len(accounts))
		for i := range rows {
			out[rows[i].User.Account] = rows[i].HistorySharedSince
		}
		return out, nil
	})
}
