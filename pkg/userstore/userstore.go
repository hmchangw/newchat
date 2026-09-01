// Package userstore resolves user records by id or account, behind a four-layer
// stack that keeps resolving them while MongoDB is unreachable.
//
// # The layers, outermost first
//
//   - Cache (cache.go) — a pod-local LRU+TTL, USER_CACHE_SIZE / USER_CACHE_TTL.
//   - l2Store (l2.go) — the shared Valkey tier, USER_L2_TTL, with refresh-on-read
//     and a TTL slide that keeps an entry alive when the source is down.
//   - breakerStore (breaker.go) — fences the Mongo read so a stalled server costs
//     one server-selection timeout rather than one per lookup.
//   - mongoStore (this file) — the source of truth.
//
// Resilient (l2.go) composes all four, and that is how callers should build it.
// The order is load-bearing rather than stylistic: the breaker must be INNERMOST
// and the caches outermost, so an open breaker still serves both cache tiers —
// during the outage that opened it they are the only tiers that can answer.
// Hand-wiring compiles just as well in the wrong order and silently loses outage
// survival, which is why the order lives in one constructor.
//
// # Why this package is not shaped like the other L2 tiers
//
// The six sibling tiers cache one value under one key and most are built on
// valkeyutil.Tier. This one is a store with three decorators, for two reasons
// that tier cannot express:
//
//   - A user is reachable through TWO key spaces, "user:id:{id}" and
//     "user:acct:{account}", each holding the whole record. Aliasing one to the
//     other would cost a second round trip, and in cluster mode the two keys hash
//     to different slots anyway.
//   - FindUsersByAccounts resolves a whole mention set in one MGET, folding the
//     stale entries into the batch the misses are already fetching. Tier is
//     single-key, single-value and has no bulk path.
//
// # Invalidation
//
// Bust (l2.go) drops both key spaces, and NOTHING CALLS IT YET: a rename is
// reconciled only by the TTL and by refresh-on-read. That is a known gap rather
// than a wired invariant, and it matters more than a stale name usually would,
// because message-worker persists the display name onto the immutable Cassandra
// message row. hr-sync-worker is the sole writer of engName/chineseName and so
// the single choke point that should call it. The pod-local L1 has no
// cross-process invalidation at all; its staleness is bounded by USER_CACHE_TTL.
//
// Not-found is never cached, in either tier: ErrUserNotFound is an answer from a
// healthy database, so it neither fills a cache entry nor counts against the
// breaker (see BreakerFailure).
package userstore

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

// ErrUserNotFound is returned by lookups when no user matches.
var ErrUserNotFound = errors.New("user not found")

// UserStore defines read operations for user records.
type UserStore interface {
	FindUserByID(ctx context.Context, id string) (*model.User, error)
	FindUserByAccount(ctx context.Context, account string) (*model.User, error)
	FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error)
}

// userProjection is the field set shared by the two account-keyed reads.
// sectName rides along for room-worker's member_added enrichment.
var userProjection = bson.M{"_id": 1, "account": 1, "siteId": 1, "engName": 1, "chineseName": 1, "employeeId": 1, "sectName": 1}

type mongoStore struct {
	col *mongo.Collection
}

// NewMongoStore returns a UserStore backed by the given MongoDB collection.
func NewMongoStore(col *mongo.Collection) UserStore {
	return &mongoStore{col: col}
}

// FindUserByID returns the user with the given ID, ErrUserNotFound on miss.
func (s *mongoStore) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	var u model.User
	if err := s.col.FindOne(ctx, bson.M{"_id": id}).Decode(&u); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find user %s: %w", id, ErrUserNotFound)
		}
		return nil, fmt.Errorf("find user %s: %w", id, err)
	}
	return &u, nil
}

// FindUserByAccount returns the user for the given account, ErrUserNotFound on miss.
func (s *mongoStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	opts := options.FindOne().SetProjection(userProjection)
	if err := s.col.FindOne(ctx, bson.M{"account": account}, opts).Decode(&u); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("find user by account %s: %w", account, ErrUserNotFound)
		}
		return nil, fmt.Errorf("find user by account %s: %w", account, err)
	}
	return &u, nil
}

// FindUsersByAccounts returns all users whose account field is in accounts.
func (s *mongoStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	filter := bson.M{"account": bson.M{"$in": accounts}}
	cursor, err := s.col.Find(ctx, filter, options.Find().SetProjection(userProjection))
	if err != nil {
		return nil, fmt.Errorf("find users by accounts: %w", err)
	}
	defer cursor.Close(ctx)
	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode users: %w", err)
	}
	return users, nil
}
