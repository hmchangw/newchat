package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessioncache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type storeMongo struct {
	users    *mongo.Collection
	sessions session.Store
	breaker  *circuitbreaker.Breaker
	// valkey is held for invalidation, not reads — the reads go through
	// sessionCache. A nil client makes every bust a no-op.
	valkey valkeyutil.Client
	// sessionCache serves validations Mongo already confirmed, so bots
	// authenticated before an outage keep working through it.
	sessionCache *sessioncache.Cache
}

func newStoreMongo(db *mongo.Database, breaker *circuitbreaker.Breaker, valkey valkeyutil.Client, sessionTTL time.Duration) *storeMongo {
	sessions := session.NewMongoStore(db)
	s := &storeMongo{
		users:    db.Collection("users"),
		sessions: sessions,
		breaker:  breaker,
		valkey:   valkey,
	}
	// Breaker innermost, cache outside it: an open breaker must still serve
	// cache hits, since during the outage that opened it they are the only
	// thing that can answer.
	s.sessionCache = sessioncache.New(func(ctx context.Context, hash string) (*session.Session, error) {
		return circuitbreaker.Do1(breaker, func() (*session.Session, error) {
			return sessions.FindByHash(ctx, hash)
		})
	}, valkey, sessionTTL)
	return s
}

// mongoBreakerFailure exempts the "healthy absence" sentinels: an unknown
// account, an unrecognised session or a missing subscription is an answer from
// a working Mongo, not evidence it is down. session.ErrNotFound especially —
// without it, a run of invalid bot tokens would open the breaker on its own and
// fence a database that is perfectly healthy.
var mongoBreakerFailure = mongoutil.BreakerFailure(model.ErrSubscriptionNotFound, session.ErrNotFound)

func (s *storeMongo) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	err := s.breaker.Do(func() error {
		return s.users.FindOne(ctx, bson.M{"account": account},
			options.FindOne().SetProjection(bson.M{
				"_id":                   1,
				"account":               1,
				"siteId":                1,
				"engName":               1,
				"chineseName":           1,
				"roles":                 1,
				"requirePasswordChange": 1,
				"services.password":     1,
				"active":                1,
			})).Decode(&u)
	})
	if err != nil {
		return nil, fmt.Errorf("find user by account: %w", err)
	}
	return &u, nil
}

func (s *storeMongo) InsertSession(ctx context.Context, sess *session.Session) error {
	return s.sessions.Insert(ctx, sess)
}

func (s *storeMongo) FindSessionByHash(ctx context.Context, hash string) (*session.Session, error) {
	return s.sessionCache.FindByHash(ctx, hash)
}

// DeleteSessionsBeyondCap evicts the over-cap rows and invalidates their cache
// entries. The bust is not optional bookkeeping: FindSessionByHash answers from
// sessionCache without touching Mongo until the entry's refresh window elapses,
// so an evicted token would otherwise keep authenticating — indefinitely during
// an outage, since the TTL slide re-arms it on every read.
func (s *storeMongo) DeleteSessionsBeyondCap(ctx context.Context, account string, max int) (int64, error) {
	if max < 0 {
		return 0, nil
	}
	evicted, err := s.sessions.DeleteBeyondCap(ctx, account, max)
	if err != nil {
		return 0, err
	}
	sessioncache.BustMany(ctx, s.valkey, evicted)
	return int64(len(evicted)), nil
}

func (s *storeMongo) Ping(ctx context.Context) error {
	if err := s.users.Database().Client().Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongo: %w", err)
	}
	return nil
}
