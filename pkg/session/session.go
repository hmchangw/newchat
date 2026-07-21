// Package session owns the shared "one row per issued token" record used by
// both admin-service and botplatform-service. Both services write/read the
// same Mongo collection; the shape lives here so it can't drift.
package session

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection is the Mongo collection name. Constant so a rename can't silently
// diverge between the two services that share the collection.
const Collection = "sessions"

// Session is the one-doc-per-token record. IDs are sessiontoken.Hash(rawToken);
// no plaintext token ever hits Mongo.
type Session struct {
	ID       string   `bson:"_id"`
	UserID   string   `bson:"userId"`
	Account  string   `bson:"account"`
	SiteID   string   `bson:"siteId"`
	Roles    []string `bson:"roles"`
	IssuedAt int64    `bson:"issuedAt"`
}

// Store is the narrow Mongo surface both services share.
type Store interface {
	Insert(ctx context.Context, s *Session) error
	FindByHash(ctx context.Context, hash string) (*Session, error)
	DeleteBeyondCap(ctx context.Context, account string, max int) (int64, error)
	DeleteForAccountExcept(ctx context.Context, siteID, account, exceptID string) (int64, error)
	DeleteForAccount(ctx context.Context, siteID, account string) (int64, error)
	ListForAccount(ctx context.Context, siteID, account string) ([]Session, error)
	DeleteByID(ctx context.Context, siteID, account, id string) (int64, error)
	EnsureIndexes(ctx context.Context) error
}

var ErrNotFound = errors.New("session not found")

// MongoStore implements Store against MongoDB. The DB is what the caller
// wants (production or test); collection selection is fixed at Collection.
type MongoStore struct {
	coll *mongo.Collection
}

func NewMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{coll: db.Collection(Collection)}
}

var projection = bson.M{"_id": 1, "userId": 1, "account": 1, "siteId": 1, "roles": 1, "issuedAt": 1}

func (s *MongoStore) Insert(ctx context.Context, sess *Session) error {
	if _, err := s.coll.InsertOne(ctx, sess); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

func (s *MongoStore) FindByHash(ctx context.Context, hash string) (*Session, error) {
	var out Session
	err := s.coll.FindOne(ctx, bson.M{"_id": hash},
		options.FindOne().SetProjection(projection)).Decode(&out)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("find session by hash: %w", err)
	}
	return &out, nil
}

// DeleteBeyondCap keeps the newest `max` sessions for account (by issuedAt,
// with _id as a deterministic tie-breaker for sessions issued within the same
// millisecond) and deletes the rest. Two round-trips only when the cap is
// exceeded.
//
// Race note: this is a find-then-delete, not a transaction. Two concurrent
// logins for the same account can each read the same over-cap snapshot and
// race a session insert against this read, so in rare sub-millisecond
// concurrent-login scenarios a just-inserted session could be evicted instead
// of the true oldest. The (issuedAt, _id) tie-breaker narrows that window to
// same-millisecond inserts; login is not high-throughput enough to warrant
// wrapping this in a transaction, and "keep newest N" is not a safety
// invariant worth the added contention.
func (s *MongoStore) DeleteBeyondCap(ctx context.Context, account string, max int) (int64, error) {
	cur, err := s.coll.Find(ctx, bson.M{"account": account},
		options.Find().
			SetProjection(bson.M{"_id": 1}).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetSkip(int64(max)),
	)
	if err != nil {
		return 0, fmt.Errorf("find over-cap sessions: %w", err)
	}
	var toDelete []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &toDelete); err != nil {
		return 0, fmt.Errorf("decode over-cap sessions: %w", err)
	}
	if len(toDelete) == 0 {
		return 0, nil
	}
	ids := make([]string, len(toDelete))
	for i, d := range toDelete {
		ids[i] = d.ID
	}
	res, err := s.coll.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, fmt.Errorf("delete over-cap sessions: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteForAccountExcept(ctx context.Context, siteID, account, exceptID string) (int64, error) {
	res, err := s.coll.DeleteMany(ctx, bson.M{
		"siteId":  siteID,
		"account": account,
		"_id":     bson.M{"$ne": exceptID},
	})
	if err != nil {
		return 0, fmt.Errorf("delete sessions for account except: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) DeleteForAccount(ctx context.Context, siteID, account string) (int64, error) {
	res, err := s.coll.DeleteMany(ctx, bson.M{"siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete sessions for account: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) ListForAccount(ctx context.Context, siteID, account string) ([]Session, error) {
	cur, err := s.coll.Find(ctx, bson.M{"siteId": siteID, "account": account},
		options.Find().
			SetProjection(projection).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}}),
	)
	if err != nil {
		return nil, fmt.Errorf("list sessions for account: %w", err)
	}
	var out []Session
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode sessions: %w", err)
	}
	if out == nil {
		out = []Session{}
	}
	return out, nil
}

func (s *MongoStore) DeleteByID(ctx context.Context, siteID, account, id string) (int64, error) {
	res, err := s.coll.DeleteOne(ctx, bson.M{"_id": id, "siteId": siteID, "account": account})
	if err != nil {
		return 0, fmt.Errorf("delete session by id: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *MongoStore) EnsureIndexes(ctx context.Context) error {
	// Legacy index from before DeleteBeyondCap became account-keyed. Left in
	// place (idempotent, harmless) — ops can drop it once the account_1_issuedAt_1
	// index below has been backfilled.
	if _, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "issuedAt", Value: 1}},
	}); err != nil {
		return fmt.Errorf("create sessions userId_issuedAt index: %w", err)
	}
	// Backs DeleteBeyondCap, which is now keyed by account rather than userID.
	if _, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "account", Value: 1}, {Key: "issuedAt", Value: 1}},
	}); err != nil {
		return fmt.Errorf("create sessions account_issuedAt index: %w", err)
	}
	// Backs the ListForAccount / DeleteForAccount queries and the
	// DeleteForAccountExcept revocation.
	if _, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "siteId", Value: 1}, {Key: "account", Value: 1}},
	}); err != nil {
		return fmt.Errorf("create sessions siteId_account index: %w", err)
	}
	return nil
}
