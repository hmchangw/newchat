package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
)

type mongoStore struct {
	users    *mongo.Collection
	sessions *mongo.Collection
}

func newMongoStore(ctx context.Context, db *mongo.Database) (*mongoStore, error) {
	s := &mongoStore{
		users:    db.Collection("users"),
		sessions: db.Collection("sessions"),
	}
	if err := s.ensureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("ensure indexes: %w", err)
	}
	return s, nil
}

// ensureIndexes creates the compound index used by FIFO eviction and the
// future list-sessions / revoke-all paths. The {_id: 1} primary is automatic.
func (s *mongoStore) ensureIndexes(ctx context.Context) error {
	_, err := s.sessions.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "issuedAt", Value: 1}},
	})
	return err
}

func (s *mongoStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	var u model.User
	err := s.users.FindOne(ctx, bson.M{"account": account},
		options.FindOne().SetProjection(bson.M{
			"_id":                   1,
			"account":               1,
			"siteId":                1,
			"engName":               1,
			"chineseName":           1,
			"roles":                 1,
			"requirePasswordChange": 1,
			"services.password":     1,
			"deactivated":           1,
		})).Decode(&u)
	if err != nil {
		return nil, fmt.Errorf("find user by account: %w", err)
	}
	return &u, nil
}

func (s *mongoStore) InsertSession(ctx context.Context, sess *session) error {
	if _, err := s.sessions.InsertOne(ctx, sess); err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

func (s *mongoStore) FindSessionByHash(ctx context.Context, hash string) (*session, error) {
	var sess session
	err := s.sessions.FindOne(ctx, bson.M{"_id": hash}).Decode(&sess)
	if err != nil {
		return nil, fmt.Errorf("find session by hash: %w", err)
	}
	return &sess, nil
}

func (s *mongoStore) DeleteSessionsBeyondCap(ctx context.Context, userID string, cap int) (int64, error) {
	if cap < 0 {
		return 0, nil
	}
	// Skip the newest `cap` rows (sorted DESC by issuedAt); anything
	// returned is over-cap. Uses the {userId:1, issuedAt:1} compound
	// index. Under-cap users: one RTT with zero docs returned, no
	// DeleteMany call. Over-cap users (typically over by 1): one RTT to
	// fetch the victim IDs + one RTT for DeleteMany.
	// Sort DESC by issuedAt with _id as a tie-breaker so two logins landing
	// in the same millisecond evict deterministically (Mongo's within-key
	// order is otherwise implementation-defined).
	cur, err := s.sessions.Find(ctx, bson.M{"userId": userID},
		options.Find().
			SetProjection(bson.M{"_id": 1}).
			SetSort(bson.D{{Key: "issuedAt", Value: -1}, {Key: "_id", Value: -1}}).
			SetSkip(int64(cap)))
	if err != nil {
		return 0, fmt.Errorf("find over-cap: %w", err)
	}
	var rows []struct {
		ID string `bson:"_id"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return 0, fmt.Errorf("decode over-cap: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	res, err := s.sessions.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, fmt.Errorf("delete over-cap: %w", err)
	}
	return res.DeletedCount, nil
}

func (s *mongoStore) Ping(ctx context.Context) error {
	if err := s.users.Database().Client().Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongo: %w", err)
	}
	return nil
}
