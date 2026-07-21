package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

type storeMongo struct {
	users    *mongo.Collection
	sessions session.Store
}

func newStoreMongo(db *mongo.Database) *storeMongo {
	return &storeMongo{
		users:    db.Collection("users"),
		sessions: session.NewMongoStore(db),
	}
}

func (s *storeMongo) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
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

func (s *storeMongo) InsertSession(ctx context.Context, sess *session.Session) error {
	return s.sessions.Insert(ctx, sess)
}

func (s *storeMongo) FindSessionByHash(ctx context.Context, hash string) (*session.Session, error) {
	return s.sessions.FindByHash(ctx, hash)
}

func (s *storeMongo) DeleteSessionsBeyondCap(ctx context.Context, account string, max int) (int64, error) {
	if max < 0 {
		return 0, nil
	}
	return s.sessions.DeleteBeyondCap(ctx, account, max)
}

func (s *storeMongo) Ping(ctx context.Context) error {
	if err := s.users.Database().Client().Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping mongo: %w", err)
	}
	return nil
}
