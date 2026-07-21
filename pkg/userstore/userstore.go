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
