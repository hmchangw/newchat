package main

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mongoDirectoryStore struct {
	users     *mongo.Collection
	employees *mongo.Collection
}

func newMongoDirectoryStore(db *mongo.Database) *mongoDirectoryStore {
	return &mongoDirectoryStore{
		users:     db.Collection("users"),
		employees: db.Collection("hr_employee"),
	}
}

// EnsureIndexes enforces account uniqueness on hr_employee so a buggy HR cron
// write fails at insert time instead of publishing two home sites for one account.
func (s *mongoDirectoryStore) EnsureIndexes(ctx context.Context) error {
	if _, err := s.employees.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "account", Value: 1}},
		Options: options.Index().SetUnique(true),
	}); err != nil {
		return fmt.Errorf("ensure hr_employee (account) unique index: %w", err)
	}
	return nil
}

// ListEmployees returns every account in users (primary collection), left-joined
// with hr_employee for enrichment — bot/admin rows with no match still come back.
// hr_employee must live in the same database as users; $lookup cannot cross databases.
func (s *mongoDirectoryStore) ListEmployees(ctx context.Context) ([]employee, error) {
	// $lookup justification: left-join hr_employee onto users so bot/admin
	// accounts (absent from hr_employee) are still in the directory.
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "hr_employee"},
			{Key: "let", Value: bson.D{
				{Key: "acct", Value: "$account"},
				{Key: "site", Value: "$siteId"},
			}},
			{Key: "pipeline", Value: mongo.Pipeline{
				bson.D{{Key: "$match", Value: bson.D{{Key: "$expr", Value: bson.D{{Key: "$and", Value: bson.A{
					bson.D{{Key: "$eq", Value: bson.A{"$account", "$$acct"}}},
					bson.D{{Key: "$eq", Value: bson.A{"$siteId", "$$site"}}},
				}}}}}}},
				bson.D{{Key: "$project", Value: bson.D{
					{Key: "_id", Value: 0},
					{Key: "employeeId", Value: 1},
				}}},
				bson.D{{Key: "$limit", Value: 1}},
			}},
			{Key: "as", Value: "hrEmployee"},
		}}},
		// preserveNullAndEmptyArrays makes this a LEFT join — a no-match users row still flows through.
		bson.D{{Key: "$unwind", Value: bson.D{
			{Key: "path", Value: "$hrEmployee"},
			{Key: "preserveNullAndEmptyArrays", Value: true},
		}}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "account", Value: 1},
			{Key: "siteId", Value: 1},
			{Key: "roles", Value: 1},
			{Key: "userId", Value: "$_id"},
			{Key: "employeeId", Value: "$hrEmployee.employeeId"},
		}}},
	}
	cur, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate users with hr_employee left-join: %w", err)
	}
	var emps []employee
	if err := cur.All(ctx, &emps); err != nil {
		return nil, fmt.Errorf("decode employees: %w", err)
	}
	return emps, nil
}

// GetByAccount reads one account straight from the users collection. The login
// role gate only needs account/siteId/roles, so this is a light indexed lookup
// rather than the hr_employee left-join ListEmployees runs — no enrichment
// fields are projected.
func (s *mongoDirectoryStore) GetByAccount(ctx context.Context, account string) (employee, bool, error) {
	proj := options.FindOne().SetProjection(bson.D{
		{Key: "_id", Value: 0},
		{Key: "account", Value: 1},
		{Key: "siteId", Value: 1},
		{Key: "roles", Value: 1},
	})
	var emp employee
	err := s.users.FindOne(ctx, bson.D{{Key: "account", Value: account}}, proj).Decode(&emp)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return employee{}, false, nil
	}
	if err != nil {
		return employee{}, false, fmt.Errorf("find user by account %q: %w", account, err)
	}
	return emp, true, nil
}
