package main

import (
	"context"
	"fmt"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

const (
	EmployeeCollection = "hr_employee"
	UserCollection     = "users"
)

// Store is the write surface this worker persists the HR feed into. Owned by
// this service — each consumer implements its own; there is no shared store.
type Store interface {
	// UpsertEmployees replaces hr_employee docs keyed by employeeId.
	UpsertEmployees(ctx context.Context, employees []model.IEmployeeWithChange) error
	// UpsertUserIdentities upserts users by account, writing IDENTITY FIELDS
	// ONLY (siteId, engName, chineseName, and employeeId when present). It must
	// never touch roles/services/password/active/status fields — users is the
	// live auth store.
	UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error
	// QuitTeamsEmployees deletes hr_employee rows for the given accounts.
	// users stays untouched (the user lifecycle is not the HR feed's to end).
	QuitTeamsEmployees(ctx context.Context, accounts []string) error
}

// MongoStore implements Store over the target write database.
type MongoStore struct {
	employees *mongoutil.Collection[model.IEmployee]
	users     *mongoutil.Collection[model.User]
}

func newMongoStore(db *mongo.Database) *MongoStore {
	return &MongoStore{
		employees: mongoutil.NewCollection[model.IEmployee](db.Collection(EmployeeCollection)),
		users:     mongoutil.NewCollection[model.User](db.Collection(UserCollection)),
	}
}

func (s *MongoStore) UpsertEmployees(ctx context.Context, employees []model.IEmployeeWithChange) error {
	rows := make([]model.IEmployee, 0, len(employees))
	for i := range employees {
		rows = append(rows, employees[i].IEmployee)
	}
	// _id = employeeId (the stable per-employee id): keys the upsert and gives
	// the row a string _id from the filter — the wire strips IEmployee.ID.
	if _, err := s.employees.BulkUpsertByID(ctx, rows, func(e model.IEmployee) string {
		return e.EmployeeID
	}); err != nil {
		return fmt.Errorf("bulk upsert hr employees: %w", err)
	}
	return nil
}

// UpsertUserIdentities $sets identity fields only; a full-doc replace would
// wipe roles/password/services on the live auth store, so the update doc is
// built by hand and never derived from the wire struct.
func (s *MongoStore) UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error {
	models := make([]mongo.WriteModel, 0, len(users))
	for i := range users {
		u := &users[i].User
		// account is the identity key (globally unique); it covers HR-feed users
		// and migrated externals alike, and an empty one would clobber other rows.
		if u.Account == "" {
			slog.WarnContext(ctx, "skip user identity upsert: empty account")
			continue
		}
		// A publisher that owns the _id (the Teams migration resolver, deterministic
		// from the Graph id) carries it so every site converges on one _id; the HR
		// feed leaves it empty and we mint a per-site one.
		idOnInsert := u.ID
		if idOnInsert == "" {
			idOnInsert = idgen.GenerateUUIDv7()
		}
		// Migrated externals carry no employeeId (they aren't employees); only set
		// it for HR-feed users that have one, so an external's row stays clean.
		set := bson.M{"siteId": u.SiteID, "engName": u.EngName, "chineseName": u.ChineseName}
		if u.EmployeeID != "" {
			set["employeeId"] = u.EmployeeID
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"account": u.Account}).
			SetUpdate(bson.M{
				"$set":         set,
				"$setOnInsert": bson.M{"_id": idOnInsert},
			}).
			SetUpsert(true))
	}
	// All inputs had an empty account → nothing to write; BulkWrite errors on
	// an empty slice (mongo.ErrEmptySlice), so no-op cleanly instead.
	if len(models) == 0 {
		return nil
	}
	if _, err := s.users.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("bulk upsert user identities: %w", err)
	}
	return nil
}

func (s *MongoStore) QuitTeamsEmployees(ctx context.Context, accounts []string) error {
	if _, err := s.employees.Raw().DeleteMany(ctx, bson.M{
		"account": bson.M{"$in": accounts},
	}); err != nil {
		return fmt.Errorf("delete quit hr employees: %w", err)
	}
	return nil
}
