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

//go:generate mockgen -source=write_store.go -destination=mock_write_store_test.go -package=main

const usersCollection = "users"

// WriteStore is the direct-mode write surface (the migration/backfill path that
// bypasses the JetStream feed). Owned by this service — each HR-feed consumer
// implements its own; there is no shared store package.
type WriteStore interface {
	UpsertEmployees(ctx context.Context, employees []model.IEmployeeWithChange) error
	// UpsertUserIdentities upserts IDENTITY FIELDS ONLY (account, siteId,
	// engName, chineseName, employeeId); it must never touch
	// roles/services/password on the live auth store.
	UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error
	QuitTeamsEmployees(ctx context.Context, accounts []string) error
}

// mongoWriteStore implements WriteStore over the migration target database.
type mongoWriteStore struct {
	employees *mongoutil.Collection[model.IEmployee]
	users     *mongoutil.Collection[model.User]
}

func newMongoWriteStore(db *mongo.Database) *mongoWriteStore {
	return &mongoWriteStore{
		employees: mongoutil.NewCollection[model.IEmployee](db.Collection(hrEmployeeCollection)),
		users:     mongoutil.NewCollection[model.User](db.Collection(usersCollection)),
	}
}

func (s *mongoWriteStore) UpsertEmployees(ctx context.Context, employees []model.IEmployeeWithChange) error {
	rows := make([]model.IEmployee, 0, len(employees))
	for i := range employees {
		rows = append(rows, employees[i].IEmployee)
	}
	// _id = employeeId (the stable per-employee id): keys the upsert; the wire
	// strips IEmployee.ID.
	if _, err := s.employees.BulkUpsertByID(ctx, rows, func(e model.IEmployee) string {
		return e.EmployeeID
	}); err != nil {
		return fmt.Errorf("bulk upsert hr employees: %w", err)
	}
	return nil
}

// UpsertUserIdentities $sets identity fields only; a full-doc replace would wipe
// roles/password/services on the live auth store, so the update doc is built by
// hand and never derived from the wire struct.
func (s *mongoWriteStore) UpsertUserIdentities(ctx context.Context, users []model.IUserWithChange) error {
	models := make([]mongo.WriteModel, 0, len(users))
	for i := range users {
		u := &users[i].User
		// employeeId is the identity key; an empty one would match every other
		// keyless row and clobber it, so skip rather than corrupt.
		if u.EmployeeID == "" {
			slog.WarnContext(ctx, "skip user identity upsert: empty employeeId", "account", u.Account)
			continue
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"employeeId": u.EmployeeID}).
			SetUpdate(bson.M{
				"$set": bson.M{
					"account": u.Account, "siteId": u.SiteID,
					"engName": u.EngName, "chineseName": u.ChineseName,
					"employeeId": u.EmployeeID,
				},
				"$setOnInsert": bson.M{"_id": idgen.GenerateUUIDv7()},
			}).
			SetUpsert(true))
	}
	if _, err := s.users.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("bulk upsert user identities: %w", err)
	}
	return nil
}

func (s *mongoWriteStore) QuitTeamsEmployees(ctx context.Context, accounts []string) error {
	if _, err := s.employees.Raw().DeleteMany(ctx, bson.M{
		"account": bson.M{"$in": accounts},
	}); err != nil {
		return fmt.Errorf("delete quit hr employees: %w", err)
	}
	return nil
}
