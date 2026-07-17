package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

const (
	hrEmployeeCollection = "hr_employee"
	usersCollection      = "users"
)

type mongoStore struct {
	employees *mongoutil.Collection[model.Employee]
	users     *mongoutil.Collection[model.User]
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		employees: mongoutil.NewCollection[model.Employee](db.Collection(hrEmployeeCollection)),
		users:     mongoutil.NewCollection[model.User](db.Collection(usersCollection)),
	}
}

func (s *mongoStore) UpsertEmployees(ctx context.Context, employees []model.EmployeeWithChange) error {
	rows := make([]model.Employee, 0, len(employees))
	for i := range employees {
		rows = append(rows, employees[i].Employee)
	}
	// filter includes source so a same-account row from another feed (legacy
	// HR) is never clobbered — feeds coexist per-source, like the quit path
	if _, err := s.employees.BulkUpsert(ctx, rows, func(e model.Employee) any {
		return bson.M{"account": e.Account, "source": e.Source}
	}); err != nil {
		return fmt.Errorf("bulk upsert hr employees: %w", err)
	}
	return nil
}

// UpsertUserIdentities $sets identity fields only; a full-doc replace would
// wipe roles/password/services on the live auth store, so the update doc is
// built by hand and never derived from the wire struct.
func (s *mongoStore) UpsertUserIdentities(ctx context.Context, users []model.UserWithChange) error {
	models := make([]mongo.WriteModel, 0, len(users))
	for i := range users {
		u := &users[i].User
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"account": u.Account}).
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

func (s *mongoStore) QuitTeamsEmployees(ctx context.Context, accounts []string) error {
	if _, err := s.employees.Raw().DeleteMany(ctx, bson.M{
		"account": bson.M{"$in": accounts},
		"source":  transform.SourceTeams,
	}); err != nil {
		return fmt.Errorf("delete quit hr employees: %w", err)
	}
	return nil
}
