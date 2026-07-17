package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/teams-hr-sync/transform"
)

const hrEmployeeCollection = "hr_employee"

// mongoStore implements Store over the read client. The projection names
// exactly the Employee fields so unrelated persister-owned fields never leak
// into the diff.
type mongoStore struct {
	employees *mongoutil.Collection[model.Employee]
}

func newMongoStore(readDB *mongo.Database) *mongoStore {
	return &mongoStore{
		employees: mongoutil.NewCollection[model.Employee](readDB.Collection(hrEmployeeCollection)),
	}
}

func (s *mongoStore) ListTeamsEmployees(ctx context.Context) ([]model.Employee, error) {
	rows, err := s.employees.FindMany(ctx,
		bson.M{"source": transform.SourceTeams},
		mongoutil.WithProjection(bson.M{
			"employeeId": 1, "account": 1, "engName": 1, "chineseName": 1,
			"siteId": 1, "source": 1, "org": 1,
		}))
	if err != nil {
		return nil, fmt.Errorf("find teams hr employees: %w", err)
	}
	return rows, nil
}
