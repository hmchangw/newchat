package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const hrEmployeeCollection = "hr_employee"

// employeeProjection is derived from model.IEmployee's bson tags (incl. the
// inline-embedded Org) so a field rename can't silently drop from the read —
// the projection tracks the struct.
var employeeProjection = bsonProjection(reflect.TypeOf(model.IEmployee{}))

// bsonProjection walks a struct's bson tags (recursing into inline-embedded
// structs) into a bson.M{tag:1} projection.
func bsonProjection(t reflect.Type) bson.M {
	proj := bson.M{}
	for i := range t.NumField() {
		f := t.Field(i)
		if strings.Contains(f.Tag.Get("bson"), "inline") && f.Type.Kind() == reflect.Struct {
			for k, v := range bsonProjection(f.Type) {
				proj[k] = v
			}
			continue
		}
		tag, _, _ := strings.Cut(f.Tag.Get("bson"), ",")
		if tag == "" || tag == "-" {
			continue
		}
		proj[tag] = 1
	}
	return proj
}

// mongoStore implements Store over the read client.
type mongoStore struct {
	employees *mongoutil.Collection[model.IEmployee]
}

func newMongoStore(readDB *mongo.Database) *mongoStore {
	return &mongoStore{
		employees: mongoutil.NewCollection[model.IEmployee](readDB.Collection(hrEmployeeCollection)),
	}
}

func (s *mongoStore) ListTeamsEmployees(ctx context.Context) ([]model.IEmployee, error) {
	rows, err := s.employees.FindMany(ctx,
		bson.M{},
		mongoutil.WithProjection(employeeProjection))
	if err != nil {
		return nil, fmt.Errorf("find teams hr employees: %w", err)
	}
	return rows, nil
}
