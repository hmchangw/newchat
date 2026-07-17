package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEmployeeJSON_OrgNested(t *testing.T) {
	e := model.Employee{
		EmployeeID: "EMP1", Account: "alice", EngName: "Alice", ChineseName: "愛麗絲",
		SiteID: "site-a", Source: "teams",
		Org: model.Org{ID: "g1", Name: "Engineering", Description: "eng dept", Type: "group"},
	}
	data, err := json.Marshal(&e)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "EMP1", raw["employeeId"])
	assert.Equal(t, "alice", raw["account"])
	assert.Equal(t, "Alice", raw["engName"])
	assert.Equal(t, "愛麗絲", raw["chineseName"])
	assert.Equal(t, "teams", raw["source"])
	// org must be a nested single node, not flattened fields
	org, ok := raw["org"].(map[string]any)
	require.True(t, ok, "org must nest as an object")
	assert.Equal(t, "g1", org["id"])
	assert.Equal(t, "Engineering", org["name"])
	assert.Equal(t, "eng dept", org["description"])
	assert.Equal(t, "group", org["type"])
	roundTrip(t, &e, &model.Employee{})
}

func TestEmployeeWithChangeRoundTrip(t *testing.T) {
	ewc := model.EmployeeWithChange{
		Employee: model.Employee{
			EmployeeID: "EMP1", Account: "alice", Source: "teams",
			Org: model.Org{ID: "g1", Type: "group"},
		},
		Change: "created",
	}
	data, err := json.Marshal(&ewc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "Employee fields must embed flat")
	assert.Equal(t, "created", raw["change"])
	roundTrip(t, &ewc, &model.EmployeeWithChange{})
}

func TestEmployeesUpsertBatchRoundTrip(t *testing.T) {
	b := model.EmployeesUpsertBatch{
		Timestamp: 1735689600001,
		Employees: []model.EmployeeWithChange{{
			Employee: model.Employee{
				Account: "alice", Source: "teams",
				Org: model.Org{ID: "g1", Name: "Engineering", Type: "group"},
			},
			Change: "created",
		}},
	}
	roundTrip(t, &b, &model.EmployeesUpsertBatch{})
}

func TestUserWithChangeJSON_UserFieldsFlat(t *testing.T) {
	uwc := model.UserWithChange{
		User:   model.User{ID: "u1", Account: "alice", SiteID: "site-a", SectID: "S"},
		Change: "updated",
	}
	data, err := json.Marshal(&uwc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "User fields must embed flat")
	assert.Equal(t, "updated", raw["change"])
	roundTrip(t, &uwc, &model.UserWithChange{})
}

func TestUsersUpsertBatchRoundTrip(t *testing.T) {
	b := model.UsersUpsertBatch{
		Timestamp: 1735689600001,
		Users: []model.UserWithChange{{
			User: model.User{ID: "u1", Account: "alice", SiteID: "site-a"}, Change: "created",
		}},
	}
	roundTrip(t, &b, &model.UsersUpsertBatch{})
}

func TestHRSyncEmployeeQuitBatchRoundTrip(t *testing.T) {
	b := model.HRSyncEmployeeQuitBatch{
		Timestamp: 1735689600001,
		SiteID:    "site-a",
		Accounts:  []string{"alice", "bob"},
	}
	roundTrip(t, &b, &model.HRSyncEmployeeQuitBatch{})
}
