package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEmployeeJSON_OrgFieldsFlat(t *testing.T) {
	e := model.Employee{
		Org: model.Org{
			SectID: "S1", SectName: "Engineering", SectDescription: "eng dept",
			DeptID: "D1", DivisionID: "V1",
		},
		EmployeeID: "EMP1", Account: "alice", EngName: "Alice", ChineseName: "愛麗絲",
		SiteID: "site-a",
	}
	data, err := json.Marshal(&e)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	// org fields promote FLAT (inline embed), no nested "org" object
	_, nested := raw["org"]
	assert.False(t, nested, "Org must embed flat")
	assert.Equal(t, "S1", raw["sectId"])
	assert.Equal(t, "Engineering", raw["sectName"])
	assert.Equal(t, "V1", raw["divisionId"])
	assert.Equal(t, "EMP1", raw["employeeId"])
	assert.Equal(t, "愛麗絲", raw["chineseName"])
	roundTrip(t, &e, &model.Employee{})
}

func TestEmployeeWithChange_BareElement(t *testing.T) {
	ewc := model.EmployeeWithChange{
		Employee:   model.Employee{Org: model.Org{SectID: "S1"}, Account: "alice"},
		ChangeType: model.ChangeTypeNewHire,
	}
	data, err := json.Marshal(&ewc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "Employee fields embed flat")
	assert.Equal(t, "S1", raw["sectId"])
	assert.Equal(t, "new_hire", raw["changeType"])
	roundTrip(t, &ewc, &model.EmployeeWithChange{})
}

func TestEmployeesUpsert_BareArrayRoundTrip(t *testing.T) {
	arr := []model.EmployeeWithChange{
		{Employee: model.Employee{Org: model.Org{SectID: "S1"}, Account: "alice"}, ChangeType: model.ChangeTypeNewHire},
		{Employee: model.Employee{Org: model.Org{DeptID: "D2"}, Account: "bob"}, ChangeType: model.ChangeTypeUpdate},
	}
	data, err := json.Marshal(arr)
	require.NoError(t, err)
	var got []model.EmployeeWithChange
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, arr, got)
}

func TestUserWithChange_BareElement(t *testing.T) {
	uwc := model.UserWithChange{
		User:       model.User{ID: "u1", Account: "alice", SiteID: "site-a"},
		ChangeType: model.ChangeTypeUpdate,
	}
	data, err := json.Marshal(&uwc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "User fields embed flat")
	assert.Equal(t, "update", raw["changeType"])
	roundTrip(t, &uwc, &model.UserWithChange{})
}

func TestHRSyncEmployeeQuitBatchRoundTrip(t *testing.T) {
	b := model.HRSyncEmployeeQuitBatch{Timestamp: 1735689600001, SiteID: "site-a", Accounts: []string{"alice", "bob"}}
	roundTrip(t, &b, &model.HRSyncEmployeeQuitBatch{})
}
