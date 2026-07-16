package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestOrgJSON_TagsMatchSpotlightOrgIndex(t *testing.T) {
	org := model.Org{
		SectID: "S", SectTCName: "科", SectName: "Sect", SectDescription: "sd",
		DeptID: "D", DeptTCName: "處", DeptName: "Dept", DeptDescription: "dd",
		DivisionID: "V",
	}
	data, err := json.Marshal(&org)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	for _, k := range []string{"sectId", "sectTCName", "sectName", "sectDescription",
		"deptId", "deptTCName", "deptName", "deptDescription", "divisionId"} {
		_, ok := raw[k]
		assert.True(t, ok, "org json key %q must be present", k)
	}
}

func TestOrgJSON_OmitEmpty(t *testing.T) {
	data, err := json.Marshal(&model.Org{})
	require.NoError(t, err)
	assert.Equal(t, "{}", string(data), "all org fields omitempty")
}

func TestEmployeeJSON_OrgFieldsFlat(t *testing.T) {
	e := model.Employee{
		Org:        model.Org{SectID: "S", DeptName: "Dept"},
		EmployeeID: "EMP1", Account: "alice", EngName: "Alice", ChineseName: "愛麗絲", SiteID: "site-a", Source: "teams",
	}
	data, err := json.Marshal(&e)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	// org fields must be FLAT (no nested "Org" object)
	_, nested := raw["Org"]
	assert.False(t, nested, "Org must embed flat, not nest")
	assert.Equal(t, "S", raw["sectId"])
	assert.Equal(t, "EMP1", raw["employeeId"])
	assert.Equal(t, "Alice", raw["engName"])
	assert.Equal(t, "愛麗絲", raw["chineseName"])
	assert.Equal(t, "teams", raw["source"])
	roundTrip(t, &e, &model.Employee{})
}

func TestEmployeeWithChangeRoundTrip(t *testing.T) {
	ewc := model.EmployeeWithChange{
		Employee: model.Employee{
			Org:        model.Org{SectID: "S", DeptID: "D"},
			EmployeeID: "EMP1", Account: "alice", Source: "teams",
		},
		Change: "created",
	}
	roundTrip(t, &ewc, &model.EmployeeWithChange{})
}

// TestEmployeesUpsertBatch_ConsumerDecodeCompat proves the existing
// search-sync-worker consumer (which decodes each element into its 9-field
// SpotlightOrgIndex) still sees the org fields when we publish
// EmployeesUpsertBatch. A local struct mirroring the consumer's org-only
// decode target stands in for SpotlightOrgIndex (json tags identical).
func TestEmployeesUpsertBatch_ConsumerDecodeCompat(t *testing.T) {
	batch := model.EmployeesUpsertBatch{
		Timestamp: 1735689600001,
		Employees: []model.EmployeeWithChange{{
			Employee: model.Employee{
				Org:        model.Org{SectID: "S1", DeptID: "D1", DivisionID: "V1"},
				EmployeeID: "EMP1", Account: "alice", Source: "teams",
			},
			Change: "created",
		}},
	}
	data, err := json.Marshal(&batch)
	require.NoError(t, err)

	var consumer struct {
		Timestamp int64       `json:"timestamp"`
		Employees []model.Org `json:"employees"`
	}
	require.NoError(t, json.Unmarshal(data, &consumer))
	require.Len(t, consumer.Employees, 1)
	assert.Equal(t, int64(1735689600001), consumer.Timestamp)
	assert.Equal(t, "S1", consumer.Employees[0].SectID)
	assert.Equal(t, "D1", consumer.Employees[0].DeptID)
	assert.Equal(t, "V1", consumer.Employees[0].DivisionID)
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
