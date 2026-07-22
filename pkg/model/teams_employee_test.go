package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestIEmployeeJSON_OrgFieldsFlat(t *testing.T) {
	e := model.IEmployee{
		IOrg: model.IOrg{
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
	// org fields promote FLAT (inline embed), no nested object
	_, nested := raw["iOrg"]
	assert.False(t, nested, "IOrg must embed flat")
	assert.Equal(t, "S1", raw["sectId"])
	assert.Equal(t, "Engineering", raw["sectName"])
	assert.Equal(t, "V1", raw["divisionId"])
	assert.Equal(t, "EMP1", raw["employeeId"])
	assert.Equal(t, "愛麗絲", raw["chineseName"])
	roundTrip(t, &e, &model.IEmployee{})
}

func TestIEmployeeWithChange_BareElement(t *testing.T) {
	ewc := model.IEmployeeWithChange{
		IEmployee:  model.IEmployee{IOrg: model.IOrg{SectID: "S1"}, Account: "alice"},
		ChangeType: model.IChangeTypeNewHire,
	}
	data, err := json.Marshal(&ewc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "IEmployee fields embed flat")
	assert.Equal(t, "S1", raw["sectId"])
	assert.Equal(t, "new_hire", raw["changeType"])
	roundTrip(t, &ewc, &model.IEmployeeWithChange{})
}

func TestIEmployeesUpsert_BareArrayRoundTrip(t *testing.T) {
	arr := []model.IEmployeeWithChange{
		{IEmployee: model.IEmployee{IOrg: model.IOrg{SectID: "S1"}, Account: "alice"}, ChangeType: model.IChangeTypeNewHire},
		{IEmployee: model.IEmployee{IOrg: model.IOrg{DeptID: "D2"}, Account: "bob"}, ChangeType: model.IChangeTypeUpdate},
	}
	data, err := json.Marshal(arr)
	require.NoError(t, err)
	var got []model.IEmployeeWithChange
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, arr, got)
}

func TestIUserWithChange_BareElement(t *testing.T) {
	uwc := model.IUserWithChange{
		User:       model.User{ID: "u1", Account: "alice", SiteID: "site-a"},
		ChangeType: model.IChangeTypeUpdate,
	}
	data, err := json.Marshal(&uwc)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	assert.Equal(t, "alice", raw["account"], "User fields embed flat")
	assert.Equal(t, "update", raw["changeType"])
	roundTrip(t, &uwc, &model.IUserWithChange{})
}

func TestIHRSyncEmployeeQuitBatchRoundTrip(t *testing.T) {
	b := model.IHRSyncEmployeeQuitBatch{Timestamp: 1735689600001, SiteID: "site-a", Accounts: []string{"alice", "bob"}}
	roundTrip(t, &b, &model.IHRSyncEmployeeQuitBatch{})
}

// TestIType_WireUnchanged pins the exact JSON each I-type marshals to the bytes
// the pre-rename types produced — this is a pure move, the wire must not drift.
// Goldens captured from the deleted Employee/EmployeeWithChange/UserWithChange/
// HRSyncEmployeeQuitBatch for the same field values.
func TestIType_WireUnchanged(t *testing.T) {
	iemp := model.IEmployee{
		IOrg: model.IOrg{
			SectID: "S1", SectTCName: "工程", SectName: "Engineering", SectDescription: "eng",
			DeptID: "D1", DeptTCName: "技術", DeptName: "Tech", DeptDescription: "tech dept", DivisionID: "V1",
		},
		ID: "ignored", EmployeeID: "EMP1", Account: "alice", EngName: "Alice Wang",
		ChineseName: "王愛麗", Mail: "alice@x.com", MailNickname: "alice", UserType: "Member",
		AccountEnabled: true, SiteID: "site-a",
	}
	cases := []struct {
		name string
		val  any
		want string
	}{
		{
			"IEmployee",
			iemp,
			`{"sectId":"S1","sectTCName":"工程","sectName":"Engineering","sectDescription":"eng","deptId":"D1","deptTCName":"技術","deptName":"Tech","deptDescription":"tech dept","divisionId":"V1","employeeId":"EMP1","account":"alice","engName":"Alice Wang","chineseName":"王愛麗","mail":"alice@x.com","mailNickname":"alice","userType":"Member","accountEnabled":true,"siteId":"site-a"}`,
		},
		{
			"IEmployeeWithChange",
			model.IEmployeeWithChange{IEmployee: iemp, ChangeType: model.IChangeTypeNewHire},
			`{"sectId":"S1","sectTCName":"工程","sectName":"Engineering","sectDescription":"eng","deptId":"D1","deptTCName":"技術","deptName":"Tech","deptDescription":"tech dept","divisionId":"V1","employeeId":"EMP1","account":"alice","engName":"Alice Wang","chineseName":"王愛麗","mail":"alice@x.com","mailNickname":"alice","userType":"Member","accountEnabled":true,"siteId":"site-a","changeType":"new_hire"}`,
		},
		{
			"IUserWithChange",
			model.IUserWithChange{User: model.User{ID: "u1", Account: "alice", SiteID: "site-a", EmployeeID: "EMP1"}, ChangeType: model.IChangeTypeUpdate},
			`{"id":"u1","account":"alice","siteId":"site-a","sectId":"","sectName":"","sectTCName":"","sectDescription":"","deptId":"","deptName":"","deptTCName":"","deptDescription":"","engName":"","chineseName":"","employeeId":"EMP1","statusIsShow":false,"statusText":"","changeType":"update"}`,
		},
		{
			"IHRSyncEmployeeQuitBatch",
			model.IHRSyncEmployeeQuitBatch{Timestamp: 1735689600001, SiteID: "site-a", Accounts: []string{"alice", "bob"}},
			`{"timestamp":1735689600001,"siteId":"site-a","accounts":["alice","bob"]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.val)
			require.NoError(t, err)
			assert.Equal(t, tc.want, string(data), "wire JSON must match the pre-rename bytes")
		})
	}
}
