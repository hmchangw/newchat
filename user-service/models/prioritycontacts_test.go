package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPriorityContactItem_UserRowOmitsApp(t *testing.T) {
	item := PriorityContactItem{
		Account: "alice",
		Type:    PriorityContactTypeUser,
		User: &PriorityContactUser{
			EngName: "Alice", ChineseName: "愛麗絲", EmployeeID: "E1", SectName: "Ops",
		},
	}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"alice","type":"user","user":{"engName":"Alice","chineseName":"愛麗絲","employeeId":"E1","sectName":"Ops"}}`, string(data))
}

func TestPriorityContactItem_BotRowOmitsUser(t *testing.T) {
	item := PriorityContactItem{
		Account: "helper.bot",
		Type:    PriorityContactTypeBot,
		App:     &PriorityContactApp{Name: "Helper"},
	}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"helper.bot","type":"bot","app":{"name":"Helper"}}`, string(data))
}

// An account that no longer resolves keeps account+type so the client can render
// a placeholder instead of dropping the row.
func TestPriorityContactItem_UnresolvedRowCarriesAccountAndType(t *testing.T) {
	item := PriorityContactItem{Account: "ghost", Type: PriorityContactTypeUser}
	data, err := json.Marshal(item)
	require.NoError(t, err)
	assert.JSONEq(t, `{"account":"ghost","type":"user"}`, string(data))
}

func TestPriorityContactsResponse_EmptyListMarshalsAsArray(t *testing.T) {
	data, err := json.Marshal(PriorityContactsResponse{Contacts: []PriorityContactItem{}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"contacts":[]}`, string(data))
}
