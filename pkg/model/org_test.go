package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestOrgRoundTrip(t *testing.T) {
	o := model.Org{
		SectID: "S1", SectTCName: "工程", SectName: "Engineering", SectDescription: "eng",
		DeptID: "D1", DeptTCName: "技術", DeptName: "Tech", DeptDescription: "tech dept", DivisionID: "V1",
	}
	roundTrip(t, &o, &model.Org{})
}

// TestOrgJSON_Flat locks the exact wire so search-sync-worker's SpotlightOrgIndex
// stays byte-compatible with model.Org.
func TestOrgJSON_Flat(t *testing.T) {
	o := model.Org{SectID: "S1", SectName: "Engineering", DivisionID: "V1"}
	data, err := json.Marshal(&o)
	require.NoError(t, err)
	require.JSONEq(t, `{"sectId":"S1","sectName":"Engineering","divisionId":"V1"}`, string(data))
}
