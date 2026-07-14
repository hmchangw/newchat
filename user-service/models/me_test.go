package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func TestMeResponse_RoundTrip(t *testing.T) {
	in := MeResponse{
		UserStatusView: UserStatusView{Account: "bob", StatusText: "hi", StatusIsShow: true, ChineseName: "鮑勃", EngName: "Bob"},
		Presence:       model.StatusOnline,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out MeResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestMeResponse_FlatFieldsAndPresence(t *testing.T) {
	// UserStatusView is embedded, so its fields serialize flat alongside "presence".
	in := MeResponse{
		UserStatusView: UserStatusView{Account: "alice", StatusText: "away", StatusIsShow: false},
		Presence:       model.StatusOffline,
	}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	require.Equal(t, "alice", raw["account"])
	require.Equal(t, "away", raw["statusText"])
	_, present := raw["statusIsShow"]
	require.True(t, present, "statusIsShow must always be present")
	require.Equal(t, "offline", raw["presence"])
}
