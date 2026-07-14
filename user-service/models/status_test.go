package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func ptrBool(b bool) *bool { return &b }

func TestUserStatusView_RoundTrip(t *testing.T) {
	in := UserStatusView{Account: "bob", StatusText: "hi", StatusIsShow: true, ChineseName: "鮑勃", EngName: "Bob"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out UserStatusView
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestUserStatusView_FalseIsShow_AlwaysPresent(t *testing.T) {
	// StatusIsShow is a plain bool: false must serialize as "statusIsShow":false
	// (always present), matching the model.User contract for never-set users.
	in := UserStatusView{Account: "alice", StatusText: "away", StatusIsShow: false}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(b, &raw))
	v, present := raw["statusIsShow"]
	require.True(t, present, "statusIsShow must always be present")
	require.Equal(t, false, v)
	var out UserStatusView
	require.NoError(t, json.Unmarshal(b, &out))
	require.False(t, out.StatusIsShow)
}

func TestStatusSetRequest_RoundTrip(t *testing.T) {
	in := StatusSetRequest{Text: "busy"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out StatusSetRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestStatusSetRequest_FalseIsShow_RoundTrip(t *testing.T) {
	in := StatusSetRequest{Text: "x", IsShow: ptrBool(false)}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out StatusSetRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
	require.NotNil(t, out.IsShow, "IsShow must not be nil after round-trip")
	require.False(t, *out.IsShow, "IsShow must be false after round-trip")
}

func TestStatusGetByNameRequest_RoundTrip(t *testing.T) {
	in := StatusGetByNameRequest{Name: "bob"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out StatusGetByNameRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}
