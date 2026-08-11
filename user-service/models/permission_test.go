package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermissionGetRequest_RoundTrip(t *testing.T) {
	in := PermissionGetRequest{Permission: "external.image.view"}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out PermissionGetRequest
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestPermissionGetRequest_UnmarshalsFromWireBody(t *testing.T) {
	var req PermissionGetRequest
	require.NoError(t, json.Unmarshal([]byte(`{"permission":"external.image.view"}`), &req))
	assert.Equal(t, "external.image.view", req.Permission)
}

func TestPermissionGetResponse_RoundTrip(t *testing.T) {
	in := PermissionGetResponse{Permission: "external.image.view", Granted: true}
	b, err := json.Marshal(in)
	require.NoError(t, err)
	var out PermissionGetResponse
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, in, out)
}

func TestPermissionGetResponse_WireShape(t *testing.T) {
	b, err := json.Marshal(PermissionGetResponse{Permission: "external.image.view", Granted: true})
	require.NoError(t, err)
	assert.JSONEq(t, `{"permission":"external.image.view","granted":true}`, string(b))
}
