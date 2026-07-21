package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSSOSetRequest_JSON(t *testing.T) {
	var req SSOSetRequest
	require.NoError(t, json.Unmarshal([]byte(`{"ssoToken":"at","refreshToken":"rt","account":"bob"}`), &req))
	assert.Equal(t, "at", req.SSOToken)
	assert.Equal(t, "rt", req.RefreshToken)
	assert.Equal(t, "bob", req.Account)
}

func TestSSORefreshRequest_AccountOptional(t *testing.T) {
	var req SSORefreshRequest
	require.NoError(t, json.Unmarshal([]byte(`{}`), &req))
	assert.Empty(t, req.Account)
}

func TestSSORefreshResponse_JSON(t *testing.T) {
	out, err := json.Marshal(SSORefreshResponse{SSOToken: "at"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"ssoToken":"at"}`, string(out))
}
