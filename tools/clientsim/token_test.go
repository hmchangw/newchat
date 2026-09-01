package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDevProvider_SendsAccountOnly(t *testing.T) {
	f, err := devProvider{}.Material("user-7")
	require.NoError(t, err)
	assert.Equal(t, "user-7", f.Account)
	assert.Empty(t, f.SSOToken)
	assert.Empty(t, f.AuthToken)
}

func TestAuthClient_Mint(t *testing.T) {
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/auth", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"natsJwt":"eyJ.fake.jwt","user":{"account":"user-7"}}`))
	}))
	defer srv.Close()

	c := newAuthClient(srv.URL, devProvider{}, newMetrics())
	jwtStr, err := c.Mint(context.Background(), "user-7", "UABCDEF")
	require.NoError(t, err)
	assert.Equal(t, "eyJ.fake.jwt", jwtStr)
	assert.Equal(t, "user-7", gotBody["account"])
	assert.Equal(t, "UABCDEF", gotBody["natsPublicKey"])
	assert.NotContains(t, gotBody, "ssoToken", "omitempty must hold for unset token fields")
	assert.NotContains(t, gotBody, "authToken")
}

func TestAuthClient_Mint_ErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"forbidden","reason":"some_reason","error":"mint refused"}`))
	}))
	defer srv.Close()

	c := newAuthClient(srv.URL, devProvider{}, newMetrics())
	_, err := c.Mint(context.Background(), "alice", "UABCDEF")
	require.Error(t, err)
	// The error surfaces the status and the server's reason/message so a run
	// can tell a rejected mint from a transport failure.
	assert.Contains(t, err.Error(), "some_reason")
	assert.Contains(t, err.Error(), "403")
}

func TestAuthClient_Mint_EmptyJWTIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"user":{"account":"user-7"}}`))
	}))
	defer srv.Close()

	c := newAuthClient(srv.URL, devProvider{}, newMetrics())
	_, err := c.Mint(context.Background(), "user-7", "UABCDEF")
	assert.ErrorContains(t, err, "no natsJwt")
}

func TestAuthClient_Mint_ServerDown(t *testing.T) {
	c := newAuthClient("http://127.0.0.1:1", devProvider{}, newMetrics())
	_, err := c.Mint(context.Background(), "user-1", "UABCDEF")
	assert.Error(t, err)
}
