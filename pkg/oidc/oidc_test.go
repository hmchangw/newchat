package oidc

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsecureTLSTransport(t *testing.T) {
	tr := insecureTLSTransport()
	require.NotNil(t, tr.TLSClientConfig)
	assert.True(t, tr.TLSClientConfig.InsecureSkipVerify, "TLS verification must be skipped")
	assert.Equal(t, uint16(tls.VersionTLS12), tr.TLSClientConfig.MinVersion)
	// Cloned from http.DefaultTransport, so its defensive defaults survive.
	assert.NotZero(t, tr.IdleConnTimeout, "DefaultTransport defaults preserved via Clone")
}

func TestContainsAudience(t *testing.T) {
	cases := []struct {
		name      string
		tokenAud  []string
		allowed   []string
		wantMatch bool
	}{
		{"single token aud matches single allowed", []string{"a"}, []string{"a"}, true},
		{"token aud matches one of many allowed", []string{"b"}, []string{"a", "b", "c"}, true},
		{"one of many token auds matches allowed", []string{"x", "b"}, []string{"a", "b"}, true},
		{"no match", []string{"x"}, []string{"a", "b"}, false},
		{"empty token aud", nil, []string{"a"}, false},
		{"empty allowed", []string{"a"}, nil, false},
		{"both empty", nil, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantMatch, containsAudience(tc.tokenAud, tc.allowed))
		})
	}
}

func TestNewValidator_RejectsEmptyAudiences(t *testing.T) {
	_, err := NewValidator(t.Context(), Config{
		IssuerURL: "http://example.invalid",
		Audiences: nil,
	})
	assert.ErrorIs(t, err, ErrNoAudiences)
}

func TestNewValidator_ClientIDWithoutTokenEndpoint(t *testing.T) {
	// Issuer whose discovery doc omits token_endpoint.
	mux := http.NewServeMux()
	srv := httptest.NewUnstartedServer(mux)
	t.Cleanup(srv.Close)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"issuer":"` + srv.URL + `","jwks_uri":"` + srv.URL + `/keys"}`))
	})
	srv.Start()

	_, err := NewValidator(context.Background(), Config{
		IssuerURL: srv.URL,
		Audiences: []string{"nats-chat"},
		ClientID:  "nats-chat",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token_endpoint")
}

func TestClaims_Account(t *testing.T) {
	tests := []struct {
		name   string
		claims Claims
		want   string
	}{
		{"preferred_username wins", Claims{PreferredUsername: "alice", Name: "Alice W"}, "alice"},
		{"name alone is not an account", Claims{Name: "Alice W"}, ""},
		{"both blank is blank", Claims{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.claims.Account())
		})
	}
}

func newTestValidator(t *testing.T, f *fakeIssuer, cfg Config) *Validator {
	t.Helper()
	if cfg.IssuerURL == "" {
		cfg.IssuerURL = f.URL()
	}
	if len(cfg.Audiences) == 0 {
		cfg.Audiences = []string{"nats-chat"}
	}
	v, err := NewValidator(context.Background(), cfg)
	require.NoError(t, err)
	return v
}

func TestValidate_HappyPath_FillsExpiry(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	exp := time.Now().Add(30 * time.Minute).Truncate(time.Second)
	claims, err := v.Validate(context.Background(), f.Mint(map[string]any{"exp": exp.Unix()}))
	require.NoError(t, err)
	assert.Equal(t, "alice", claims.Account())
	assert.WithinDuration(t, exp, claims.Expiry, time.Second)
}

func TestValidate_ExpiredToken(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	_, err := v.Validate(context.Background(), f.Mint(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()}))
	assert.ErrorIs(t, err, ErrTokenExpired)
}

func TestValidate_WrongAudience(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{})

	_, err := v.Validate(context.Background(), f.Mint(map[string]any{"aud": "other-client"}))
	assert.ErrorIs(t, err, ErrAudienceNotAllowed)
}

func TestRefresh_Success(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})

	exp := time.Now().Add(20 * time.Minute).Truncate(time.Second)
	newAccess := f.Mint(map[string]any{"exp": exp.Unix()})
	f.TokenHandler = func(w http.ResponseWriter, r *http.Request) {
		if !assert.NoError(t, r.ParseForm()) {
			return
		}
		assert.Equal(t, "refresh_token", r.PostForm.Get("grant_type"))
		assert.Equal(t, "old-refresh", r.PostForm.Get("refresh_token"))
		assert.Equal(t, "nats-chat", r.PostForm.Get("client_id"))
		writeJSON(t, w, map[string]any{"access_token": newAccess, "refresh_token": "rotated-refresh"})
	}

	ts, err := v.Refresh(context.Background(), "old-refresh")
	require.NoError(t, err)
	assert.Equal(t, newAccess, ts.SSOToken)
	assert.Equal(t, "rotated-refresh", ts.RefreshToken)
	assert.Equal(t, "alice", ts.Account, "Account carries the verified token's preferred_username")
	assert.WithinDuration(t, exp, ts.Expiry, time.Second)
}

func TestRefresh_InvalidGrantIsRejected(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Token is not active"}`))
	}

	_, err := v.Refresh(context.Background(), "dead-refresh")
	assert.ErrorIs(t, err, ErrRefreshRejected)
}

func TestRefresh_ServerErrorIsNotRejected(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}

	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected) // transport/5xx is NOT an OAuth rejection
}

func TestRefresh_MissingAccessToken(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"refresh_token": "r2"}) // no access_token
	}

	_, err := v.Refresh(context.Background(), "any")
	assert.ErrorIs(t, err, ErrNoAccessToken)
}

func TestRefresh_UnverifiableAccessTokenFails(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		expired := f.Mint(map[string]any{"exp": time.Now().Add(-time.Hour).Unix()})
		writeJSON(t, w, map[string]any{"access_token": expired})
	}

	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err) // returned token must verify before we hand it out
}

// Documents the config coupling: the refreshed access token is re-checked against the
// OIDC_AUDIENCES allow-list, so a token whose aud is not allowed fails every refresh.
func TestRefresh_WrongAudienceAccessTokenFails(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		wrongAud := f.Mint(map[string]any{"aud": "other-client"})
		writeJSON(t, w, map[string]any{"access_token": wrongAud})
	}

	_, err := v.Refresh(context.Background(), "any")
	assert.ErrorIs(t, err, ErrAudienceNotAllowed)
}

func TestRefresh_WithoutClientIDFails(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{}) // no ClientID
	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
}

func TestRefresh_RespectsContextCancellation(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	// Endpoint stalls so the client's ctx deadline fires first; the 3s cap keeps a late request-context cancellation from hanging httptest.Server.Close().
	f.TokenHandler = func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := v.Refresh(ctx, "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected)
}

func TestRefresh_Malformed200IsParseError(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{not json"))
	}
	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected) // a malformed 200 is not an OAuth rejection
}

func TestRefresh_TransportErrorIsNotRejected(t *testing.T) {
	f := newFakeIssuer(t)
	v := newTestValidator(t, f, Config{ClientID: "nats-chat"})
	// Hijack and drop the connection so client.Do returns a transport error.
	f.TokenHandler = func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	}
	_, err := v.Refresh(context.Background(), "any")
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRefreshRejected)
}
