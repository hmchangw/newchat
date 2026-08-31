package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/user-service/config"
)

// newDiscoveryStub serves an in-process OIDC discovery document (loopback only,
// no outbound network) — enough for pkgoidc.NewValidator, which fetches only the
// well-known metadata at construction time; JWKS is fetched lazily on verify.
func newDiscoveryStub(t *testing.T, withTokenEndpoint bool) string {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		doc := map[string]any{
			"issuer":                                srv.URL,
			"jwks_uri":                              srv.URL + "/keys",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		}
		if withTokenEndpoint {
			doc["token_endpoint"] = srv.URL + "/token"
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(doc))
	})
	return srv.URL
}

func TestOIDCValidator_Unconfigured(t *testing.T) {
	t.Run("empty issuer URL yields a no-op validator, not an error", func(t *testing.T) {
		v, r, err := oidcValidator(context.Background(), &config.Config{OIDCIssuerURL: ""})
		require.NoError(t, err)
		assert.Nil(t, v, "sso.set must reply unavailable when OIDC is unconfigured")
		assert.Nil(t, r, "sso.refresh must reply unavailable when OIDC is unconfigured")
	})

	t.Run("empty issuer URL short-circuits before any other OIDC config is read", func(t *testing.T) {
		// Audiences/ClientID/TLSSkipVerify are all set, but the unset issuer wins.
		v, r, err := oidcValidator(context.Background(), &config.Config{
			OIDCIssuerURL: "",
			OIDCAudiences: []string{"nats-chat"},
			OIDCClientID:  "nats-chat",
			TLSSkipVerify: true,
		})
		require.NoError(t, err)
		assert.Nil(t, v)
		assert.Nil(t, r)
	})
}

func TestOIDCValidator_ConstructionErrors(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.Config
		wantErr string
	}{
		{
			// pkgoidc.NewValidator rejects an empty audience list before any I/O.
			name:    "issuer set but no audiences",
			cfg:     config.Config{OIDCIssuerURL: "https://issuer.invalid/realms/chat"},
			wantErr: "init oidc validator",
		},
		{
			name: "no audiences with TLS verification disabled",
			cfg: config.Config{
				OIDCIssuerURL: "https://issuer.invalid/realms/chat",
				TLSSkipVerify: true,
			},
			wantErr: "init oidc validator",
		},
		{
			name: "malformed issuer URL",
			cfg: config.Config{
				OIDCIssuerURL: "://not-a-url",
				OIDCAudiences: []string{"nats-chat"},
				OIDCClientID:  "nats-chat",
			},
			wantErr: "init oidc validator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, r, err := oidcValidator(context.Background(), &tt.cfg)
			require.Error(t, err)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, v)
			assert.Nil(t, r)
		})
	}
}

func TestOIDCValidator_Configured(t *testing.T) {
	t.Run("reachable issuer returns the same value as validator and refresher", func(t *testing.T) {
		issuer := newDiscoveryStub(t, true)

		v, r, err := oidcValidator(context.Background(), &config.Config{
			OIDCIssuerURL: issuer,
			OIDCAudiences: []string{"nats-chat"},
			OIDCClientID:  "nats-chat",
		})
		require.NoError(t, err)
		require.NotNil(t, v)
		require.NotNil(t, r)
		assert.Same(t, v, r, "validator and refresher must be the same *oidc.Validator")
	})

	t.Run("TLS verification disabled still builds a validator", func(t *testing.T) {
		issuer := newDiscoveryStub(t, true)

		v, r, err := oidcValidator(context.Background(), &config.Config{
			OIDCIssuerURL: issuer,
			OIDCAudiences: []string{"nats-chat"},
			OIDCClientID:  "nats-chat",
			TLSSkipVerify: true,
		})
		require.NoError(t, err)
		assert.NotNil(t, v)
		assert.NotNil(t, r)
	})

	t.Run("issuer without token_endpoint but with client ID fails fast", func(t *testing.T) {
		issuer := newDiscoveryStub(t, false)

		v, r, err := oidcValidator(context.Background(), &config.Config{
			OIDCIssuerURL: issuer,
			OIDCAudiences: []string{"nats-chat"},
			OIDCClientID:  "nats-chat",
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "init oidc validator")
		assert.Nil(t, v)
		assert.Nil(t, r)
	})
}
