package oidc

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeIssuer is a stdlib-only OIDC issuer: discovery, JWKS, RS256 minting, and a swappable token endpoint for refresh-grant tests.
type fakeIssuer struct {
	t            *testing.T
	key          *rsa.PrivateKey
	srv          *httptest.Server
	TokenHandler http.HandlerFunc // set per-test; 500s when nil
}

func newFakeIssuer(t *testing.T) *fakeIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	f := &fakeIssuer{t: t, key: key}

	mux := http.NewServeMux()
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{
			"issuer":                                f.srv.URL,
			"jwks_uri":                              f.srv.URL + "/keys",
			"token_endpoint":                        f.srv.URL + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "alg": "RS256", "use": "sig", "kid": "test-key",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if f.TokenHandler == nil {
			http.Error(w, "no token handler configured", http.StatusInternalServerError)
			return
		}
		f.TokenHandler(w, r)
	})
	return f
}

func (f *fakeIssuer) URL() string { return f.srv.URL }

// Mint signs an RS256 JWT with sane defaults; overrides replace/add claims (set a value to nil to delete a default claim).
func (f *fakeIssuer) Mint(overrides map[string]any) string {
	f.t.Helper()
	claims := map[string]any{
		"iss":                f.srv.URL,
		"aud":                "nats-chat",
		"sub":                "user-1",
		"preferred_username": "alice",
		"iat":                time.Now().Add(-time.Minute).Unix(),
		"exp":                time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range overrides {
		if v == nil {
			delete(claims, k)
			continue
		}
		claims[k] = v
	}
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": "test-key"}
	signing := b64JSON(f.t, header) + "." + b64JSON(f.t, claims)
	digest := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, digest[:])
	require.NoError(f.t, err)
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func b64JSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(b)
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}
