package svcjwt

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testIssuer   = "admin-service"
	testAudience = "client-update-service"
	testSubject  = "svc-updater"
)

// testKeys returns a fresh keypair in the base64 form the constructors expect.
func testKeys(t *testing.T) (seedB64, pubB64 string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	enc := base64.StdEncoding
	return enc.EncodeToString(priv.Seed()), enc.EncodeToString(pub)
}

// testPair returns a signer and a verifier that trust each other.
func testPair(t *testing.T) (*Signer, *Verifier) {
	t.Helper()
	seed, pub := testKeys(t)
	s, err := NewSigner(seed, testIssuer)
	require.NoError(t, err)
	v, err := NewVerifier(pub, testIssuer, testAudience)
	require.NoError(t, err)
	return s, v
}

// retoken re-signs mutated claims with the signer's key, so a test can change a
// claim without invalidating the signature (isolating claim checks from the
// signature check).
func retoken(t *testing.T, s *Signer, c Claims) string { //nolint:gocritic // hugeParam: c is passed by value so callers can pass a local copy without aliasing it
	t.Helper()
	hb, err := json.Marshal(header{Alg: alg, Typ: "JWT"})
	require.NoError(t, err)
	cb, err := json.Marshal(c)
	require.NoError(t, err)
	signing := b64(hb) + "." + b64(cb)
	return signing + "." + b64(ed25519.Sign(s.key, []byte(signing)))
}

// claimsOf decodes a token's payload without verifying it.
func claimsOf(t *testing.T, token string) Claims {
	t.Helper()
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	raw, err := unb64(parts[1])
	require.NoError(t, err)
	var c Claims
	require.NoError(t, json.Unmarshal(raw, &c))
	return c
}

func TestSigner_Verifier_RoundTrip(t *testing.T) {
	s, v := testPair(t)

	token, exp, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	assert.NotEmpty(t, token)
	assert.Greater(t, exp, time.Now().Unix())

	got, err := v.Verify(token)
	require.NoError(t, err)
	assert.Equal(t, testIssuer, got.Issuer)
	assert.Equal(t, testSubject, got.Subject)
	assert.Equal(t, testAudience, got.Audience)
	assert.Equal(t, exp, got.ExpiresAt)
	assert.NotEmpty(t, got.JTI, "every token needs a unique id")
}

func TestSigner_Sign_UniqueJTI(t *testing.T) {
	s, _ := testPair(t)
	a, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	b, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	assert.NotEqual(t, claimsOf(t, a).JTI, claimsOf(t, b).JTI)
}

func TestSigner_Sign_RejectsBadArguments(t *testing.T) {
	s, _ := testPair(t)
	tests := []struct {
		name              string
		subject, audience string
		ttl               time.Duration
	}{
		{"empty subject", "", testAudience, time.Hour},
		{"empty audience", testSubject, "", time.Hour},
		{"zero ttl", testSubject, testAudience, 0},
		{"negative ttl", testSubject, testAudience, -time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := s.Sign(tc.subject, tc.audience, tc.ttl)
			assert.Error(t, err)
		})
	}
}

// TestVerify_RejectsInvalidTokens is the security core: one case per rule in
// spec §4.2. Every failure must wrap ErrInvalidToken so callers answer
// uniformly on the wire.
func TestVerify_RejectsInvalidTokens(t *testing.T) {
	s, v := testPair(t)
	valid, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	parts := strings.Split(valid, ".")

	// A second, unrelated keypair: a correctly-formed token signed by the wrong key.
	otherSeed, _ := testKeys(t)
	other, err := NewSigner(otherSeed, testIssuer)
	require.NoError(t, err)
	foreign, _, err := other.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)

	// swapAlg re-encodes the header with a different alg, keeping the original
	// payload and signature.
	swapAlg := func(a string) string {
		hb, err := json.Marshal(map[string]string{"alg": a, "typ": "JWT"})
		require.NoError(t, err)
		return b64(hb) + "." + parts[1] + "." + parts[2]
	}

	tests := []struct {
		name  string
		token string
	}{
		{"empty", ""},
		{"two segments", parts[0] + "." + parts[1]},
		{"four segments", valid + ".extra"},
		{"header not base64", "!!!." + parts[1] + "." + parts[2]},
		{"header not json", b64([]byte("not json")) + "." + parts[1] + "." + parts[2]},
		{"alg none", swapAlg("none")},
		{"alg HS256", swapAlg("HS256")},
		{"alg empty", swapAlg("")},
		{"signature not base64", parts[0] + "." + parts[1] + ".!!!"},
		{"signature tampered", parts[0] + "." + parts[1] + "." + b64([]byte("wrong signature bytes here"))},
		{"payload tampered", parts[0] + "." + b64([]byte(`{"iss":"admin-service","sub":"attacker"}`)) + "." + parts[2]},
		{"signed by another key", foreign},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.Verify(tc.token)
			assert.Nil(t, got)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidToken,
				"every verification failure must wrap ErrInvalidToken")
		})
	}
}

// TestVerify_RejectsBadClaims isolates the claim rules: each token is correctly
// signed, so only the claim under test can cause the failure.
func TestVerify_RejectsBadClaims(t *testing.T) {
	s, v := testPair(t)
	now := time.Now().UTC()
	base := Claims{
		Issuer: testIssuer, Subject: testSubject, Audience: testAudience,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(), JTI: "test-jti",
	}

	mutate := func(f func(*Claims)) string {
		c := base
		f(&c)
		return retoken(t, s, c)
	}

	tests := []struct {
		name  string
		token string
	}{
		{"wrong issuer", mutate(func(c *Claims) { c.Issuer = "someone-else" })},
		{"empty issuer", mutate(func(c *Claims) { c.Issuer = "" })},
		{"wrong audience", mutate(func(c *Claims) { c.Audience = "upload-service" })},
		{"empty audience", mutate(func(c *Claims) { c.Audience = "" })},
		{"empty subject", mutate(func(c *Claims) { c.Subject = "" })},
		{"no expiry", mutate(func(c *Claims) { c.ExpiresAt = 0 })},
		{"expired beyond leeway", mutate(func(c *Claims) {
			c.ExpiresAt = now.Add(-leeway - time.Minute).Unix()
		})},
		{"issued far in the future", mutate(func(c *Claims) {
			c.IssuedAt = now.Add(leeway + time.Minute).Unix()
		})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := v.Verify(tc.token)
			assert.Nil(t, got)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidToken)
		})
	}
}

// TestVerify_ExpiryLeeway pins the skew allowance: just-expired passes,
// expired past the leeway does not.
func TestVerify_ExpiryLeeway(t *testing.T) {
	s, v := testPair(t)
	token, _, err := s.Sign(testSubject, testAudience, time.Minute)
	require.NoError(t, err)

	v.now = func() time.Time { return time.Now().Add(time.Minute + leeway/2) }
	_, err = v.Verify(token)
	assert.NoError(t, err, "just past exp but inside leeway must still verify")

	v.now = func() time.Time { return time.Now().Add(time.Minute + leeway + time.Second) }
	_, err = v.Verify(token)
	assert.ErrorIs(t, err, ErrInvalidToken, "past exp+leeway must fail")
}

// TestVerify_ErrorsCarryNoToken guards the logging rule: the token must never
// appear in an error string, since the caller attaches it as an errcode cause.
func TestVerify_ErrorsCarryNoToken(t *testing.T) {
	s, v := testPair(t)
	token, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)
	parts := strings.Split(token, ".")
	tampered := parts[0] + "." + parts[1] + "." + b64([]byte("nope"))

	_, err = v.Verify(tampered)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), parts[1], "claims segment must not leak into the error")
	assert.NotContains(t, err.Error(), tampered)
}

func TestNewSigner_RejectsBadKey(t *testing.T) {
	seed, _ := testKeys(t)
	tests := []struct {
		name, key, issuer string
	}{
		{"empty key", "", testIssuer},
		{"not base64", "!!!not base64!!!", testIssuer},
		{"wrong length", base64.StdEncoding.EncodeToString([]byte("too short")), testIssuer},
		{"empty issuer", seed, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewSigner(tc.key, tc.issuer)
			assert.Nil(t, got)
			assert.Error(t, err)
		})
	}
}

func TestNewVerifier_RejectsBadKey(t *testing.T) {
	_, pub := testKeys(t)
	tests := []struct {
		name, key, issuer, audience string
	}{
		{"empty key", "", testIssuer, testAudience},
		{"not base64", "!!!", testIssuer, testAudience},
		{"wrong length", base64.StdEncoding.EncodeToString([]byte("short")), testIssuer, testAudience},
		{"empty issuer", pub, "", testAudience},
		{"empty audience", pub, testIssuer, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewVerifier(tc.key, tc.issuer, tc.audience)
			assert.Nil(t, got)
			assert.Error(t, err)
		})
	}
}

// TestVerify_AudienceIsolation proves a token minted for one service is
// refused by another — the reason `aud` exists.
func TestVerify_AudienceIsolation(t *testing.T) {
	seed, pub := testKeys(t)
	s, err := NewSigner(seed, testIssuer)
	require.NoError(t, err)
	other, err := NewVerifier(pub, testIssuer, "upload-service")
	require.NoError(t, err)

	token, _, err := s.Sign(testSubject, testAudience, time.Hour)
	require.NoError(t, err)

	_, err = other.Verify(token)
	assert.ErrorIs(t, err, ErrInvalidToken)
}

var _ = errors.Is // keep the errors import meaningful if assertions change
