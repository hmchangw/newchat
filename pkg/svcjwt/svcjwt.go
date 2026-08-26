// Package svcjwt mints and verifies the Ed25519 tokens that authenticate one
// internal service to another. Signing is asymmetric on purpose: the minting
// service holds the private key and the verifying service holds only the public
// key, so a compromised verifier cannot forge tokens for itself.
package svcjwt

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hmchangw/chat/pkg/idgen"
)

// alg is the only signing algorithm this package accepts. Verify COMPARES the
// token header against this constant rather than dispatching on it: there is no
// table mapping an algorithm name to a verifier, so "none" and HS256-confusion
// have no code path to reach.
const alg = "EdDSA"

// leeway absorbs clock skew between the minting and verifying services.
const leeway = 30 * time.Second

// ErrInvalidToken is wrapped by every Verify failure — bad structure, unknown
// algorithm, bad signature, wrong issuer or audience, expired. The wrapping
// message names the broken rule for the server log; callers match the sentinel
// and answer uniformly, so the wire never reveals which rule failed.
var ErrInvalidToken = errors.New("invalid service token")

// header is the fixed JWT header, never negotiated per token.
type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Claims is the token payload: registered JWT names only.
type Claims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	JTI       string `json:"jti"`
}

// Signer mints tokens. Construct once at startup and reuse.
type Signer struct {
	key    ed25519.PrivateKey
	issuer string
}

// NewSigner decodes seedB64 — base64 of the raw 32-byte Ed25519 seed — into a
// signer. A malformed or wrong-length seed fails here, at startup, rather than
// at the first request.
func NewSigner(seedB64, issuer string) (*Signer, error) {
	if issuer == "" {
		return nil, errors.New("new svcjwt signer: issuer must not be empty")
	}
	seed, err := decodeKey(seedB64, ed25519.SeedSize)
	if err != nil {
		return nil, fmt.Errorf("new svcjwt signer: %w", err)
	}
	return &Signer{key: ed25519.NewKeyFromSeed(seed), issuer: issuer}, nil
}

// Sign mints a token for subject/audience valid for ttl, returning it with its
// expiry as unix seconds.
func (s *Signer) Sign(subject, audience string, ttl time.Duration) (string, int64, error) {
	if subject == "" || audience == "" {
		return "", 0, errors.New("sign svcjwt: subject and audience must not be empty")
	}
	if ttl <= 0 {
		return "", 0, fmt.Errorf("sign svcjwt: ttl must be positive, got %s", ttl)
	}
	now := time.Now().UTC()
	exp := now.Add(ttl).Unix()
	hb, err := json.Marshal(header{Alg: alg, Typ: "JWT"})
	if err != nil {
		return "", 0, fmt.Errorf("marshal svcjwt header: %w", err)
	}
	cb, err := json.Marshal(Claims{
		Issuer:    s.issuer,
		Subject:   subject,
		Audience:  audience,
		IssuedAt:  now.Unix(),
		ExpiresAt: exp,
		JTI:       idgen.GenerateUUIDv7(),
	})
	if err != nil {
		return "", 0, fmt.Errorf("marshal svcjwt claims: %w", err)
	}
	signing := b64(hb) + "." + b64(cb)
	return signing + "." + b64(ed25519.Sign(s.key, []byte(signing))), exp, nil
}

// Verifier checks tokens against one issuer and one audience.
type Verifier struct {
	key      ed25519.PublicKey
	issuer   string
	audience string
	// now is injected so tests can drive expiry without sleeping.
	now func() time.Time
}

// NewVerifier decodes pubKeyB64 — base64 of the raw 32-byte Ed25519 public key.
func NewVerifier(pubKeyB64, issuer, audience string) (*Verifier, error) {
	if issuer == "" || audience == "" {
		return nil, errors.New("new svcjwt verifier: issuer and audience must not be empty")
	}
	pub, err := decodeKey(pubKeyB64, ed25519.PublicKeySize)
	if err != nil {
		return nil, fmt.Errorf("new svcjwt verifier: %w", err)
	}
	return &Verifier{key: ed25519.PublicKey(pub), issuer: issuer, audience: audience, now: time.Now}, nil
}

// Verify checks structure, then algorithm, then signature, then claims — in
// that order. Claims are unmarshalled only after the signature has proven the
// bytes authentic, so no attacker-controlled JSON is parsed on an unverified
// token. Error messages deliberately exclude the token and any segment of it.
func (v *Verifier) Verify(token string) (*Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("svcjwt: want 3 segments, got %d: %w", len(parts), ErrInvalidToken)
	}
	hb, err := unb64(parts[0])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: header is not base64url: %w", ErrInvalidToken)
	}
	var h header
	if err := json.Unmarshal(hb, &h); err != nil {
		return nil, fmt.Errorf("svcjwt: header is not json: %w", ErrInvalidToken)
	}
	// Compared, never dispatched on — see the alg const.
	if h.Alg != alg {
		return nil, fmt.Errorf("svcjwt: unsupported algorithm: %w", ErrInvalidToken)
	}
	sig, err := unb64(parts[2])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: signature is not base64url: %w", ErrInvalidToken)
	}
	if !ed25519.Verify(v.key, []byte(parts[0]+"."+parts[1]), sig) {
		return nil, fmt.Errorf("svcjwt: signature mismatch: %w", ErrInvalidToken)
	}

	// Authenticated from here on.
	cb, err := unb64(parts[1])
	if err != nil {
		return nil, fmt.Errorf("svcjwt: claims are not base64url: %w", ErrInvalidToken)
	}
	var c Claims
	if err := json.Unmarshal(cb, &c); err != nil {
		return nil, fmt.Errorf("svcjwt: claims are not json: %w", ErrInvalidToken)
	}
	if c.Issuer != v.issuer {
		return nil, fmt.Errorf("svcjwt: issuer mismatch: %w", ErrInvalidToken)
	}
	if c.Audience != v.audience {
		return nil, fmt.Errorf("svcjwt: audience mismatch: %w", ErrInvalidToken)
	}
	if c.Subject == "" {
		return nil, fmt.Errorf("svcjwt: empty subject: %w", ErrInvalidToken)
	}
	now := v.now().UTC()
	if c.ExpiresAt <= 0 || now.After(time.Unix(c.ExpiresAt, 0).Add(leeway)) {
		return nil, fmt.Errorf("svcjwt: token expired: %w", ErrInvalidToken)
	}
	if c.IssuedAt > 0 && time.Unix(c.IssuedAt, 0).After(now.Add(leeway)) {
		return nil, fmt.Errorf("svcjwt: issued in the future: %w", ErrInvalidToken)
	}
	return &c, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func unb64(s string) ([]byte, error) { return base64.RawURLEncoding.DecodeString(s) }

// decodeKey decodes a standard-base64 key and checks its decoded length, so a
// truncated key or one of the wrong kind is rejected at construction. The key
// itself never appears in the returned error.
func decodeKey(s string, want int) ([]byte, error) {
	if s == "" {
		return nil, errors.New("key must not be empty")
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.New("key is not valid base64")
	}
	if len(b) != want {
		return nil, fmt.Errorf("key must decode to %d bytes, got %d", want, len(b))
	}
	return b, nil
}
