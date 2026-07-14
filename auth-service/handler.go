package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	pkgoidc "github.com/hmchangw/chat/pkg/oidc"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/subject"
)

// TokenValidator validates an SSO token and returns OIDC claims.
type TokenValidator interface {
	Validate(ctx context.Context, rawToken string) (pkgoidc.Claims, error)
}

type authRequest struct {
	// Exactly one of SSOToken / AuthToken must be set: SSOToken -> OIDC path,
	// AuthToken -> botplatform-validate path (bots/admins), neither -> dev mode.
	SSOToken      string `json:"ssoToken"`
	AuthToken     string `json:"authToken"`
	Account       string `json:"account"`
	NATSPublicKey string `json:"natsPublicKey" binding:"required"`
}

type authResponse struct {
	NATSJWT  string       `json:"natsJwt"`
	UserInfo userInfoResp `json:"user"`
}

type userInfoResp struct {
	Email       string `json:"email"`
	Account     string `json:"account"`
	EmployeeID  string `json:"employeeId"`
	EngName     string `json:"engName"`
	ChineseName string `json:"chineseName"`
	DeptName    string `json:"deptName"`
	DeptID      string `json:"deptId"`
}

// BotplatformValidator validates a session authToken against botplatform-service.
// Returns errcode.Unauthenticated for invalid tokens, a raw wrapped error otherwise (503 upstream).
type BotplatformValidator interface {
	Validate(ctx context.Context, authToken string) (principal.Principal, error)
}

// AuthHandler validates SSO or botplatform session tokens and returns signed,
// scoped NATS user JWTs.
type AuthHandler struct {
	validator     TokenValidator
	bpValidator   BotplatformValidator // optional; nil disables the session-token branch
	signingKey    nkeys.KeyPair
	accountPubKey string
	jwtExpiry     time.Duration
	jwtJitter     float64        // fraction of jwtExpiry; 0 = fixed lifetime
	randFloat     func() float64 // injectable [0,1) source; defaults to crypto rand
	devMode       bool
}

// Option configures optional AuthHandler behavior.
type Option func(*AuthHandler)

// WithJitter sets the JWT-lifetime jitter fraction (clamped to [0, 0.9]) so a
// fleet of sessions minted together does not expire in lockstep.
func WithJitter(frac float64) Option {
	return func(h *AuthHandler) {
		if frac < 0 {
			frac = 0
		}
		if frac > 0.9 {
			frac = 0.9
		}
		h.jwtJitter = frac
	}
}

// WithRandFloat overrides the randomness source (test seam).
func WithRandFloat(fn func() float64) Option {
	return func(h *AuthHandler) { h.randFloat = fn }
}

// WithBotplatformValidator enables the session-token branch of POST /auth.
// Without it, a request carrying an authToken is rejected as if the field
// were unsupported.
func WithBotplatformValidator(v BotplatformValidator) Option {
	return func(h *AuthHandler) { h.bpValidator = v }
}

// NewAuthHandler creates an AuthHandler; accountPubKey stamps the JWT's issuer_account claim.
func NewAuthHandler(validator TokenValidator, signingKey nkeys.KeyPair, accountPubKey string, jwtExpiry time.Duration, devMode bool, opts ...Option) *AuthHandler {
	h := &AuthHandler{
		validator:     validator,
		signingKey:    signingKey,
		accountPubKey: accountPubKey,
		jwtExpiry:     jwtExpiry,
		randFloat:     cryptoRandFloat,
		devMode:       devMode,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// cryptoRandFloat returns a uniform float in [0,1) from crypto/rand. On the
// (practically impossible) read error it returns 0.5 — the no-skew midpoint.
func cryptoRandFloat() float64 {
	const denom = 1 << 53
	n, err := rand.Int(rand.Reader, big.NewInt(denom))
	if err != nil {
		slog.Error("crypto/rand read failed, using no-skew midpoint for JWT jitter", "error", err)
		return 0.5
	}
	return float64(n.Int64()) / float64(denom)
}

// HandleAuth routes on which of ssoToken/authToken is set (OIDC vs botplatform
// session), both set is 400 ambiguous_token, and neither is dev-mode mint or
// 400 missing_token in prod. A dev-mode request carrying a token still
// validates normally — only the fully tokenless case short-circuits.
func (h *AuthHandler) HandleAuth(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req authRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		errhttp.Write(ctx, c, errcode.BadRequest("natsPublicKey is required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	if !nkeys.IsValidPublicUserKey(req.NATSPublicKey) {
		errhttp.Write(ctx, c, errcode.BadRequest("invalid natsPublicKey format",
			errcode.WithReason(errcode.AuthInvalidNKey)))
		return
	}

	switch {
	case req.SSOToken != "" && req.AuthToken != "":
		errhttp.Write(ctx, c, errcode.BadRequest("set exactly one of ssoToken / authToken",
			errcode.WithReason(errcode.BotplatformAmbiguousToken)))
	case req.SSOToken != "":
		h.handleSSO(ctx, c, req)
	case req.AuthToken != "":
		h.handleSession(ctx, c, req)
	case h.devMode:
		h.handleDevAuth(ctx, c, req)
	default:
		errhttp.Write(ctx, c, errcode.BadRequest("set exactly one of ssoToken / authToken",
			errcode.WithReason(errcode.BotplatformMissingToken)))
	}
}

// handleSSO runs the existing OIDC validation + JWT mint. Behavior unchanged
// from the pre-extension code path.
func (h *AuthHandler) handleSSO(ctx context.Context, c *gin.Context, req authRequest) {
	claims, err := h.validator.Validate(ctx, req.SSOToken)
	if err != nil {
		if errors.Is(err, pkgoidc.ErrTokenExpired) {
			errhttp.Write(ctx, c, errcode.Unauthenticated("SSO token has expired, please re-login",
				errcode.WithReason(errcode.AuthTokenExpired)))
			return
		}
		errhttp.Write(ctx, c, errcode.Unauthenticated("invalid SSO token",
			errcode.WithReason(errcode.AuthInvalidToken),
			errcode.WithCause(err)))
		return
	}

	account := claims.Account()
	if account == "" {
		errhttp.Write(ctx, c, errcode.Unauthenticated("token missing account claim",
			errcode.WithReason(errcode.AuthInvalidToken)))
		return
	}
	if !subject.IsValidAccountToken(account) {
		errhttp.Write(ctx, c, errcode.BadRequest("account must be a single NATS subject token (no '.', '*', '>' or whitespace)"))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", account)

	natsJWT, err := h.signNATSJWT(req.NATSPublicKey, account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generating NATS token: %w", err))
		return
	}

	slog.Debug("auth success", "account", account, "subject", claims.Subject)
	employeeID, engName, chineseName := parseDescription(claims.Description)
	c.JSON(http.StatusOK, authResponse{
		NATSJWT: natsJWT,
		UserInfo: userInfoResp{
			Email:       claims.Email,
			Account:     account,
			EmployeeID:  employeeID,
			EngName:     engName,
			ChineseName: chineseName,
			DeptName:    claims.DeptName,
			DeptID:      claims.DeptID,
		},
	})
}

// handleSession exchanges a botplatform session authToken for a NATS JWT,
// stamped with `account:<name>` for the server-side scoped signing key template.
func (h *AuthHandler) handleSession(ctx context.Context, c *gin.Context, req authRequest) {
	if h.bpValidator == nil {
		errhttp.Write(ctx, c, errcode.Unavailable("session-token auth not configured",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable)))
		return
	}

	p, err := h.bpValidator.Validate(ctx, req.AuthToken)
	if err != nil {
		var ec *errcode.Error
		if errors.As(err, &ec) {
			errhttp.Write(ctx, c, ec)
			return
		}
		errhttp.Write(ctx, c, errcode.Unavailable("botplatform unavailable",
			errcode.WithReason(errcode.BotplatformUpstreamUnavailable),
			errcode.WithCause(err)))
		return
	}
	if p.Account == "" {
		errhttp.Write(ctx, c, errcode.Unauthenticated("principal missing account",
			errcode.WithReason(errcode.AuthInvalidToken)))
		return
	}
	// NATS subject slots are single-token, so dotted bot accounts
	// (`botname.shortsiteid.bot`) collapse to underscores; others pass through.
	natsAccount := strings.ReplaceAll(p.Account, ".", "_")
	if !subject.IsValidAccountToken(natsAccount) {
		errhttp.Write(ctx, c, errcode.BadRequest("account contains invalid characters"))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", p.Account, "nats_account", natsAccount)

	natsJWT, err := h.signNATSJWT(req.NATSPublicKey, natsAccount)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generating NATS token: %w", err))
		return
	}

	slog.Debug("session auth success", "account", p.Account, "nats_account", natsAccount, "roles", p.Roles)
	c.JSON(http.StatusOK, authResponse{
		NATSJWT: natsJWT,
		UserInfo: userInfoResp{
			// Return the NATS-safe form so the client can build subject
			// prefixes (`chat.user.<account>.>`) directly from this field.
			Account: natsAccount,
			// No employee fields for bot/admin sessions — botplatform's
			// principal carries identity, not directory metadata.
		},
	})
}

// handleDevAuth mints a JWT directly from req.Account without OIDC/botplatform
// validation, for local development only. Only reached from HandleAuth's
// tokenless branch, which has already parsed and nkey-validated req.
func (h *AuthHandler) handleDevAuth(ctx context.Context, c *gin.Context, req authRequest) {
	if !subject.IsValidAccountToken(req.Account) {
		errhttp.Write(ctx, c, errcode.BadRequest("account must be a single NATS subject token (no '.', '*', '>' or whitespace)"))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Account)

	natsJWT, err := h.signNATSJWT(req.NATSPublicKey, req.Account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generating NATS token: %w", err))
		return
	}

	slog.Debug("dev auth success", "account", req.Account)

	c.JSON(http.StatusOK, authResponse{
		NATSJWT: natsJWT,
		UserInfo: userInfoResp{
			Email:   req.Account + "@dev.local",
			Account: req.Account,
			EngName: req.Account,
		},
	})
}

// signNATSJWT signs a scoped NATS user JWT; permissions come from the
// account's scoped signing key template, not this code. IssuerAccount marks
// which account the (non-root) signing key belongs to.
//
// Effective grants (kept in sync with docker-local/setup.sh and
// docs/client-api.md §2.1 — a platform-team template change must mirror both):
//
//	Pub allow: chat.user.{account}.>, _INBOX.>, chat.user.presence.*.query.batch (+allow-pub-response)
//	Sub allow: chat.user.{account}.>, chat.room.>, _INBOX.>, chat.user.presence.state.*
func (h *AuthHandler) signNATSJWT(userPubKey, account string) (string, error) {
	uc := jwt.NewUserClaims(userPubKey)
	uc.IssuerAccount = h.accountPubKey
	uc.Expires = h.jwtExpiryAt().Unix()
	uc.Tags.Add("account:" + account)
	uc.SetScoped(true)
	return uc.Encode(h.signingKey)
}

// jwtExpiryAt returns the absolute expiry, applying ±jwtJitter around the base
// lifetime: factor = 1 + jitter*(2r-1), r in [0,1).
func (h *AuthHandler) jwtExpiryAt() time.Time {
	factor := 1 + h.jwtJitter*(2*h.randFloat()-1)
	return time.Now().Add(time.Duration(float64(h.jwtExpiry) * factor))
}

// parseDescription splits the description field "employeeId, engName, chineseName"
// into its three components.
func parseDescription(desc string) (employeeID, engName, chineseName string) {
	parts := strings.SplitN(desc, ",", 3)
	if len(parts) >= 1 {
		employeeID = strings.TrimSpace(parts[0])
	}
	if len(parts) >= 2 {
		engName = strings.TrimSpace(parts[1])
	}
	if len(parts) >= 3 {
		chineseName = strings.TrimSpace(parts[2])
	}
	return
}

func (h *AuthHandler) HandleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
