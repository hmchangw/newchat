package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/principal"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

type handler struct {
	store     BotplatformStore
	cfg       *config
	forwarder botRPCForwarder
	subs      subscriptionStore
	dmEnsurer dmEnsurer

	// Test seams; production leaves these at defaults.
	tokenGen func() (string, error)
	now      func() int64
}

// botRPCForwarder is the narrow surface bot HTTP handlers use to reach the bot services.
type botRPCForwarder interface {
	sendRoom(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) (*model.Message, error)
	sendDM(ctx context.Context, sess *session.Session, siteID, targetUserID string, body []byte) (*model.Message, error)
	createRoom(ctx context.Context, sess *session.Session, siteID string, body []byte) ([]byte, error)
	addMembers(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) ([]byte, error)
	removeMembers(ctx context.Context, sess *session.Session, siteID, roomID string, body []byte) ([]byte, error)
}

var _ botRPCForwarder = (*botForwarder)(nil)

func newHandler(s BotplatformStore, cfg *config) *handler {
	return &handler{
		store:    s,
		cfg:      cfg,
		tokenGen: sessiontoken.New,
		now:      func() int64 { return time.Now().UTC().UnixMilli() },
	}
}

func (h *handler) HandleHealth(c *gin.Context) {
	if err := h.store.Ping(c.Request.Context()); err != nil {
		slog.WarnContext(c.Request.Context(), "healthz ping failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "down"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Status string    `json:"status"`
	Data   loginData `json:"data"`
}

type loginData struct {
	UserID    string  `json:"userId"`
	AuthToken string  `json:"authToken"`
	Me        meBlock `json:"me"`
}

type meBlock struct {
	ID                    string   `json:"_id"`
	Username              string   `json:"username"`
	Name                  string   `json:"name"`
	Active                bool     `json:"active"`
	Roles                 []string `json:"roles"`
	RequirePasswordChange bool     `json:"requirePasswordChange"`
}

func (h *handler) HandleLogin(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.Set("login_outcome", "bad_request")
		errhttp.Write(ctx, c, errMissingFields)
		return
	}
	if req.Username == "" || req.Password == "" {
		c.Set("login_outcome", "bad_request")
		errhttp.Write(ctx, c, errMissingFields)
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Username)

	u, err := h.store.FindUserByAccount(ctx, req.Username)
	switch {
	case errors.Is(err, mongo.ErrNoDocuments):
		h.denied(c, ctx, req.Username, "invalid_credentials")
		return
	case err != nil:
		errhttp.Write(ctx, c, fmt.Errorf("find user: %w", err))
		return
	}

	if !model.HasLoginRole(u.Roles) {
		h.denied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	if !verifyPassword(u.Services.Password.Bcrypt, req.Password) {
		h.denied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	// Deactivation check runs after password verify so timing matches wrong-password.
	if u.Deactivated {
		h.denied(c, ctx, req.Username, "invalid_credentials")
		return
	}

	raw, err := h.tokenGen()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generate token: %w", err))
		return
	}
	roleStrs := rolesToStrings(u.Roles)
	s := &session.Session{
		ID:       sessiontoken.Hash(raw),
		UserID:   u.ID,
		Account:  u.Account,
		SiteID:   u.SiteID,
		Roles:    roleStrs,
		IssuedAt: h.now(),
	}
	if err := h.store.InsertSession(ctx, s); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("insert session: %w", err))
		return
	}

	// FIFO cap: Skip-Sort-by-issuedAt returns only over-cap rows, avoiding a Count round-trip.
	if h.cfg.SessionsMaxPerAccount > 0 {
		if _, err := h.store.DeleteSessionsBeyondCap(ctx, u.Account, h.cfg.SessionsMaxPerAccount); err != nil {
			// Best-effort; don't fail login on eviction failure.
			slog.WarnContext(ctx, "evict sessions failed", "error", err)
		}
	}

	c.Set("login_outcome", "ok")
	slog.InfoContext(ctx, "login ok", "account", req.Username, "userId", u.ID)
	c.JSON(http.StatusOK, loginResponse{
		Status: "success",
		Data: loginData{
			UserID:    u.ID,
			AuthToken: raw,
			Me: meBlock{
				ID:                    u.ID,
				Username:              u.Account,
				Name:                  u.DisplayName(),
				Active:                !u.Deactivated,
				Roles:                 roleStrs,
				RequirePasswordChange: u.RequirePasswordChange,
			},
		},
	})
}

// denied writes the uniform 401 envelope; uses ctx so log-values (request_id, account) thread through.
func (h *handler) denied(c *gin.Context, ctx context.Context, account, reason string) {
	c.Set("login_outcome", reason)
	slog.WarnContext(ctx, "login denied", "account", account, "reason", reason)
	errhttp.Write(ctx, c, errInvalidCredentials)
}

type validateRequest struct {
	AuthToken string `json:"authToken"`
}

type validateResponse struct {
	Valid     bool                `json:"valid"`
	Principal principal.Principal `json:"principal"`
}

func (h *handler) HandleValidate(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req validateRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.AuthToken == "" {
		c.Set("validate_outcome", "bad_request")
		errhttp.Write(ctx, c, errMissingFields)
		return
	}

	s, err := h.store.FindSessionByHash(ctx, sessiontoken.Hash(req.AuthToken))
	switch {
	case errors.Is(err, session.ErrNotFound):
		c.Set("validate_outcome", "invalid_token")
		errhttp.Write(ctx, c, errInvalidToken)
		return
	case err != nil:
		errhttp.Write(ctx, c, fmt.Errorf("find session: %w", err))
		return
	}

	c.Set("validate_outcome", "ok")
	c.JSON(http.StatusOK, validateResponse{
		Valid: true,
		Principal: principal.Principal{
			UserID:  s.UserID,
			Account: s.Account,
			SiteID:  s.SiteID,
			Roles:   s.Roles,
		},
	})
}

// verifyPassword checks plaintext against the stored bcrypt(sha256hex) hash.
func verifyPassword(stored, plaintext string) bool {
	return pwhash.Verify(stored, plaintext)
}

// rolesToStrings converts []UserRole to []string; nil/empty becomes `[]` not `null`.
func rolesToStrings(roles []model.UserRole) []string {
	out := make([]string, len(roles))
	for i, r := range roles {
		out[i] = string(r)
	}
	return out
}
