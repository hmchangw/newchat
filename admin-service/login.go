package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/pwhash"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
)

// dummyBcrypt is a bcrypt hash computed once at package init (cost 10, matching
// the prod default) so the user-not-found and not-admin denial paths can burn
// a comparable amount of time to the real pwhash.Verify call on the happy
// path. Without this, those two arms return before ever touching bcrypt,
// leaking "this is a known admin account" via response latency.
var dummyBcrypt = mustHashDummy()

func mustHashDummy() string {
	h, err := pwhash.Hash("timing-dummy", 10)
	if err != nil {
		// Only fails if the bcrypt library itself is broken — fail loud at
		// startup rather than silently skipping the timing guard.
		panic(err)
	}
	return h
}

// loginRequest is the request body for POST /v1/login.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// loginResponse is the response body for POST /v1/login. Deliberately
// excludes userId/baseUrl/natsUrl — admin-frontend never opens NATS.
type loginResponse struct {
	AuthToken             string `json:"authToken"`
	Account               string `json:"account"`
	SiteID                string `json:"siteId"`
	RequirePasswordChange bool   `json:"requirePasswordChange"`
}

// handleLogin handles POST /v1/login — verifies platform-admin credentials
// and issues a session token. User-not-found, not-admin, wrong-password, and
// deactivated all fall through to the same generic invalid_credentials
// response so a bot can't distinguish the cases by shape or reason.
func (h *Handler) handleLogin(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.Username == "" || req.Password == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("username and password are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}
	ctx = errcode.WithLogValues(ctx, "account", req.Username)

	u, err := h.store.GetUserForAuth(ctx, h.cfg.SiteID, req.Username)
	switch {
	case errors.Is(err, ErrUserNotFound):
		// No real hash to compare against — burn a comparable amount of time
		// against the dummy hash so latency doesn't leak "unknown account".
		_ = pwhash.Verify(dummyBcrypt, req.Password)
		h.loginDenied(c, ctx, req.Username)
		return
	case err != nil:
		errhttp.Write(ctx, c, fmt.Errorf("get user by account: %w", err))
		return
	}

	if !model.IsPlatformAdmin(u) {
		// Same timing guard as user-not-found — a bot/non-admin account must
		// not be distinguishable from "unknown" by response latency.
		_ = pwhash.Verify(dummyBcrypt, req.Password)
		h.loginDenied(c, ctx, req.Username)
		return
	}

	if !pwhash.Verify(u.Services.Password.Bcrypt, req.Password) {
		h.loginDenied(c, ctx, req.Username)
		return
	}

	// Deactivated check after password verify — keeps timing indistinguishable
	// from wrong-password, so bots can't probe accounts.
	if u.Deactivated {
		h.loginDenied(c, ctx, req.Username)
		return
	}

	raw, err := sessiontoken.New()
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("generate token: %w", err))
		return
	}
	roles := make([]string, len(u.Roles))
	for i, r := range u.Roles {
		roles[i] = string(r)
	}
	s := &session.Session{
		ID:       sessiontoken.Hash(raw),
		UserID:   u.ID,
		Account:  u.Account,
		SiteID:   u.SiteID,
		Roles:    roles,
		IssuedAt: nowMillis(),
	}
	if err := h.sessions.Insert(ctx, s); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("insert session: %w", err))
		return
	}

	if h.cfg.SessionsMaxPerAccount > 0 {
		if _, err := h.sessions.DeleteBeyondCap(ctx, u.Account, h.cfg.SessionsMaxPerAccount); err != nil {
			// Eviction is best-effort — log but don't fail the login.
			slog.WarnContext(ctx, "evict sessions failed", "error", err)
		}
	}

	slog.InfoContext(ctx, "admin login", "login_outcome", "ok", "account", req.Username)
	c.JSON(http.StatusOK, loginResponse{
		AuthToken:             raw,
		Account:               u.Account,
		SiteID:                u.SiteID,
		RequirePasswordChange: u.RequirePasswordChange,
	})
}

// loginDenied writes the uniform invalid_credentials response shared by every
// login-denial path (user-not-found, not-admin, wrong-password, deactivated).
func (h *Handler) loginDenied(c *gin.Context, ctx context.Context, account string) {
	slog.WarnContext(ctx, "admin login", "login_outcome", "denied", "account", account)
	errhttp.Write(ctx, c, errcode.Unauthenticated("invalid credentials",
		errcode.WithReason(errcode.AdminInvalidCredentials)))
}

// changePasswordRequest is the request body for POST /v1/password/change.
type changePasswordRequest struct {
	OldPassword string `json:"oldPassword"`
	NewPassword string `json:"newPassword"`
}

// handleChangePassword handles POST /v1/password/change — the logged-in
// admin's self-service password change. Verifies the old password, then
// atomically updates the bcrypt hash (clearing requirePasswordChange) and
// revokes every OTHER session for the account (admin-console and
// chat-frontend share the one session collection) in a single Mongo
// transaction, and audits the change. The caller's own session is preserved
// via the exceptSessionID argument so they stay logged in.
func (h *Handler) handleChangePassword(c *gin.Context) {
	ctx := errcode.WithLogValues(c.Request.Context(), "request_id", c.GetString("request_id"))

	var req changePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.OldPassword == "" || req.NewPassword == "" {
		errhttp.Write(ctx, c, errcode.BadRequest("oldPassword and newPassword are required",
			errcode.WithReason(errcode.AuthMissingFields)))
		return
	}

	caller := principalFrom(c)
	ctx = errcode.WithLogValues(ctx, "account", caller.Account)

	u, err := h.store.GetUserForAuth(ctx, caller.SiteID, caller.Account)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("get user for change-password: %w", err))
		return
	}

	if !pwhash.Verify(u.Services.Password.Bcrypt, req.OldPassword) {
		errhttp.Write(ctx, c, errcode.Unauthenticated("old password does not match",
			errcode.WithReason(errcode.AdminOldPasswordMismatch)))
		return
	}

	newHash, err := pwhash.Hash(req.NewPassword, h.cfg.BcryptCost)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("hash password: %w", err))
		return
	}

	// requireChange=false: caller has just satisfied the change-password
	// prompt, don't re-prompt them on next login. The password write and the
	// sibling-session revoke run in one Mongo transaction (admin-console AND
	// chat-frontend sessions, since both live in the shared collection) — the
	// caller's own session is preserved via exceptSessionID so they stay
	// logged in. A failure surfaces as a 500; there is no partial state where
	// the password changed but stale sessions remain valid.
	if err := h.store.UpdateUserPasswordAndRevoke(ctx, caller.SiteID, caller.Account, newHash, false, caller.ID); err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("update user password and revoke sessions: %w", err))
		return
	}

	if err := h.store.AppendAudit(ctx, &AuditEntry{
		ID:            idgen.GenerateID(),
		ActorUserID:   caller.UserID,
		ActorAccount:  caller.Account,
		Action:        "password_change_self",
		TargetUserID:  caller.UserID,
		TargetAccount: caller.Account,
		SiteID:        caller.SiteID,
		Timestamp:     nowMillis(),
	}); err != nil {
		slog.WarnContext(ctx, "append audit failed", "error", err)
	}

	c.Status(http.StatusNoContent)
}
