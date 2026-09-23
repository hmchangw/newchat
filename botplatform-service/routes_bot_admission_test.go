package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
	"github.com/hmchangw/chat/pkg/valkeyfake"
)

// The bot routes' per-request cost before the token is known valid is one
// uncached MongoDB lookup: sessioncache is positive-only (pkg/sessioncache:12),
// so an invalid token can never be served from cache and reaches Mongo every
// time. botRateLimit cannot meter that — it needs the principal requireBot
// establishes — so the only control that can run first is admission.
//
// The assertion that matters is not the 429: it is that the shed request never
// reached the store. That is the Mongo load the cap exists to prevent.
func TestRegisterBotRoutes_ShedsBeforeTheSessionLookup(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// #nosec G101 -- fabricated test fixture, not a live credential
	// nosemgrep: gosec.G101-1, hardcoded-credential-literal
	const rawToken = "admission-test-token"
	botSess := &session.Session{
		ID: sessiontoken.Hash(rawToken), UserID: "bot-user-id",
		Account: "myapp.bot", SiteID: "site-a", Roles: []string{"bot"},
	}

	var lookups atomic.Int64
	entered := make(chan struct{})
	release := make(chan struct{})
	sessions := &sessionOnlyStore{
		FindSessionByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
			if lookups.Add(1) == 1 {
				entered <- struct{}{}
				<-release
			}
			return botSess, nil
		},
	}

	cfg := &config{
		SiteID:                      "site-a",
		BotRateLimitPerCallerPerMin: 100,
		BotRateLimitGlobalPerMin:    1000,
		BotIdempotencyMsgTTL:        30 * time.Second,
		BotIdempotencyRoomMgmtTTL:   60 * time.Second,
		BotRoutes:                   ginutil.ConcurrencyConfig{MaxConcurrency: 1},
	}
	h := &handler{
		cfg: cfg, store: sessions, forwarder: successForwarder{},
		subs:      &fakeSubStore{FindForBotFn: alwaysLocalSub, FindDMForBotFn: alwaysLocalSub},
		dmEnsurer: &fakeDMEnsurer{},
	}

	r := gin.New()
	registerBotRoutes(r, valkeyfake.New(), cfg, h)

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/r1/messages",
			bytes.NewReader([]byte(`{"content":"hi"}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-user-id", "bot-user-id")
		req.Header.Set("x-auth-token", rawToken)
		return req
	}

	go func() { r.ServeHTTP(httptest.NewRecorder(), newReq()) }()
	<-entered // the single slot is held, inside the store lookup

	w := httptest.NewRecorder()
	r.ServeHTTP(w, newReq())
	assert.Equal(t, http.StatusTooManyRequests, w.Code)
	assert.Equal(t, "1", w.Header().Get("Retry-After"))
	assert.Equal(t, int64(1), lookups.Load(),
		"the shed request must not have reached the session store — that lookup is the Mongo load being prevented")

	close(release)
}

// Unset means disabled, so adopting the field cannot silently cap bot traffic
// that runs fine today.
func TestRegisterBotRoutes_ZeroCapAdmitsEverything(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// #nosec G101 -- fabricated test fixture, not a live credential
	// nosemgrep: gosec.G101-1, hardcoded-credential-literal
	const rawToken = "admission-off-token"
	botSess := &session.Session{
		ID: sessiontoken.Hash(rawToken), UserID: "bot-user-id",
		Account: "myapp.bot", SiteID: "site-a", Roles: []string{"bot"},
	}
	sessions := &sessionOnlyStore{
		FindSessionByHashFn: func(_ context.Context, _ string) (*session.Session, error) { return botSess, nil },
	}
	cfg := &config{
		SiteID:                      "site-a",
		BotRateLimitPerCallerPerMin: 100,
		BotRateLimitGlobalPerMin:    1000,
		BotIdempotencyMsgTTL:        30 * time.Second,
		BotIdempotencyRoomMgmtTTL:   60 * time.Second,
		BotRoutes:                   ginutil.ConcurrencyConfig{MaxConcurrency: 0},
	}
	h := &handler{
		cfg: cfg, store: sessions, forwarder: successForwarder{},
		subs:      &fakeSubStore{FindForBotFn: alwaysLocalSub, FindDMForBotFn: alwaysLocalSub},
		dmEnsurer: &fakeDMEnsurer{},
	}
	r := gin.New()
	registerBotRoutes(r, valkeyfake.New(), cfg, h)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/r1/messages",
		bytes.NewReader([]byte(`{"content":"hi"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-user-id", "bot-user-id")
	req.Header.Set("x-auth-token", rawToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}
