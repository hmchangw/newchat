package main

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// login takes the admission cap: it is unauthenticated and pays bcrypt before
// the credential is known to be valid. /auth/validate resolves through the
// cached, breaker-fenced store lookup and healthz must answer while login is
// shedding, so neither is capped here.
func registerRoutes(r *gin.Engine, h *handler, login ginutil.ConcurrencyConfig, onShed func()) {
	r.GET("/healthz", h.HandleHealth)

	r.POST("/api/v1/login", login.Middleware(onShed), h.HandleLogin)
	r.POST("/api/v1/auth/validate", h.HandleValidate)
}

// registerBotRoutes attaches bot endpoints with the auth → rate-limit → idempotency → handler chain
// (login/validate use a different auth model). Nil valkey (dev) omits rate-limit + idempotency.
//
// Auth resolves through h.store, deliberately: that is the cached,
// breaker-fenced lookup /auth/validate already uses. Taking a session.Store of
// its own is what let the bot routes read Mongo raw on every request, so there
// is no second store to pass — the uncached path is not reachable from here.
func registerBotRoutes(r *gin.Engine, valkey valkeyutil.Client, cfg *config, h *handler) {
	auth := requireBot(h.store)

	var rateLimit gin.HandlerFunc
	var msgIdem, roomMgmtIdem func(endpoint string, resourceFrom resourceIDFunc) gin.HandlerFunc
	if valkey != nil {
		rateLimit = botRateLimit(valkey, cfg.BotRateLimitPerCallerPerMin, cfg.BotRateLimitGlobalPerMin)
		msgIdem = func(endpoint string, resourceFrom resourceIDFunc) gin.HandlerFunc {
			return botIdempotency(valkey, cfg.SiteID, endpoint, cfg.BotIdempotencyMsgTTL, resourceFrom, nil)
		}
		roomMgmtIdem = func(endpoint string, resourceFrom resourceIDFunc) gin.HandlerFunc {
			return botIdempotency(valkey, cfg.SiteID, endpoint, cfg.BotIdempotencyRoomMgmtTTL, resourceFrom, nil)
		}
	}

	// admit runs ahead of auth deliberately. requireBot's session lookup is the
	// per-request cost paid before the token is known valid, and it cannot be
	// rate-limited — botRateLimit needs the principal requireBot produces, so an
	// invalid token is never metered. Shedding here costs no I/O (a channel
	// select), whereas a rate limiter would need a Valkey round trip to decide
	// to reject and fails open when Valkey is down.
	admit := cfg.BotRoutes.Middleware(func() { botRequestsShed.Add(context.Background(), 1) })

	// chain composes admission + auth + rate-limit + idempotency with nils elided.
	chain := func(idem gin.HandlerFunc) []gin.HandlerFunc {
		out := []gin.HandlerFunc{admit, auth}
		if rateLimit != nil {
			out = append(out, rateLimit)
		}
		if idem != nil {
			out = append(out, idem)
		}
		return out
	}
	idemOrNil := func(build func(string, resourceIDFunc) gin.HandlerFunc, endpoint string, r resourceIDFunc) gin.HandlerFunc {
		if build == nil {
			return nil
		}
		return build(endpoint, r)
	}

	roomID := func(c *gin.Context) string { return c.Param("roomID") }
	userID := func(c *gin.Context) string { return c.Param("userID") }
	empty := func(*gin.Context) string { return "" }

	r.POST("/api/v1/rooms/:roomID/messages",
		append(chain(idemOrNil(msgIdem, "sendRoom", roomID)), h.botSendRoomMessage)...)

	r.POST("/api/v1/dms/:userID/messages",
		append(chain(idemOrNil(msgIdem, "sendDM", userID)), h.botSendDMMessage)...)

	r.POST("/api/v1/rooms",
		append(chain(idemOrNil(roomMgmtIdem, "createRoom", empty)), h.botCreateRoom)...)

	r.POST("/api/v1/rooms/:roomID/members/add",
		append(chain(idemOrNil(roomMgmtIdem, "addMember", roomID)), h.botAddMembers)...)

	r.POST("/api/v1/rooms/:roomID/members/remove",
		append(chain(idemOrNil(roomMgmtIdem, "removeMember", roomID)), h.botRemoveMembers)...)
}

// Compile-time proof valkeyutil.Client supplies the primitives the middlewares expect.
var (
	_ incrExClient   = (valkeyutil.Client)(nil)
	_ sentinelClient = (valkeyutil.Client)(nil)
)
