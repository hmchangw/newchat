package main

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func registerRoutes(r *gin.Engine, h *handler) {
	r.GET("/healthz", h.HandleHealth)

	r.POST("/api/v1/login", h.HandleLogin)
	r.POST("/api/v1/auth/validate", h.HandleValidate)
}

// registerBotRoutes attaches bot endpoints with the auth → rate-limit → idempotency → handler chain
// (login/validate use a different auth model). Nil valkey (dev) omits rate-limit + idempotency.
func registerBotRoutes(r *gin.Engine, sessions session.Store, valkey valkeyutil.Client, cfg *config, h *handler) {
	auth := requireBot(sessions)

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

	// chain composes auth + rate-limit + idempotency with nils elided.
	chain := func(idem gin.HandlerFunc) []gin.HandlerFunc {
		out := []gin.HandlerFunc{auth}
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
