package natsrouter_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// Example_basicUsage demonstrates registering a handler with params.
func Example_basicUsage() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "my-service")

	// Register a handler — {account} and {roomID} are extracted from the subject.
	// The pattern is automatically converted to a NATS wildcard for subscription.
	// The third argument is the route's rpc.method: a declared natsmetrics
	// constant, unique within this router, that every metric for this route is
	// labelled with. An undeclared method is not rejected: registration logs
	// and degrades that route to natsmetrics.MethodOther ("_OTHER"). The gates
	// that actually catch a wrong method run before deploy — .semgrep/
	// rpcmethod.yml and the service's own testdata/routes.golden test.
	natsrouter.Register[RenameRoomRequest, Room](
		router,
		"chat.user.{account}.request.room.{roomID}.site-a.rename",
		natsmetrics.MethodRenameRoom,
		func(c *natsrouter.Context, req RenameRoomRequest) (*Room, error) {
			return &Room{ID: c.Param("roomID"), Name: req.Name}, nil
		},
	)
}

// Example_withMiddleware demonstrates the recommended baseline stack
// via Default(), then opts into a per-handler timeout. Default
// pre-installs Recovery, RequestID, and Logging — mirroring
// gin.Default()'s shape. Add HandlerTimeout (or any other middleware)
// via r.Use after Default returns.
func Example_withMiddleware() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.Default(nc, "my-service")
	router.Use(natsrouter.HandlerTimeout(5 * time.Second))

	natsrouter.RegisterNoBody[Settings](
		router,
		"chat.user.{account}.request.settings.get",
		natsmetrics.MethodGetSettings,
		func(c *natsrouter.Context) (*Settings, error) {
			return &Settings{Account: c.Param("account"), Locale: "zh-TW"}, nil
		},
	)
}

type Room struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Request and response types are named for the operation their route's
// rpc.method names. A rename route taking a greeting payload would be exactly
// the method-does-not-match-the-handler mismatch this vocabulary exists to
// prevent, demonstrated in the package's own godoc.
type RenameRoomRequest struct {
	Name string `json:"name"`
}

type OpenRoomRequest struct {
	IncludeMembers bool `json:"includeMembers"`
}

type Settings struct {
	Account string `json:"account"`
	Locale  string `json:"locale"`
}

type SetSettingsRequest struct {
	Locale string `json:"locale"`
}

// Example_noBodyHandler demonstrates RegisterNoBody for GET-style endpoints.
func Example_noBodyHandler() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "room-service")

	// No request body needed — the roomID comes from the subject.
	natsrouter.RegisterNoBody[Room](
		router,
		"chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(c *natsrouter.Context) (*Room, error) {
			roomID := c.Param("roomID")
			return &Room{ID: roomID, Name: "General"}, nil
		},
	)
}

// Example_errorHandling demonstrates user-facing vs internal errors.
func Example_errorHandling() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "room-service")

	natsrouter.Register(
		router,
		"chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(c *natsrouter.Context, req OpenRoomRequest) (*Room, error) {
			room := findRoom(c.Param("roomID"))
			if room == nil {
				// User-facing error — client receives: {"code":"not_found","error":"room not found"}
				return nil, errcode.NotFound("room not found")
			}
			return room, nil
			// If findRoom returned a Go error (e.g. DB failure), return it as-is:
			//   return nil, fmt.Errorf("db lookup: %w", err)
			// Client would receive: {"error":"internal error"} (sanitized)
		},
	)
}

func findRoom(_ string) *Room { return nil }

type TypingEvent struct {
	RoomID string `json:"roomId"`
}

// Example_fireAndForget demonstrates RegisterVoid for events with no response.
func Example_fireAndForget() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "chat-service")

	// No response sent — the sender publishes and moves on.
	natsrouter.RegisterVoid(
		router,
		"chat.user.{account}.event.typing",
		func(c *natsrouter.Context, req TypingEvent) error {
			fmt.Printf("user %s is typing in room %s\n", c.Param("account"), req.RoomID)
			return nil
		},
	)
}

// Example_customMiddleware demonstrates writing custom middleware.
func Example_customMiddleware() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "my-service")

	// Custom middleware that rejects requests with empty payloads. Middleware
	// can't return an error like a handler, so it replies with a typed errcode
	// envelope directly through the router context.
	requireBody := natsrouter.HandlerFunc(func(c *natsrouter.Context) {
		if len(c.Msg.Data) == 0 {
			c.ReplyError(errcode.BadRequest("request body required"))
			return
		}
		c.Next()
	})

	router.Use(natsrouter.Recovery())
	router.Use(requireBody)

	natsrouter.Register[SetSettingsRequest, Settings](
		router,
		"chat.user.{account}.request.settings.set",
		natsmetrics.MethodSetSettings,
		func(c *natsrouter.Context, req SetSettingsRequest) (*Settings, error) {
			return &Settings{Account: c.Param("account"), Locale: req.Locale}, nil
		},
	)
}
