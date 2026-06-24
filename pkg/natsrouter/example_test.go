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
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

type GreetRequest struct {
	Message string `json:"message"`
}

type GreetResponse struct {
	Reply string `json:"reply"`
}

// Example_basicUsage demonstrates registering a handler with params.
func Example_basicUsage() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "my-service")

	// Register a handler — {account} and {roomID} are extracted from the subject.
	// The pattern is automatically converted to a NATS wildcard for subscription.
	natsrouter.Register[GreetRequest, GreetResponse](
		router,
		"chat.user.{account}.room.{roomID}.greet",
		func(c *natsrouter.Context, req GreetRequest) (*GreetResponse, error) {
			account := c.Param("account")
			roomID := c.Param("roomID")
			reply := fmt.Sprintf("%s says %s in room %s", account, req.Message, roomID)
			return &GreetResponse{Reply: reply}, nil
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

	natsrouter.Register(
		router,
		"chat.user.{account}.greet",
		func(c *natsrouter.Context, req GreetRequest) (*GreetResponse, error) {
			return &GreetResponse{Reply: "hello " + c.Param("account")}, nil
		},
	)
}

type Room struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Example_noBodyHandler demonstrates RegisterNoBody for GET-style endpoints.
func Example_noBodyHandler() {
	nc, _ := o11ynats.Connect(context.Background(), nats.DefaultURL, noop.NewTracerProvider(), propagation.TraceContext{})
	router := natsrouter.New(nc, "room-service")

	// No request body needed — the roomID comes from the subject.
	natsrouter.RegisterNoBody[Room](
		router,
		"chat.user.{account}.request.rooms.get.{roomID}",
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
		"chat.user.{account}.request.rooms.get.{roomID}",
		func(c *natsrouter.Context, req GreetRequest) (*Room, error) {
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
	// envelope directly via errnats.Reply.
	requireBody := natsrouter.HandlerFunc(func(c *natsrouter.Context) {
		if len(c.Msg.Data) == 0 {
			errnats.Reply(c, c.Msg, errcode.BadRequest("request body required"))
			return
		}
		c.Next()
	})

	router.Use(natsrouter.Recovery())
	router.Use(requireBody)

	natsrouter.Register[GreetRequest, GreetResponse](
		router,
		"chat.user.{account}.greet",
		func(c *natsrouter.Context, req GreetRequest) (*GreetResponse, error) {
			return &GreetResponse{Reply: "hello"}, nil
		},
	)
}
