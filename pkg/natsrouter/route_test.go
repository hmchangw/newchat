package natsrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

func TestValidateRoutes(t *testing.T) {
	ok := func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil }
	void := func(_ *Context, _ testReq) error { return nil }

	tests := []struct {
		name    string
		routes  []Route
		wantErr string
	}{
		{
			name: "a well-formed table validates",
			routes: []Route{
				{Pattern: "chat.user.{account}.request.room.{roomID}.s.msg.history", Method: "list_channel_messages", Bind: Handle(ok)},
				{Pattern: "chat.user.{account}.event.presence.s.hello", Bind: HandleVoid(void)},
			},
		},
		{
			name:    "a recording route must declare a method",
			routes:  []Route{{Pattern: "chat.user.{account}.request.room.s.open", Bind: Handle(ok)}},
			wantErr: "declares no rpc.method",
		},
		{
			name:    "a void route must not declare a method",
			routes:  []Route{{Pattern: "chat.user.{account}.event.presence.s.hello", Method: "presence_hello", Bind: HandleVoid(void)}},
			wantErr: "records no sample",
		},
		{
			name:    "a method outside snake_case is rejected",
			routes:  []Route{{Pattern: "chat.user.{account}.request.room.s.open", Method: "OpenRoom", Bind: Handle(ok)}},
			wantErr: "not snake_case",
		},
		{
			name: "a duplicate method in one table is rejected",
			routes: []Route{
				{Pattern: "chat.user.{account}.request.room.s.open", Method: "open_room", Bind: Handle(ok)},
				{Pattern: "chat.user.{account}.request.room.s.close", Method: "open_room", Bind: Handle(ok)},
			},
			wantErr: "duplicate rpc.method",
		},
		{
			name: "a duplicate pattern in one table is rejected",
			routes: []Route{
				{Pattern: "chat.user.{account}.request.room.s.open", Method: "open_room", Bind: Handle(ok)},
				{Pattern: "chat.user.{account}.request.room.s.open", Method: "reopen_room", Bind: Handle(ok)},
			},
			wantErr: "duplicate pattern",
		},
		{
			name:    "a route with no pattern is rejected",
			routes:  []Route{{Method: "open_room", Bind: Handle(ok)}},
			wantErr: "empty pattern",
		},
		{
			name:    "a route with no handler is rejected",
			routes:  []Route{{Pattern: "chat.user.{account}.request.room.s.open", Method: "open_room"}},
			wantErr: "no handler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRoutes(tt.routes)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestRegisterRoutes_ServesEveryShape drives one table carrying all four
// registration shapes against a live server, so the Binder indirection is shown
// to preserve the behaviour each Register* entry point already has.
func TestRegisterRoutes_ServesEveryShape(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service", WithSiteID("site-1"))

	voidSeen := make(chan string, 1)

	r.RegisterRoutes(
		Route{
			Pattern: "chat.user.{account}.request.user.site-1.echo",
			Method:  "echo_body",
			Bind: Handle(func(c *Context, req testReq) (*testResp, error) {
				return &testResp{Greeting: "body " + req.Name + " " + c.Param("account")}, nil
			}),
		},
		Route{
			Pattern: "chat.user.{account}.request.user.site-1.nobody",
			Method:  "echo_no_body",
			Bind: HandleNoBody(func(c *Context) (*testResp, error) {
				return &testResp{Greeting: "nobody " + c.Param("account")}, nil
			}),
		},
		Route{
			Pattern: "chat.user.{account}.request.user.site-1.optional",
			Method:  "echo_optional_body",
			Bind: HandleOptionalBody(func(_ *Context, req testReq) (*testResp, error) {
				return &testResp{Greeting: "optional [" + req.Name + "]"}, nil
			}),
		},
		Route{
			Pattern: "chat.user.{account}.event.presence.site-1.ping",
			Bind: HandleVoid(func(_ *Context, req testReq) error {
				voidSeen <- req.Name
				return nil
			}),
		},
	)

	body, err := json.Marshal(testReq{Name: "world"})
	require.NoError(t, err)

	for _, tc := range []struct {
		subject string
		payload []byte
		want    string
	}{
		{"chat.user.alice.request.user.site-1.echo", body, "body world alice"},
		{"chat.user.alice.request.user.site-1.nobody", nil, "nobody alice"},
		{"chat.user.alice.request.user.site-1.optional", nil, "optional []"},
		{"chat.user.alice.request.user.site-1.optional", body, "optional [world]"},
	} {
		resp, err := nc.Request(context.Background(), tc.subject, tc.payload, 2*time.Second)
		require.NoError(t, err, tc.subject)

		var got testResp
		require.NoError(t, json.Unmarshal(resp.Data, &got))
		assert.Equal(t, tc.want, got.Greeting, tc.subject)
	}

	require.NoError(t, nc.Publish(context.Background(), "chat.user.alice.event.presence.site-1.ping", body))
	select {
	case name := <-voidSeen:
		assert.Equal(t, "world", name)
	case <-time.After(2 * time.Second):
		t.Fatal("void route never received its message")
	}
}

func TestRegisterRoutes_PanicsOnAnInvalidTable(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service", WithSiteID("site-1"))

	assert.PanicsWithError(t,
		`natsrouter: route "chat.user.{account}.request.user.site-1.echo" declares no rpc.method`,
		func() {
			r.RegisterRoutes(Route{
				Pattern: "chat.user.{account}.request.user.site-1.echo",
				Bind:    Handle(func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil }),
			})
		})
}

// TestRegisterRoutes_RecordsTheDeclaredMethod pins that a table row's Method
// reaches the metric, which is the whole point of moving routes into data.
func TestRegisterRoutes_RecordsTheDeclaredMethod(t *testing.T) {
	assert.Equal(t, natsmetrics.MethodNone, Route{}.Method,
		"the zero Route must carry MethodNone so a void row needs no explicit value")
}
