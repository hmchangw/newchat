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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// wireCaptureClient records SetNX / IncrEx / Del so tests assert the chain ran end-to-end.
type wireCaptureClient struct {
	setNX int32
	incr  int32
	del   int32
}

func (w *wireCaptureClient) Get(context.Context, string) (string, error) { return "", nil }
func (w *wireCaptureClient) Set(context.Context, string, string, time.Duration) error {
	return nil
}
func (w *wireCaptureClient) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	atomic.AddInt32(&w.setNX, 1)
	return true, nil
}
func (w *wireCaptureClient) IncrEx(context.Context, string, time.Duration) (int64, error) {
	atomic.AddInt32(&w.incr, 1)
	return 1, nil
}
func (w *wireCaptureClient) Del(context.Context, ...string) error {
	atomic.AddInt32(&w.del, 1)
	return nil
}
func (w *wireCaptureClient) Close() error { return nil }

var _ valkeyutil.Client = (*wireCaptureClient)(nil)

// successForwarder returns empty 2xx replies so idempotency Del fires.
type successForwarder struct{}

func (successForwarder) sendRoom(context.Context, *session.Session, string, string, []byte) (*model.Message, error) {
	return &model.Message{}, nil
}
func (successForwarder) sendDM(context.Context, *session.Session, string, string, []byte) (*model.Message, error) {
	return &model.Message{}, nil
}
func (successForwarder) createRoom(context.Context, *session.Session, string, []byte) ([]byte, error) {
	return []byte(`{}`), nil
}
func (successForwarder) addMembers(context.Context, *session.Session, string, string, []byte) ([]byte, error) {
	return []byte(`{}`), nil
}
func (successForwarder) removeMembers(context.Context, *session.Session, string, string, []byte) ([]byte, error) {
	return []byte(`{}`), nil
}

// alwaysLocalSub returns a subscription pointing at BP's own site so route-
// wiring tests exercise the normal (same-site) path.
func alwaysLocalSub(_ context.Context, _, _ string) (*BotSub, error) {
	return &BotSub{RoomID: "r1", SiteID: "site-a"}, nil
}

type fakeDMEnsurer struct{}

func (*fakeDMEnsurer) Ensure(_ context.Context, _ *session.Session, _ string) (string, error) {
	return "dm-room-id", nil
}

func TestRegisterBotRoutes_ChainWiring(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const rawToken = "wire-test-token"
	botSess := &session.Session{
		ID:      sessiontoken.Hash(rawToken),
		UserID:  "bot-user-id",
		Account: "myapp.bot",
		SiteID:  "site-a",
		Roles:   []string{"bot"},
	}
	sessions := &fakeSessionStore{
		FindByHashFn: func(_ context.Context, hash string) (*session.Session, error) {
			require.Equal(t, sessiontoken.Hash(rawToken), hash)
			return botSess, nil
		},
	}

	cfg := &config{
		SiteID:                      "site-a",
		BotRateLimitPerCallerPerMin: 100,
		BotRateLimitGlobalPerMin:    1000,
		BotIdempotencyMsgTTL:        30 * time.Second,
		BotIdempotencyRoomMgmtTTL:   60 * time.Second,
	}
	h := &handler{
		cfg:       cfg,
		forwarder: successForwarder{},
		subs:      &fakeSubStore{FindForBotFn: alwaysLocalSub, FindDMForBotFn: alwaysLocalSub},
		dmEnsurer: &fakeDMEnsurer{},
	}

	routes := []struct {
		name string
		path string
		body []byte
	}{
		{"send-in-room", "/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)},
		{"send-DM", "/api/v1/dms/u1/messages", []byte(`{"content":"hi"}`)},
		{"create-room", "/api/v1/rooms", []byte(`{"name":"deployments"}`)},
		{"add-members", "/api/v1/rooms/r1/members/add", []byte(`{"userIds":["u1"]}`)},
		{"remove-members", "/api/v1/rooms/r1/members/remove", []byte(`{"userIds":["u1"]}`)},
	}

	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			valkey := &wireCaptureClient{}
			r := gin.New()
			registerBotRoutes(r, sessions, valkey, cfg, h)

			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-user-id", "bot-user-id")
			req.Header.Set("x-auth-token", rawToken)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, int32(2), atomic.LoadInt32(&valkey.incr), "rate-limit IncrEx must run per-caller + global")
			assert.Equal(t, int32(1), atomic.LoadInt32(&valkey.setNX), "idempotency SetNX must run")
			assert.Equal(t, int32(1), atomic.LoadInt32(&valkey.del), "idempotency Del must run on 2xx")
		})
	}
}

func TestRegisterBotRoutes_NoValkey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const rawToken = "no-valkey-token"
	botSess := &session.Session{
		ID:      sessiontoken.Hash(rawToken),
		UserID:  "bot-user-id",
		Account: "myapp.bot",
		SiteID:  "site-a",
		Roles:   []string{"bot"},
	}
	sessions := &fakeSessionStore{
		FindByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
			return botSess, nil
		},
	}
	cfg := &config{
		SiteID:                    "site-a",
		BotIdempotencyMsgTTL:      30 * time.Second,
		BotIdempotencyRoomMgmtTTL: 60 * time.Second,
	}
	h := &handler{
		cfg:       cfg,
		forwarder: successForwarder{},
		subs:      &fakeSubStore{FindForBotFn: alwaysLocalSub, FindDMForBotFn: alwaysLocalSub},
		dmEnsurer: &fakeDMEnsurer{},
	}

	r := gin.New()
	registerBotRoutes(r, sessions, nil, cfg, h)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/r1/messages",
		bytes.NewReader([]byte(`{"content":"hi"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-user-id", "bot-user-id")
	req.Header.Set("x-auth-token", rawToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, "auth still runs; forwarder returns the canonical Message")
}

func TestRegisterBotRoutes_AuthRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sessions := &fakeSessionStore{
		FindByHashFn: func(_ context.Context, _ string) (*session.Session, error) {
			return nil, session.ErrNotFound
		},
	}
	r := gin.New()
	registerBotRoutes(r, sessions, nil, &config{SiteID: "site-a"}, &handler{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/rooms/r1/messages",
		bytes.NewReader([]byte(`{"content":"hi"}`)))
	req.Header.Set("x-user-id", "unknown")
	req.Header.Set("x-auth-token", "unknown")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
