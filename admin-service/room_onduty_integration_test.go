//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/sessiontoken"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// onDutyRig is the full stack on a real NATS, with a stub room-service responder.
type onDutyRig struct {
	router     *gin.Engine
	authToken  string
	received   chan model.RoomRestrictedRequest
	gotHeaders chan string
}

// newOnDutyRig wires Mongo, an admin session, NATS, and a stub responder.
func newOnDutyRig(t *testing.T, siteID string, reply func(model.RoomRestrictedRequest) []byte) *onDutyRig {
	t.Helper()
	ctx := context.Background()

	db := testutil.MongoDBReplicaSet(t, "adminonduty")
	sessions := session.NewMongoStore(db)
	require.NoError(t, sessions.EnsureIndexes(ctx))
	store := newStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	const authToken = "onduty-test-token"
	seedSession(t, db, session.Session{
		ID:       sessiontoken.Hash(authToken),
		UserID:   "u-admin",
		Account:  "p_admin",
		SiteID:   siteID,
		Roles:    []string{string(model.UserRoleAdmin)},
		IssuedAt: 1,
	})

	url := testutil.NATS(t)
	gotHeaders := make(chan string, 16)

	serverConn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverConn.Drain() })

	// Buffered and written non-blocking so a failed subtest cannot wedge the
	// responder, and with it the next subtest, on a full channel.
	received := make(chan model.RoomRestrictedRequest, 16)
	sub, err := serverConn.Subscribe(subject.RoomRestricted(siteID), func(m *nats.Msg) {
		var req model.RoomRestrictedRequest
		if err := json.Unmarshal(m.Data, &req); err != nil {
			_ = m.Respond(errnats.MarshalQuiet(errcode.BadRequest("unmarshal")))
			return
		}
		select {
		case gotHeaders <- m.Header.Get(natsutil.RequestIDHeader):
		default:
		}
		select {
		case received <- req:
		default:
		}
		_ = m.Respond(reply(req))
	})
	require.NoError(t, err)
	require.NoError(t, serverConn.Flush())
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	clientConn, err := natsutil.Connect(ctx, url, "", noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.NatsConn().Drain() })

	cfg := Config{SiteID: siteID, BcryptCost: 4, SessionsMaxPerAccount: 100, RoomRPCTimeout: 5 * time.Second}
	h := newHandler(store, sessions, cfg, clientConn, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Mirrors main.go: without it the X-Request-ID never reaches the NATS message.
	r.Use(ginutil.RequestID())
	registerRoutes(r, h, sessions, cfg.SiteID)

	return &onDutyRig{router: r, authToken: authToken, received: received, gotHeaders: gotHeaders}
}

// drain clears both channels so a subtest never reads a message left behind by a
// sibling that failed before consuming its own.
func (rig *onDutyRig) drain() {
	for len(rig.received) > 0 {
		<-rig.received
	}
	for len(rig.gotHeaders) > 0 {
		<-rig.gotHeaders
	}
}

func (rig *onDutyRig) post(t *testing.T, roomID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/rooms/"+roomID+"/onduty", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rig.authToken)
	req.Header.Set(natsutil.RequestIDHeader, "01970a4f-8c2d-7c9a-abcd-e0123456789f")
	w := httptest.NewRecorder()
	rig.router.ServeHTTP(w, req)
	return w
}

func okEnvelope(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(model.StatusWithRequestReply{Status: "ok", RequestID: "req-1"})
	require.NoError(t, err)
	return b
}

// Route, admin middleware and the NATS round-trip must all line up.
func TestIntegration_SetRoomOnDuty_EndToEnd(t *testing.T) {
	rig := newOnDutyRig(t, "site-a", func(model.RoomRestrictedRequest) []byte { return okEnvelope(t) })

	t.Run("on sets both flags", func(t *testing.T) {
		rig.drain()
		w := rig.post(t, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)
		require.Equal(t, http.StatusOK, w.Code)

		sent := <-rig.received
		assert.Equal(t, "r1", sent.RoomID)
		assert.True(t, sent.Restricted)
		assert.True(t, sent.ExternalAccess)
		assert.Equal(t, "alice", sent.OwnerAccount, "turning on designates the room owner")
		assert.Equal(t, "p_admin", sent.Account)
	})

	t.Run("off clears both flags", func(t *testing.T) {
		rig.drain()
		w := rig.post(t, "r1", `{"onDuty":false}`)
		require.Equal(t, http.StatusOK, w.Code)

		sent := <-rig.received
		assert.False(t, sent.Restricted)
		assert.False(t, sent.ExternalAccess)
		assert.Empty(t, sent.OwnerAccount, "turning off must not reshuffle roles")
	})

	t.Run("correlation id reaches room-service", func(t *testing.T) {
		rig.drain()
		w := rig.post(t, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)
		require.Equal(t, http.StatusOK, w.Code)
		<-rig.received

		assert.Equal(t, "01970a4f-8c2d-7c9a-abcd-e0123456789f", <-rig.gotHeaders,
			"X-Request-ID must survive the HTTP→NATS hop")
	})
}

// A typed error from room-service keeps its status across the real transport.
func TestIntegration_SetRoomOnDuty_RemoteErrorStatus(t *testing.T) {
	rig := newOnDutyRig(t, "site-b", func(model.RoomRestrictedRequest) []byte {
		return errnats.MarshalQuiet(errcode.Conflict("not enough members to restrict (need at least 5)"))
	})

	w := rig.post(t, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)
	assert.Equal(t, http.StatusConflict, w.Code)
	assert.Contains(t, w.Body.String(), "not enough members")
}

// A responder that receives and never answers, so the RPC genuinely runs out of
// RoomRPCTimeout rather than short-circuiting on ErrNoResponders.
func TestIntegration_SetRoomOnDuty_RPCTimeout(t *testing.T) {
	ctx := context.Background()

	db := testutil.MongoDBReplicaSet(t, "ondutytimeout")
	sessions := session.NewMongoStore(db)
	require.NoError(t, sessions.EnsureIndexes(ctx))
	store := newStoreMongo(db)
	require.NoError(t, store.EnsureIndexes(ctx))

	const authToken = "onduty-timeout-token"
	seedSession(t, db, session.Session{
		ID: sessiontoken.Hash(authToken), UserID: "u-admin", Account: "p_admin",
		SiteID: "site-c", Roles: []string{string(model.UserRoleAdmin)}, IssuedAt: 1,
	})

	url := testutil.NATS(t)

	// Subscribe without responding: NATS sees a responder, so the client waits out
	// the deadline instead of failing fast with ErrNoResponders.
	deafConn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = deafConn.Drain() })
	sub, err := deafConn.Subscribe(subject.RoomRestricted("site-c"), func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, deafConn.Flush())
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	clientConn, err := natsutil.Connect(ctx, url, "", noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.NatsConn().Drain() })

	const rpcTimeout = 300 * time.Millisecond
	cfg := Config{SiteID: "site-c", BcryptCost: 4, SessionsMaxPerAccount: 100, RoomRPCTimeout: rpcTimeout}
	h := newHandler(store, sessions, cfg, clientConn, nil)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, h, sessions, cfg.SiteID)

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/rooms/r1/onduty", strings.NewReader(`{"onDuty":true,"ownerAccount":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+authToken)
	w := httptest.NewRecorder()

	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)

	// 503, not 500: the write may have landed, so the console should retry.
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.NotContains(t, w.Body.String(), "nats", "transport cause must not reach the client")
	// Bracketed both ways: at least the deadline proves it really waited (not a
	// fast ErrNoResponders), and well under it proves RoomRPCTimeout is honoured.
	assert.GreaterOrEqual(t, elapsed, rpcTimeout, "must wait out RoomRPCTimeout, not fail fast")
	assert.Less(t, elapsed, 5*time.Second, "must give up on RoomRPCTimeout, not a default")
}

// Without a valid admin session the request never reaches NATS.
func TestIntegration_SetRoomOnDuty_RequiresAdminSession(t *testing.T) {
	rig := newOnDutyRig(t, "site-d", func(model.RoomRestrictedRequest) []byte { return okEnvelope(t) })

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/rooms/r1/onduty", strings.NewReader(`{"onDuty":true,"ownerAccount":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rig.router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.Empty(t, rig.received, "unauthenticated request must not issue an RPC")
}
