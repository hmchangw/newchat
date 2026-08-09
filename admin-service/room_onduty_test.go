package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errnats"
	"github.com/hmchangw/chat/pkg/ginutil"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/subject"
)

// fakeRoomRPC records what the handler sent, so the toggle runs without NATS.
type fakeRoomRPC struct {
	calls      int
	gotSubject string
	gotData    []byte
	gotHeader  nats.Header
	gotTimeout time.Duration
	reply      *nats.Msg
	err        error
}

func (f *fakeRoomRPC) RequestMsg(_ context.Context, msg *nats.Msg, timeout time.Duration) (*nats.Msg, error) {
	f.calls++
	f.gotSubject = msg.Subject
	f.gotData = msg.Data
	f.gotHeader = msg.Header
	f.gotTimeout = timeout
	if f.err != nil {
		return nil, f.err
	}
	return f.reply, nil
}

// decodeSent unmarshals the payload the handler put on the wire.
func (f *fakeRoomRPC) decodeSent(t *testing.T) model.RoomRestrictedRequest {
	t.Helper()
	var sent model.RoomRestrictedRequest
	require.NoError(t, json.Unmarshal(f.gotData, &sent))
	return sent
}

// okReply is the success envelope room-service replies with.
func okReply(t *testing.T) *nats.Msg {
	t.Helper()
	b, err := json.Marshal(model.StatusWithRequestReply{Status: "ok", RequestID: "req-1"})
	require.NoError(t, err)
	return &nats.Msg{Data: b}
}

// errReply builds room-service's envelope for a typed error.
func errReply(err error) *nats.Msg {
	return &nats.Msg{Data: errnats.MarshalQuiet(err)}
}

func onDutyTestCfg() Config {
	cfg := testCfg()
	cfg.RoomRPCTimeout = 3 * time.Second
	return cfg
}

// setupOnDutyRouter wires just the duty route with a fixed admin principal.
func setupOnDutyRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{
			ID:      "sess-1",
			UserID:  "admin-user-id",
			Account: "p_admin",
			SiteID:  "site-A",
			Roles:   []string{"admin"},
		})
		c.Next()
	})
	r.POST("/rooms/:roomId/onduty", h.setRoomOnDuty)
	return r
}

// doOnDuty sends a raw body so malformed JSON can be tested.
func doOnDuty(h *Handler, roomID, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rooms/"+roomID+"/onduty", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setupOnDutyRouter(h).ServeHTTP(w, req)
	return w
}

// Turning on designates an owner; turning off sends none, so roles survive.
func TestSetRoomOnDuty_MapsOnDutyToBothFlags(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		want      bool
		wantOwner string
	}{
		{name: "on", body: `{"onDuty":true,"ownerAccount":"alice"}`, want: true, wantOwner: "alice"},
		{name: "off", body: `{"onDuty":false}`, want: false, wantOwner: ""},
		{name: "off ignores a supplied owner", body: `{"onDuty":false,"ownerAccount":"alice"}`, want: false, wantOwner: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			rpc := &fakeRoomRPC{reply: okReply(t)}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", tc.body)

			require.Equal(t, http.StatusOK, w.Code)
			require.Equal(t, 1, rpc.calls)
			assert.Equal(t, subject.RoomRestricted("site-A"), rpc.gotSubject)
			assert.Equal(t, 3*time.Second, rpc.gotTimeout)

			sent := rpc.decodeSent(t)
			assert.Equal(t, "r1", sent.RoomID)
			assert.Equal(t, tc.want, sent.Restricted)
			assert.Equal(t, tc.want, sent.ExternalAccess)
			assert.Equal(t, tc.wantOwner, sent.OwnerAccount)
			assert.Equal(t, "p_admin", sent.Account)
		})
	}
}

// Turning duty on without a usable owner is rejected before the round trip.
func TestSetRoomOnDuty_OwnerRequiredWhenTurningOn(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "absent", body: `{"onDuty":true}`},
		{name: "empty", body: `{"onDuty":true,"ownerAccount":""}`},
		{name: "whitespace only", body: `{"onDuty":true,"ownerAccount":"   "}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			rpc := &fakeRoomRPC{reply: okReply(t)}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", tc.body)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Zero(t, rpc.calls, "no RPC may be issued without an owner")
		})
	}
}

// A padded account is trimmed before it goes on the wire, so room-service is not
// asked to look up a member that cannot exist.
func TestSetRoomOnDuty_TrimsOwnerAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	rpc := &fakeRoomRPC{reply: okReply(t)}
	h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

	w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"  alice  "}`)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "alice", rpc.decodeSent(t).OwnerAccount)
}

// A body that cannot yield a boolean must not reach the RPC.
func TestSetRoomOnDuty_InvalidBody(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{"onDuty":`},
		{name: "missing onDuty", body: `{}`},
		{name: "wrong type", body: `{"onDuty":"yes"}`},
		{name: "empty body", body: ``},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			rpc := &fakeRoomRPC{reply: okReply(t)}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", tc.body)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Zero(t, rpc.calls, "no RPC may be issued for an unusable body")
		})
	}
}

// room-service's typed errors keep their status through the HTTP boundary.
func TestSetRoomOnDuty_DownstreamErrorsPassThrough(t *testing.T) {
	tests := []struct {
		name       string
		remote     error
		wantStatus int
		wantReason string
	}{
		{
			name:       "not a platform admin",
			remote:     errcode.Forbidden("only admins can change room restricted state"),
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "room not found",
			remote:     errcode.NotFound("room not found"),
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "not enough members",
			remote:     errcode.Conflict("not enough members to restrict (need at least 5)"),
			wantStatus: http.StatusConflict,
		},
		{
			name:       "not a channel room",
			remote:     errcode.BadRequest("restricted change is only allowed in channel rooms", errcode.WithReason(errcode.RoomNonChannelOperation)),
			wantStatus: http.StatusBadRequest,
			wantReason: string(errcode.RoomNonChannelOperation),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			rpc := &fakeRoomRPC{reply: errReply(tc.remote)}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

			require.Equal(t, tc.wantStatus, w.Code)
			body := respBody(t, w)
			assert.Equal(t, tc.remote.Error(), body["error"])
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, body["reason"])
			}
		})
	}
}

// Ambiguous and retryable: the write may have landed. 503, not 500.
func TestSetRoomOnDuty_TransportErrorIsUnavailable(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "timeout", err: fmt.Errorf("nats: %w", nats.ErrTimeout)},
		{name: "deadline exceeded", err: fmt.Errorf("rpc: %w", context.DeadlineExceeded)},
		{name: "no responders", err: fmt.Errorf("nats: %w", nats.ErrNoResponders)},
		{name: "client hung up", err: fmt.Errorf("rpc: %w", context.Canceled)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)

			rpc := &fakeRoomRPC{err: tc.err}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			assert.NotContains(t, w.Body.String(), "nats", "infra cause must not reach the client")
		})
	}
}

// Any other transport fault is internal, with the cause not leaked.
func TestSetRoomOnDuty_OtherTransportErrorIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	rpc := &fakeRoomRPC{err: fmt.Errorf("connection refused")}
	h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

	w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

	require.Equal(t, http.StatusInternalServerError, w.Code)
	assert.NotContains(t, w.Body.String(), "connection refused")
}

// Neither an error envelope nor a success payload means the toggle is unproven.
func TestSetRoomOnDuty_UnrecognizedReplyIsInternal(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: []byte{}},
		{name: "truncated", data: []byte(`{"stat`)},
		{name: "foreign shape", data: []byte(`{"x":1}`)},
		{name: "wrong status", data: []byte(`{"status":"accepted"}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl) // strict mock: proves the handler does no store I/O

			rpc := &fakeRoomRPC{reply: &nats.Msg{Data: tc.data}}
			h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

			w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

			assert.Equal(t, http.StatusInternalServerError, w.Code)
		})
	}
}

// A Handler built without a room client answers 503 rather than panicking.
func TestSetRoomOnDuty_NilClientIsUnavailable(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	h := newHandler(m, emptySessionStore(), onDutyTestCfg(), nil)

	w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// Without this hop the two services' logs cannot be correlated.
func TestSetRoomOnDuty_PropagatesRequestID(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	rpc := &fakeRoomRPC{reply: okReply(t)}
	h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

	const reqID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	r := gin.New()
	r.Use(ginutil.RequestID())
	r.Use(func(c *gin.Context) {
		c.Set(ctxPrincipal, session.Session{ID: "sess-1", UserID: "u", Account: "p_admin", SiteID: "site-A", Roles: []string{"admin"}})
		c.Next()
	})
	r.POST("/rooms/:roomId/onduty", h.setRoomOnDuty)

	req := httptest.NewRequest(http.MethodPost, "/rooms/r1/onduty", strings.NewReader(`{"onDuty":true,"ownerAccount":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(natsutil.RequestIDHeader, reqID)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, rpc.gotHeader, "request must carry a header")
	assert.Equal(t, reqID, rpc.gotHeader.Get(natsutil.RequestIDHeader))
}

// A foreign envelope carrying a code outside the closed set must not be
// re-emitted verbatim — errcode.Parse does not validate it.
func TestSetRoomOnDuty_UnknownRemoteCodeIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockAdminStore(ctrl)

	rpc := &fakeRoomRPC{reply: &nats.Msg{Data: []byte(`{"code":"teapot","error":"i am a teapot"}`)}}
	h := newHandler(m, emptySessionStore(), onDutyTestCfg(), rpc)

	w := doOnDuty(h, "r1", `{"onDuty":true,"ownerAccount":"alice"}`)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
