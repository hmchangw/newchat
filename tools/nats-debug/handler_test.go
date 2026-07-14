package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	errNoResponders = nats.ErrNoResponders
	errTimeout      = nats.ErrTimeout
)

// newTestHandler builds a handler whose every session resolves to the same hub,
// so existing single-hub tests behave as before.
func newTestHandler(hub Hub) *handler {
	return newHandler(newSessionManager(func() Hub { return hub }, time.Hour))
}

func TestHandler_SetsSessionCookie(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockHub(ctrl)
	m.EXPECT().Status().Return(ConnectionStatus{})

	h := newTestHandler(m)
	w := httptest.NewRecorder()
	h.status(w, httptest.NewRequest(http.MethodGet, "/api/status", nil))

	var cookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}
	require.NotNil(t, cookie, "expected a session cookie on the response")
	assert.True(t, cookie.HttpOnly)
}

func TestHandler_IsolatesSessions(t *testing.T) {
	ctrl := gomock.NewController(t)
	hubA := NewMockHub(ctrl)
	hubB := NewMockHub(ctrl)
	hubA.EXPECT().Status().Return(ConnectionStatus{SourceURL: "nats://a:4222"})
	hubB.EXPECT().Status().Return(ConnectionStatus{SourceURL: "nats://b:4222"})

	factory, calls := hubFactory(hubA, hubB)
	h := newHandler(newSessionManager(factory, time.Hour))

	// Two browsers with no cookie each get their own hub.
	w1 := httptest.NewRecorder()
	h.status(w1, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	w2 := httptest.NewRecorder()
	h.status(w2, httptest.NewRequest(http.MethodGet, "/api/status", nil))

	var got1, got2 ConnectionStatus
	require.NoError(t, json.NewDecoder(w1.Body).Decode(&got1))
	require.NoError(t, json.NewDecoder(w2.Body).Decode(&got2))

	assert.Equal(t, "nats://a:4222", got1.SourceURL)
	assert.Equal(t, "nats://b:4222", got2.SourceURL)
	assert.Equal(t, 2, *calls)
}

func TestHandler_Connect(t *testing.T) {
	tests := []struct {
		name       string
		body       any
		setupMock  func(*MockHub)
		wantStatus int
	}{
		{
			name: "successful connect",
			body: map[string]string{"sourceURL": "nats://src:4222", "destURL": "nats://dst:4222"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Connect("nats://src:4222", "nats://dst:4222").Return(nil)
				m.EXPECT().Status().Return(ConnectionStatus{
					SourceConnected: true, DestConnected: true,
					SourceURL: "nats://src:4222", DestURL: "nats://dst:4222",
				})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing sourceURL",
			body:       map[string]string{"sourceURL": "", "destURL": "nats://dst:4222"},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing destURL",
			body:       map[string]string{"sourceURL": "nats://src:4222", "destURL": ""},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON body",
			body:       "not-json",
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "connection error",
			body: map[string]string{"sourceURL": "nats://src:4222", "destURL": "nats://dst:4222"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Connect("nats://src:4222", "nats://dst:4222").Return(errors.New("refused"))
			},
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)

			h := newTestHandler(m)

			var body bytes.Buffer
			if s, ok := tc.body.(string); ok {
				body.WriteString(s)
			} else {
				require.NoError(t, json.NewEncoder(&body).Encode(tc.body))
			}

			req := httptest.NewRequest(http.MethodPost, "/api/connect", &body)
			w := httptest.NewRecorder()
			h.connect(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestHandler_Disconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockHub(ctrl)
	m.EXPECT().Disconnect()

	h := newTestHandler(m)
	req := httptest.NewRequest(http.MethodPost, "/api/disconnect", nil)
	w := httptest.NewRecorder()
	h.disconnect(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandler_Status(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockHub(ctrl)
	m.EXPECT().Status().Return(ConnectionStatus{
		SourceConnected: true, DestConnected: false,
		SourceURL: "nats://src:4222",
	})

	h := newTestHandler(m)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	h.status(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var got ConnectionStatus
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.True(t, got.SourceConnected)
	assert.False(t, got.DestConnected)
}

func TestHandler_Subscribe(t *testing.T) {
	tests := []struct {
		name       string
		body       any
		setupMock  func(*MockHub)
		wantStatus int
		wantSub    *Subscription
	}{
		{
			name: "successful subscribe",
			body: map[string]string{"subject": "chat.>"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Subscribe("chat.>").Return(Subscription{ID: "sub-1", Subject: "chat.>"}, nil)
			},
			wantStatus: http.StatusCreated,
			wantSub:    &Subscription{ID: "sub-1", Subject: "chat.>"},
		},
		{
			name:       "missing subject",
			body:       map[string]string{"subject": ""},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON",
			body:       "bad",
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "hub subscribe error",
			body: map[string]string{"subject": "chat.>"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Subscribe("chat.>").Return(Subscription{}, errors.New("not connected"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)

			h := newTestHandler(m)

			var body bytes.Buffer
			if s, ok := tc.body.(string); ok {
				body.WriteString(s)
			} else {
				require.NoError(t, json.NewEncoder(&body).Encode(tc.body))
			}

			req := httptest.NewRequest(http.MethodPost, "/api/subscriptions", &body)
			w := httptest.NewRecorder()
			h.subscribe(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantSub != nil {
				var got Subscription
				require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
				assert.Equal(t, *tc.wantSub, got)
			}
		})
	}
}

func TestHandler_Unsubscribe(t *testing.T) {
	tests := []struct {
		name       string
		id         string
		setupMock  func(*MockHub)
		wantStatus int
	}{
		{
			name: "successful unsubscribe",
			id:   "sub-1",
			setupMock: func(m *MockHub) {
				m.EXPECT().Unsubscribe("sub-1").Return(nil)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "not found",
			id:   "sub-99",
			setupMock: func(m *MockHub) {
				m.EXPECT().Unsubscribe("sub-99").Return(errors.New("not found"))
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)

			h := newTestHandler(m)

			mux := http.NewServeMux()
			h.registerRoutes(mux)

			req := httptest.NewRequest(http.MethodDelete, "/api/subscriptions/"+tc.id, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestHandler_ListSubscriptions(t *testing.T) {
	tests := []struct {
		name       string
		setupMock  func(*MockHub)
		wantCount  int
		wantStatus int
	}{
		{
			name: "returns subscriptions",
			setupMock: func(m *MockHub) {
				m.EXPECT().Subscriptions().Return([]Subscription{
					{ID: "s1", Subject: "chat.>"},
					{ID: "s2", Subject: "fanout.>"},
				})
			},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
		{
			name: "empty list",
			setupMock: func(m *MockHub) {
				m.EXPECT().Subscriptions().Return(nil)
			},
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)

			h := newTestHandler(m)
			req := httptest.NewRequest(http.MethodGet, "/api/subscriptions", nil)
			w := httptest.NewRecorder()
			h.listSubscriptions(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			var got []Subscription
			require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
			assert.Len(t, got, tc.wantCount)
		})
	}
}

func TestHandler_Publish(t *testing.T) {
	tests := []struct {
		name       string
		body       any
		setupMock  func(*MockHub)
		wantStatus int
	}{
		{
			name: "successful publish without debug",
			body: map[string]any{"subject": "chat.room.123", "payload": `{"msg":"hello"}`},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", `{"msg":"hello"}`, DebugHeaders{}).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "publish with debug level trace",
			body: map[string]any{"subject": "chat.room.123", "payload": "{}", "debug": "trace"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", "{}", DebugHeaders{Level: "trace"}).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "publish with debug payload",
			body: map[string]any{"subject": "chat.room.123", "payload": "{}", "debug": "flow", "debugPayload": true},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", "{}", DebugHeaders{Level: "flow", Payload: true}).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "publish with truthy debug normalizes to debug",
			body: map[string]any{"subject": "chat.room.123", "payload": "{}", "debug": "1"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", "{}", DebugHeaders{Level: "debug"}).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "publish with garbage debug emits no header",
			body: map[string]any{"subject": "chat.room.123", "payload": "{}", "debug": "wat"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", "{}", DebugHeaders{}).Return(nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing subject",
			body:       map[string]any{"subject": "", "payload": "{}"},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON",
			body:       "bad",
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "publish error",
			body: map[string]any{"subject": "chat.room.123", "payload": "{}"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Publish("chat.room.123", "{}", DebugHeaders{}).Return(errors.New("not connected"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)

			h := newTestHandler(m)

			var body bytes.Buffer
			if s, ok := tc.body.(string); ok {
				body.WriteString(s)
			} else {
				require.NoError(t, json.NewEncoder(&body).Encode(tc.body))
			}

			req := httptest.NewRequest(http.MethodPost, "/api/publish", &body)
			w := httptest.NewRecorder()
			h.publish(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestHandler_Events(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockHub(ctrl)

	// chReady signals that capturedCh has been set by the mock DoAndReturn.
	chReady := make(chan chan<- Message, 1)

	m.EXPECT().RegisterSSEClient(gomock.Any()).DoAndReturn(func(ch chan<- Message) string {
		chReady <- ch
		return "client-1"
	})
	m.EXPECT().UnregisterSSEClient("client-1")

	h := newTestHandler(m)

	w := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}

	req, cancel := newCancelableRequest(t, "/api/events")
	defer cancel()

	go func() {
		// Wait for the SSE handler to call RegisterSSEClient before sending.
		capturedCh := <-chReady
		capturedCh <- Message{ID: "m1", Subject: "chat.>", Payload: "{}", Timestamp: time.Now()}
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	h.events(w, req)

	body := w.Body.String()
	assert.Contains(t, body, "event: connected")
	assert.Contains(t, body, "event: message")
	assert.Contains(t, body, `"subject":"chat.>"`)
}

// flushRecorder wraps httptest.ResponseRecorder and implements http.Flusher.
type flushRecorder struct {
	*httptest.ResponseRecorder
}

func (f *flushRecorder) Flush() {}

func newCancelableRequest(t *testing.T, path string) (*http.Request, context.CancelFunc) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	ctx, cancel := context.WithCancel(req.Context())
	return req.WithContext(ctx), cancel
}

func TestHandler_RequestConnect(t *testing.T) {
	tests := []struct {
		name       string
		body       any
		setupMock  func(*MockHub)
		wantStatus int
	}{
		{
			name: "successful connect",
			body: map[string]string{"url": "nats://req:4222"},
			setupMock: func(m *MockHub) {
				m.EXPECT().ConnectRequest("nats://req:4222").Return(nil)
				m.EXPECT().Status().Return(ConnectionStatus{RequestConnected: true, RequestURL: "nats://req:4222"})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing url",
			body:       map[string]string{"url": ""},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON body",
			body:       "not-json",
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "connection error",
			body: map[string]string{"url": "nats://req:4222"},
			setupMock: func(m *MockHub) {
				m.EXPECT().ConnectRequest("nats://req:4222").Return(errors.New("refused"))
			},
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)
			h := newTestHandler(m)

			var body bytes.Buffer
			if s, ok := tc.body.(string); ok {
				body.WriteString(s)
			} else {
				require.NoError(t, json.NewEncoder(&body).Encode(tc.body))
			}

			req := httptest.NewRequest(http.MethodPost, "/api/request/connect", &body)
			w := httptest.NewRecorder()
			h.requestConnect(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
		})
	}
}

func TestHandler_RequestDisconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := NewMockHub(ctrl)
	m.EXPECT().DisconnectRequest()

	h := newTestHandler(m)
	req := httptest.NewRequest(http.MethodPost, "/api/request/disconnect", nil)
	w := httptest.NewRecorder()
	h.requestDisconnect(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandler_Request(t *testing.T) {
	tests := []struct {
		name        string
		body        any
		setupMock   func(*MockHub)
		wantStatus  int
		wantPayload string
	}{
		{
			name: "successful request without debug",
			body: map[string]any{"subject": "chat.validate", "payload": `{"msg":"hi"}`, "timeoutMs": 3000},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", `{"msg":"hi"}`, 3000, DebugHeaders{}).Return(`{"ok":true}`, nil)
			},
			wantStatus:  http.StatusOK,
			wantPayload: `{"ok":true}`,
		},
		{
			name: "request with debug level and payload",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 3000, "debug": "debug", "debugPayload": true},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 3000, DebugHeaders{Level: "debug", Payload: true}).Return(`{"ok":true}`, nil)
			},
			wantStatus:  http.StatusOK,
			wantPayload: `{"ok":true}`,
		},
		{
			name: "request with garbage debug emits no header",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 3000, "debug": "off"},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 3000, DebugHeaders{}).Return(`{"ok":true}`, nil)
			},
			wantStatus:  http.StatusOK,
			wantPayload: `{"ok":true}`,
		},
		{
			name:       "missing subject",
			body:       map[string]any{"subject": "", "payload": "{}", "timeoutMs": 1000},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "timeout zero",
			body:       map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 0},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "timeout negative",
			body:       map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": -1},
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid JSON body",
			body:       "bad",
			setupMock:  func(m *MockHub) {},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not connected",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 1000},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 1000, DebugHeaders{}).Return("", errors.New("not connected to request NATS"))
			},
			wantStatus: http.StatusConflict,
		},
		{
			name: "no responders",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 1000},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 1000, DebugHeaders{}).Return("", errNoResponders)
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "timeout error",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 100},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 100, DebugHeaders{}).Return("", errTimeout)
			},
			wantStatus: http.StatusRequestTimeout,
		},
		{
			name: "generic hub error",
			body: map[string]any{"subject": "chat.validate", "payload": "{}", "timeoutMs": 1000},
			setupMock: func(m *MockHub) {
				m.EXPECT().Request("chat.validate", "{}", 1000, DebugHeaders{}).Return("", errors.New("something broke"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockHub(ctrl)
			tc.setupMock(m)
			h := newTestHandler(m)

			var body bytes.Buffer
			if s, ok := tc.body.(string); ok {
				body.WriteString(s)
			} else {
				require.NoError(t, json.NewEncoder(&body).Encode(tc.body))
			}

			req := httptest.NewRequest(http.MethodPost, "/api/request", &body)
			w := httptest.NewRecorder()
			h.request(w, req)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.wantPayload != "" {
				var got map[string]string
				require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
				assert.Equal(t, tc.wantPayload, got["payload"])
			}
		})
	}
}
