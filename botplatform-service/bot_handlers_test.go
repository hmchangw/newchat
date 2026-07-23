package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/session"
)

// mountBotHandler mounts one handler with a pre-populated bot principal, bypassing requireBot.
func mountBotHandler(t *testing.T, method, path string, handlerFn gin.HandlerFunc) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxBotPrincipal, &session.Session{
			UserID:  "bot-user-id",
			Account: "myapp.bot",
			SiteID:  "site-a",
			Roles:   []string{"bot"},
		})
		c.Next()
	})
	r.Handle(method, path, handlerFn)
	return r
}

func doJSON(t *testing.T, r http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func doRaw(t *testing.T, r http.Handler, method, path, raw string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestBotSendRoomMessage_Validation(t *testing.T) {
	h := &handler{}
	r := mountBotHandler(t, http.MethodPost, "/api/v1/rooms/:roomID/messages", h.botSendRoomMessage)

	t.Run("empty content rejected as content_invalid", func(t *testing.T) {
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms/r1/messages",
			map[string]any{"content": ""})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "content_invalid", readReason(t, w))
	})

	t.Run("over-limit content rejected as content_invalid", func(t *testing.T) {
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms/r1/messages",
			map[string]any{"content": strings.Repeat("a", 20*1024+1)})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "content_invalid", readReason(t, w))
	})

	t.Run("attachments (unknown field) rejected as unknown_field", func(t *testing.T) {
		w := doRaw(t, r, http.MethodPost, "/api/v1/rooms/r1/messages",
			`{"content":"hi","attachments":[{"foo":"bar"}]}`)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "unknown_field", readReason(t, w))
	})

	t.Run("malformed json rejected as content_invalid", func(t *testing.T) {
		w := doRaw(t, r, http.MethodPost, "/api/v1/rooms/r1/messages", `{not-json`)
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "content_invalid", readReason(t, w))
	})
}

func TestBotCreateRoom_Validation(t *testing.T) {
	h := &handler{}
	r := mountBotHandler(t, http.MethodPost, "/api/v1/rooms", h.botCreateRoom)

	t.Run("missing name rejected as content_invalid", func(t *testing.T) {
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms",
			map[string]any{"topic": "no-name"})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "content_invalid", readReason(t, w))
	})

	t.Run("too many members rejected as batch_too_large", func(t *testing.T) {
		members := make([]string, 101)
		for i := range members {
			members[i] = "u"
		}
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms",
			map[string]any{"name": "big", "members": members})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "batch_too_large", readReason(t, w))
	})

	t.Run("too many orgs rejected as batch_too_large", func(t *testing.T) {
		orgs := make([]string, 6)
		for i := range orgs {
			orgs[i] = "o"
		}
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms",
			map[string]any{"name": "big", "orgs": orgs})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "batch_too_large", readReason(t, w))
	})
}

func TestBotAddMembers_Validation(t *testing.T) {
	h := &handler{}
	r := mountBotHandler(t, http.MethodPost, "/api/v1/rooms/:roomID/members/add", h.botAddMembers)

	t.Run("empty add batch rejected as content_invalid", func(t *testing.T) {
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms/r1/members/add",
			map[string]any{})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "content_invalid", readReason(t, w))
	})

	t.Run("too many userIds rejected as batch_too_large", func(t *testing.T) {
		u := make([]string, 101)
		for i := range u {
			u[i] = "u"
		}
		w := doJSON(t, r, http.MethodPost, "/api/v1/rooms/r1/members/add",
			map[string]any{"userIds": u})
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Equal(t, "batch_too_large", readReason(t, w))
	})
}

// readReason extracts the errcode wire envelope's reason field.
func readReason(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	s, _ := body["reason"].(string)
	return s
}
