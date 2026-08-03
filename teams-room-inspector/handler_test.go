package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/model"
)

// newTestServer wires a Gin engine around a mocked store, matching production
// route registration so path and binding behaviour are exercised too.
func newTestServer(t *testing.T, store RoomStore) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerRoutes(r, NewHandler(store, "site-a"))
	return r
}

func postVerify(t *testing.T, r *gin.Engine, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/internal/teams/rooms/verify", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHandler_HandleVerify_ReportsPerChatState(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)

	const chatFull = "19:full@thread.v2"
	const chatShort = "19:short@thread.v2"
	const chatMissing = "19:missing@unq.gbl.spaces"
	roomFull := idgen.DeterministicID([]byte(chatFull))
	roomShort := idgen.DeterministicID([]byte(chatShort))
	roomMissing := idgen.DeterministicID([]byte(chatMissing))

	store.EXPECT().RoomStates(gomock.Any(), []string{roomFull, roomShort, roomMissing}).
		Return(map[string]RoomState{
			roomFull:  {Exists: true, UserCount: 4, SubscriptionCount: 4},
			roomShort: {Exists: true, UserCount: 3, SubscriptionCount: 2},
		}, nil)

	w := postVerify(t, newTestServer(t, store),
		`{"chatIds":["`+chatFull+`","`+chatShort+`","`+chatMissing+`"]}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got model.TeamsRoomVerifyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "site-a", got.SiteID)
	assert.Equal(t, 3, got.RequestedCount)
	assert.Equal(t, 2, got.FoundCount)
	require.Len(t, got.Chats, 3)

	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatFull, RoomID: roomFull, RoomExists: true, SubscriptionCount: 4, RoomUserCount: 4,
	}, got.Chats[0], "results must come back in request order")
	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatShort, RoomID: roomShort, RoomExists: true, SubscriptionCount: 2, RoomUserCount: 3,
	}, got.Chats[1])
	assert.Equal(t, model.TeamsRoomVerifyResult{
		ChatID: chatMissing, RoomID: roomMissing,
	}, got.Chats[2], "an id the store never saw reports as a missing room")
}

func TestHandler_HandleVerify_RoomWithNoSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	const chatID = "19:empty@thread.v2"
	roomID := idgen.DeterministicID([]byte(chatID))

	store.EXPECT().RoomStates(gomock.Any(), []string{roomID}).
		Return(map[string]RoomState{roomID: {Exists: true}}, nil)

	w := postVerify(t, newTestServer(t, store), `{"chatIds":["`+chatID+`"]}`)
	require.Equal(t, http.StatusOK, w.Code)

	var got model.TeamsRoomVerifyResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, 1, got.FoundCount)
	assert.True(t, got.Chats[0].RoomExists)
	assert.Zero(t, got.Chats[0].SubscriptionCount)
}

func TestHandler_HandleVerify_InvalidInput(t *testing.T) {
	tooMany := make([]string, maxChatIDsPerRequest+1)
	for i := range tooMany {
		tooMany[i] = "19:chat@thread.v2"
	}
	tooManyJSON, err := json.Marshal(model.TeamsRoomVerifyRequest{ChatIDs: tooMany})
	require.NoError(t, err)

	// A body over verifyRequestBodyMaxBytes must 400 during the read, not after
	// a full unbounded decode — the endpoint is unauthenticated and
	// cluster-internal, so this is the only guard against an oversized body.
	oversizedChatID := `"` + strings.Repeat("x", verifyRequestBodyMaxBytes) + `"`
	oversizedJSON := `{"chatIds":[` + oversizedChatID + `]}`

	tests := []struct {
		name string
		body string
	}{
		{"malformed json", `{"chatIds":`},
		{"wrong type", `{"chatIds":"not-an-array"}`},
		{"empty array", `{"chatIds":[]}`},
		{"missing field", `{}`},
		{"over the limit", string(tooManyJSON)},
		{"body too large", oversizedJSON},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := NewMockRoomStore(ctrl) // no EXPECT: the store must not be touched
			w := postVerify(t, newTestServer(t, store), tt.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestHandler_HandleVerify_StoreErrorIsInternal(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockRoomStore(ctrl)
	store.EXPECT().RoomStates(gomock.Any(), gomock.Any()).Return(nil, errors.New("mongo down"))

	w := postVerify(t, newTestServer(t, store), `{"chatIds":["19:abc@thread.v2"]}`)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.NotContains(t, w.Body.String(), "mongo down", "internal cause must never reach the client")
}

func TestHandler_HandleHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	r := newTestServer(t, NewMockRoomStore(ctrl))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
