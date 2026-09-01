package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// setupRoomsRouter wires just the room-listing routes.
func setupRoomsRouter(h *Handler) *gin.Engine {
	r := gin.New()
	r.GET("/rooms", h.listRooms)
	r.GET("/rooms/:roomId/members", h.listRoomMembers)
	return r
}

func doRoomsGet(h *Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	setupRoomsRouter(h).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

// firstRoom unwraps the single room a one-row listing response carries.
func firstRoom(t *testing.T, body map[string]any) map[string]any {
	t.Helper()
	rooms, ok := body["rooms"].([]any)
	require.True(t, ok)
	require.Len(t, rooms, 1)
	room, ok := rooms[0].(map[string]any)
	require.True(t, ok)
	return room
}

// -------------------------------------------------------------------------
// listRooms tests
// -------------------------------------------------------------------------

func TestHandler_listRooms(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name:  "lists the configured site's rooms with paging",
			query: "?page=2&limit=10",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 2, 10).
					Return([]model.Room{{
						ID: "r1", Name: "general", Type: model.RoomTypeChannel,
						UserCount: 7, Restricted: true, ExternalAccess: true,
					}}, int64(1), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(1), body["total"])
				room := firstRoom(t, body)
				assert.Equal(t, "r1", room["id"])
				assert.Equal(t, "general", room["name"])
				assert.Equal(t, "channel", room["type"])
				assert.Equal(t, float64(7), room["userCount"])
				assert.Equal(t, true, room["restricted"])
				assert.Equal(t, true, room["externalAccess"])
				assert.Equal(t, true, room["onDuty"])
			},
		},
		{
			name:  "defaults page=1 limit=20",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 1, 20).
					Return([]model.Room{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				assert.Equal(t, float64(0), body["total"])
				rooms, ok := body["rooms"].([]any)
				require.True(t, ok)
				assert.Empty(t, rooms)
			},
		},
		{
			name:  "limit is clamped to maxPageLimit",
			query: "?limit=100000",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 1, 100).
					Return([]model.Room{}, int64(0), nil)
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "an unrestricted room reports both duty flags as false rather than omitting them",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 1, 20).
					Return([]model.Room{{
						ID: "r2", Name: "dm", Type: model.RoomTypeDM, UserCount: 2,
					}}, int64(1), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				room := firstRoom(t, body)
				// The console branches on these, so they must always be present.
				assert.Contains(t, room, "restricted")
				assert.Equal(t, false, room["restricted"])
				assert.Contains(t, room, "externalAccess")
				assert.Equal(t, false, room["externalAccess"])
				assert.Contains(t, room, "onDuty")
				assert.Equal(t, false, room["onDuty"])
			},
		},
		{
			name:  "reports a restricted room without external access as exactly that",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 1, 20).
					Return([]model.Room{{
						ID: "r3", Name: "half", Type: model.RoomTypeChannel,
						UserCount: 9, Restricted: true,
					}}, int64(1), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				room := firstRoom(t, body)
				assert.Equal(t, true, room["restricted"])
				assert.Equal(t, false, room["externalAccess"])
				// Half-set is not on duty — this is the case a copy-pasted
				// derivation from either flag alone would get wrong.
				assert.Equal(t, false, room["onDuty"])
			},
		},
		{
			name:  "reports external access without the restriction as not on duty",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), "site-A", 1, 20).
					Return([]model.Room{{
						ID: "r4", Name: "other-half", Type: model.RoomTypeChannel,
						UserCount: 9, ExternalAccess: true,
					}}, int64(1), nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				room := firstRoom(t, body)
				assert.Equal(t, false, room["restricted"])
				assert.Equal(t, true, room["externalAccess"])
				assert.Equal(t, false, room["onDuty"])
			},
		},
		{
			name:  "store error → 500",
			query: "",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRooms(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
					Return(nil, int64(0), fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			w := doRoomsGet(newHandler(m, emptySessionStore(), testCfg(), nil, nil), "/rooms"+tc.query)

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.checkBody != nil {
				tc.checkBody(t, respBody(t, w))
			}
		})
	}
}

// -------------------------------------------------------------------------
// listRoomMembers tests
// -------------------------------------------------------------------------

func TestHandler_listRoomMembers(t *testing.T) {
	tests := []struct {
		name       string
		roomID     string
		setupMock  func(m *MockAdminStore)
		wantStatus int
		checkBody  func(t *testing.T, body map[string]any)
	}{
		{
			name:   "returns the room's subscribed accounts",
			roomID: "r1",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRoomMembers(gomock.Any(), "r1").
					Return([]model.SubscriptionUser{
						{Account: "alice"},
						{Account: "helperbot", IsBot: true},
					}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				members, ok := body["members"].([]any)
				require.True(t, ok)
				require.Len(t, members, 2)
				first, ok := members[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "alice", first["account"])
				assert.Equal(t, false, first["isBot"])
				second, ok := members[1].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "helperbot", second["account"])
				assert.Equal(t, true, second["isBot"])
			},
		},
		{
			name:   "a room with no subscriptions returns an empty list, not null",
			roomID: "r-empty",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRoomMembers(gomock.Any(), "r-empty").
					Return([]model.SubscriptionUser{}, nil)
			},
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body map[string]any) {
				members, ok := body["members"].([]any)
				require.True(t, ok, "members must marshal as an array")
				assert.Empty(t, members)
			},
		},
		{
			name:   "store error → 500",
			roomID: "r1",
			setupMock: func(m *MockAdminStore) {
				m.EXPECT().ListRoomMembers(gomock.Any(), gomock.Any()).
					Return(nil, fmt.Errorf("db offline"))
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := NewMockAdminStore(ctrl)
			tc.setupMock(m)

			h := newHandler(m, emptySessionStore(), testCfg(), nil, nil)
			w := doRoomsGet(h, "/rooms/"+tc.roomID+"/members")

			assert.Equal(t, tc.wantStatus, w.Code)
			if tc.checkBody != nil {
				tc.checkBody(t, respBody(t, w))
			}
		})
	}
}
