package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// driveMembersBody mirrors the success envelope for assertions.
type driveMembersBody struct {
	Success bool `json:"success"`
	Data    struct {
		Members []struct {
			ID       string `json:"_id"`
			Username string `json:"username"`
			Name     string `json:"name"`
			Active   bool   `json:"active"`
		} `json:"members"`
		Count    int    `json:"count"`
		RoomName string `json:"roomName"`
		RoomType string `json:"roomType"`
	} `json:"data"`
}

// driveErrorBody mirrors the error envelope for assertions.
type driveErrorBody struct {
	Success   bool   `json:"success"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

func doDriveMembers(t *testing.T, r http.Handler, query string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/drive.members"+query, nil))
	return w
}

func TestHandleDriveMembers_MissingRoomID(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := doDriveMembers(t, r, "?accountName=alice")
	require.Equal(t, http.StatusBadRequest, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.False(t, body.Success)
	assert.Equal(t, "MISSING_PARAMETER", body.ErrorType)
	assert.NotEmpty(t, body.Error)
}

func TestHandleDriveMembers_MissingAccountName(t *testing.T) {
	r, _, _ := newTestRouter(t)
	w := doDriveMembers(t, r, "?roomId=room1")
	require.Equal(t, http.StatusBadRequest, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.False(t, body.Success)
	assert.Equal(t, "MISSING_PARAMETER", body.ErrorType)
}

func TestHandleDriveMembers_RoomNotFound(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("", model.RoomType(""), "", false, nil)
	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusNotFound, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.False(t, body.Success)
	assert.Equal(t, "ROOM_NOT_FOUND", body.ErrorType)
}

func TestHandleDriveMembers_AccountNotFound(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "alice").Return(nil, false, nil)
	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusNotFound, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.False(t, body.Success)
	assert.Equal(t, "ACCOUNT_NOT_FOUND", body.ErrorType)
}

func TestHandleDriveMembers_Member(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "alice").
		Return(&model.User{ID: "u_1", Account: "alice", EngName: "Alice", ChineseName: "Chan"}, true, nil)
	store.EXPECT().RoomMember(gomock.Any(), "room1", "alice").Return(true, nil)

	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusOK, w.Code)
	var body driveMembersBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.True(t, body.Success)
	assert.Equal(t, 1, body.Data.Count)
	assert.Equal(t, "General", body.Data.RoomName)
	assert.Equal(t, "channel", body.Data.RoomType)
	require.Len(t, body.Data.Members, 1)
	assert.Equal(t, "u_1", body.Data.Members[0].ID)
	assert.Equal(t, "alice", body.Data.Members[0].Username)
	assert.Equal(t, "Alice Chan", body.Data.Members[0].Name)
	assert.True(t, body.Data.Members[0].Active)
}

func TestHandleDriveMembers_NonMember(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "bob").
		Return(&model.User{ID: "u_2", Account: "bob"}, true, nil)
	store.EXPECT().RoomMember(gomock.Any(), "room1", "bob").Return(false, nil)

	w := doDriveMembers(t, r, "?roomId=room1&accountName=bob")
	require.Equal(t, http.StatusOK, w.Code)
	// members must serialize as [] not null.
	assert.Contains(t, w.Body.String(), `"members":[]`)
	var body driveMembersBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.True(t, body.Success)
	assert.Equal(t, 0, body.Data.Count)
	assert.Equal(t, "General", body.Data.RoomName)
	assert.Equal(t, "channel", body.Data.RoomType)
	assert.Empty(t, body.Data.Members)
}

func TestHandleDriveMembers_MemberDeactivated_ActiveFalse(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "alice").
		Return(&model.User{ID: "u_1", Account: "alice", Deactivated: true}, true, nil)
	store.EXPECT().RoomMember(gomock.Any(), "room1", "alice").Return(true, nil)

	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusOK, w.Code)
	var body driveMembersBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Data.Members, 1)
	assert.False(t, body.Data.Members[0].Active)
	// name falls back to account when a name field is missing.
	assert.Equal(t, "alice", body.Data.Members[0].Name)
}

func TestHandleDriveMembers_RoomSiteStoreError(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("", model.RoomType(""), "", false, errors.New("mongo down"))
	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusInternalServerError, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.False(t, body.Success)
	assert.Equal(t, "INTERNAL_ERROR", body.ErrorType)
	// internal detail must not leak to the client.
	assert.NotContains(t, w.Body.String(), "mongo down")
}

func TestHandleDriveMembers_UserByAccountStoreError(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "alice").Return(nil, false, errors.New("mongo down"))
	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusInternalServerError, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "INTERNAL_ERROR", body.ErrorType)
}

func TestHandleDriveMembers_RoomMemberStoreError(t *testing.T) {
	r, store, _ := newTestRouter(t)
	store.EXPECT().RoomSite(gomock.Any(), "room1").Return("s1", model.RoomTypeChannel, "General", true, nil)
	store.EXPECT().UserByAccount(gomock.Any(), "alice").
		Return(&model.User{ID: "u_1", Account: "alice"}, true, nil)
	store.EXPECT().RoomMember(gomock.Any(), "room1", "alice").Return(false, errors.New("mongo down"))
	w := doDriveMembers(t, r, "?roomId=room1&accountName=alice")
	require.Equal(t, http.StatusInternalServerError, w.Code)
	var body driveErrorBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "INTERNAL_ERROR", body.ErrorType)
}
