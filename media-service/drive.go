package main

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/model"
)

// driveMember is one entry in the drive.members response.
type driveMember struct {
	ID       string `json:"_id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Active   bool   `json:"active"`
}

type driveMembersData struct {
	Members  []driveMember  `json:"members"`
	Count    int            `json:"count"`
	RoomName string         `json:"roomName"`
	RoomType model.RoomType `json:"roomType"`
}

type driveMembersResponse struct {
	Success bool             `json:"success"`
	Data    driveMembersData `json:"data"`
}

type driveErrorResponse struct {
	Success   bool   `json:"success"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
}

const (
	errTypeMissingParameter = "MISSING_PARAMETER"
	errTypeRoomNotFound     = "ROOM_NOT_FOUND"
	errTypeAccountNotFound  = "ACCOUNT_NOT_FOUND"
	errTypeInternal         = "INTERNAL_ERROR"
)

// writeDriveError writes the bespoke {success:false, error, errorType} envelope.
func writeDriveError(c *gin.Context, status int, errorType, msg string) {
	c.JSON(status, driveErrorResponse{Success: false, Error: msg, ErrorType: errorType})
}

// HandleDriveMembers reports whether accountName is a member of roomId, along
// with the room's name and type. It is a single-account probe: members is
// either empty (not a member) or the one probed account (member), with count
// hardcoded to 0 or 1.
func (h *handler) HandleDriveMembers(c *gin.Context) {
	ctx := c.Request.Context()
	roomID := c.Query("roomId")
	account := c.Query("accountName")

	if roomID == "" {
		writeDriveError(c, http.StatusBadRequest, errTypeMissingParameter, "roomId is required")
		return
	}
	if account == "" {
		writeDriveError(c, http.StatusBadRequest, errTypeMissingParameter, "accountName is required")
		return
	}

	_, roomType, roomName, found, err := h.store.RoomSite(ctx, roomID)
	if err != nil {
		slog.ErrorContext(ctx, "drive.members: room lookup failed", "error", err, "roomId", roomID)
		writeDriveError(c, http.StatusInternalServerError, errTypeInternal, "internal error")
		return
	}
	if !found {
		writeDriveError(c, http.StatusNotFound, errTypeRoomNotFound, "room not found")
		return
	}

	user, found, err := h.store.UserByAccount(ctx, account)
	if err != nil {
		slog.ErrorContext(ctx, "drive.members: user lookup failed", "error", err, "account", account)
		writeDriveError(c, http.StatusInternalServerError, errTypeInternal, "internal error")
		return
	}
	if !found {
		writeDriveError(c, http.StatusNotFound, errTypeAccountNotFound, "account not found")
		return
	}

	member, err := h.store.RoomMember(ctx, roomID, account)
	if err != nil {
		slog.ErrorContext(ctx, "drive.members: membership lookup failed", "error", err, "roomId", roomID, "account", account)
		writeDriveError(c, http.StatusInternalServerError, errTypeInternal, "internal error")
		return
	}

	resp := driveMembersResponse{
		Success: true,
		Data: driveMembersData{
			Members:  []driveMember{},
			Count:    0,
			RoomName: roomName,
			RoomType: roomType,
		},
	}
	if member {
		resp.Data.Members = []driveMember{{
			ID:       user.ID,
			Username: user.Account,
			Name:     user.DisplayName(),
			Active:   !user.Deactivated,
		}}
		resp.Data.Count = 1
	}
	c.JSON(http.StatusOK, resp)
}
