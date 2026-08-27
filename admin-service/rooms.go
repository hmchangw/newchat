package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// roomView is the admin-console projection of a room: identity plus the three
// fields the duty toggle branches on. Restricted carries no omitempty — the
// console renders on its value, so it must survive as false rather than vanish.
type roomView struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Type       model.RoomType `json:"type"`
	UserCount  int            `json:"userCount"`
	Restricted bool           `json:"restricted"`
}

// roomMemberView is one member of a room, projected to what the owner picker
// needs. Sourced from subscriptions, the same collection room-service validates
// a designated owner against.
type roomMemberView struct {
	Account string `json:"account"`
	IsBot   bool   `json:"isBot"`
}

// listRooms handles GET /rooms — the rooms homed at this site, paged.
func (h *Handler) listRooms(c *gin.Context) {
	ctx := c.Request.Context()
	page, limit := parsePaging(c, 1, 20)

	rooms, total, err := h.store.ListRooms(ctx, h.cfg.SiteID, page, limit)
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list rooms: %w", err))
		return
	}

	views := make([]roomView, len(rooms))
	for i := range rooms {
		views[i] = roomView{
			ID:         rooms[i].ID,
			Name:       rooms[i].Name,
			Type:       rooms[i].Type,
			UserCount:  rooms[i].UserCount,
			Restricted: rooms[i].Restricted,
		}
	}
	c.JSON(http.StatusOK, gin.H{"rooms": views, "total": total})
}

// listRoomMembers handles GET /rooms/:roomId/members — every subscribed account,
// unpaged, so the duty dialog can offer an owner the toggle will accept.
func (h *Handler) listRoomMembers(c *gin.Context) {
	ctx := c.Request.Context()

	subs, err := h.store.ListRoomMembers(ctx, c.Param("roomId"))
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list room members: %w", err))
		return
	}

	views := make([]roomMemberView, len(subs))
	for i := range subs {
		views[i] = roomMemberView{Account: subs[i].User.Account, IsBot: subs[i].User.IsBot}
	}
	c.JSON(http.StatusOK, gin.H{"members": views})
}
