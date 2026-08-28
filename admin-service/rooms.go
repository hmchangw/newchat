package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode/errhttp"
	"github.com/hmchangw/chat/pkg/model"
)

// roomView is the admin-console projection of a room. OnDuty is the inverse of
// the mapping setRoomOnDuty writes (one boolean onto both flags), kept in this
// package so the two directions cannot drift; the raw flags ride along so a
// half-set room stays diagnosable. None carries omitempty: the console renders
// on their values, so they must survive as false rather than vanish.
type roomView struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Type           model.RoomType `json:"type"`
	UserCount      int            `json:"userCount"`
	Restricted     bool           `json:"restricted"`
	ExternalAccess bool           `json:"externalAccess"`
	OnDuty         bool           `json:"onDuty"`
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
			ID:             rooms[i].ID,
			Name:           rooms[i].Name,
			Type:           rooms[i].Type,
			UserCount:      rooms[i].UserCount,
			Restricted:     rooms[i].Restricted,
			ExternalAccess: rooms[i].ExternalAccess,
			OnDuty:         rooms[i].Restricted && rooms[i].ExternalAccess,
		}
	}
	c.JSON(http.StatusOK, gin.H{"rooms": views, "total": total})
}

// listRoomMembers handles GET /rooms/:roomId/members — every subscribed account,
// unpaged, so the duty dialog can offer an owner the toggle will accept.
func (h *Handler) listRoomMembers(c *gin.Context) {
	ctx := c.Request.Context()

	members, err := h.store.ListRoomMembers(ctx, c.Param("roomId"))
	if err != nil {
		errhttp.Write(ctx, c, fmt.Errorf("list room members: %w", err))
		return
	}

	views := make([]roomMemberView, len(members))
	for i := range members {
		views[i] = roomMemberView{Account: members[i].Account, IsBot: members[i].IsBot}
	}
	c.JSON(http.StatusOK, gin.H{"members": views})
}
