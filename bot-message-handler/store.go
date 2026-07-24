package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
)

// Store is the narrow Mongo surface bot-message-handler needs.
type Store interface {
	// FindSubscription returns (nil, ErrNotFound) when the bot is not a member.
	FindSubscription(ctx context.Context, roomID, userID string) (*Subscription, error)

	// FindRoom returns (nil, ErrNotFound) when the room does not live at this site.
	FindRoom(ctx context.Context, roomID string) (*Room, error)

	// ListMemberIDs returns userIDs subscribed to roomID; used to gate mentions.
	ListMemberIDs(ctx context.Context, roomID string) ([]string, error)

	// FindUser returns the users doc used to canonicalize mention Participants.
	FindUser(ctx context.Context, userID string) (*model.User, error)
}

// ErrNotFound is returned by store lookups when the requested document does not exist.
var ErrNotFound = errors.New("store: not found")

type Subscription struct {
	RoomID string
	UserID string
	SiteID string
}

type Room struct {
	ID     string
	Type   string
	Name   string
	SiteID string
}
