package main

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

// RoomStore is the narrow Mongo surface bot-room-service writes to. Sync req/reply must write directly (no JS-worker materialization).
type RoomStore interface {
	// InsertRoom returns ErrDuplicate when the ID already exists (idempotent retry).
	InsertRoom(ctx context.Context, room *Room) error

	// FindRoom returns ErrNotFound if the room does not live at this site.
	FindRoom(ctx context.Context, roomID string) (*Room, error)

	// UpsertSubscription returns created=true on fresh insert (used to compute newlyAdded diff).
	UpsertSubscription(ctx context.Context, sub *Subscription) (created bool, err error)

	// DeleteSubscription returns deleted=true when a doc was removed (used for the remove diff).
	DeleteSubscription(ctx context.Context, roomID, userID string) (deleted bool, err error)

	// FindUser enriches the owner Participant on create-room.
	FindUser(ctx context.Context, userID string) (*model.User, error)

	// ListRoomMemberAccounts returns the accounts subscribed to roomID. RemoveMember snapshots this
	// AFTER deletes but BEFORE key rotation, so fan-out reaches only survivors.
	ListRoomMemberAccounts(ctx context.Context, roomID string) ([]string, error)
}

var (
	ErrNotFound  = errors.New("bot-room-service: not found")
	ErrDuplicate = errors.New("bot-room-service: duplicate")
)

// RoomKeyStore is the narrow room-key store surface bot-room-service needs: Set on create,
// Get to fan the current key to new members, Rotate/SetWithVersion on remove.
type RoomKeyStore interface {
	Set(ctx context.Context, roomID string, pair roomkeystore.RoomKeyPair) (int, error)

	// Get returns the room's current key pair, or roomkeystore.ErrNoCurrentKey if the room has no key (legacy/broken room).
	Get(ctx context.Context, roomID string) (*roomkeystore.VersionedKeyPair, error)

	// Rotate is the normal remove-member path: swaps in newPair as the room's current key.
	// Returns roomkeystore.ErrNoCurrentKey if the key was concurrently deleted mid-rotation.
	Rotate(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair) (int, error)

	// SetWithVersion is the Rotate-ErrNoCurrentKey fallback: stamps newPair at the caller-supplied version so it matches what was already fanned out to survivors.
	SetWithVersion(ctx context.Context, roomID string, newPair roomkeystore.RoomKeyPair, version int) error
}

// Room is the projection of a rooms doc bot-room-service reads and writes.
type Room struct {
	ID           string
	Type         string
	Name         string
	Topic        string
	SiteID       string
	CreatedAt    time.Time
	Owner        *Participant
	CreatedByBot string
}

// Participant is the shared shape stored on rooms.u + subscriptions.u.
type Participant struct {
	UserID      string
	Account     string
	SiteID      string
	EngName     string
	ChineseName string
	AppID       string
	AppName     string
	IsBot       bool
}

// Subscription is the projection of a subscriptions doc bot-room-service upserts/deletes.
type Subscription struct {
	ID        string
	RoomID    string
	UserID    string
	Account   string
	SiteID    string
	CreatedAt time.Time
	IsBot     bool
}
