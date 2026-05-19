package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

type SearchStore interface {
	Search(ctx context.Context, indices []string, body json.RawMessage) (json.RawMessage, error)
	GetUserRoomDoc(ctx context.Context, account string) (UserRoomDoc, bool, error)
}

// RestrictedRoomCache stores only the restricted-rooms map (rid → HSS
// millis). The unrestricted rooms[] array is always resolved via ES
// terms-lookup at query time, so no local copy is needed.
type RestrictedRoomCache interface {
	GetRestricted(ctx context.Context, account string) (map[string]int64, bool, error)
	SetRestricted(ctx context.Context, account string, rooms map[string]int64, ttl time.Duration) error
}

// UserRoomDoc mirrors the subset of the user-room ES doc that
// search-service reads. Fields must stay in sync with the upsert shape
// in search-sync-worker/user_room.go userRoomUpsertDoc.
type UserRoomDoc struct {
	UserAccount     string           `json:"userAccount"`
	Rooms           []string         `json:"rooms"`
	RestrictedRooms map[string]int64 `json:"restrictedRooms"`
}

// MongoStore is the Mongo-backed store interface for search-service.
type MongoStore interface {
	SearchAppsByName(
		ctx context.Context,
		query, account string,
		assistantEnabled *bool,
		offset, limit int,
	) ([]model.App, error)
}

// SearchUsersClient is the outbound HTTP interface for user search.
// It wraps the third-party HR endpoint; the handler tests inject a fake
// implementation so no real HTTP call is needed in unit tests.
type SearchUsersClient interface {
	SearchUsers(ctx context.Context, query string) ([]model.SearchUser, error)
}
