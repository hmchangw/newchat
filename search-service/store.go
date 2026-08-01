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

// SubscriptionMeta is the caller's subscription projection used for enrichment:
// the room type, the join-key Name (DM counterpart account / botDM bot
// account), and the botDM IsSubscribed flag.
type SubscriptionMeta struct {
	RoomType     model.RoomType
	Name         string
	IsSubscribed bool
}

// HRUser is the users-collection projection used to render display / HR names.
type HRUser struct {
	Account     string
	EngName     string
	ChineseName string
}

// MongoStore is the Mongo-backed store interface for search-service.
type MongoStore interface {
	SearchAppsByName(
		ctx context.Context,
		query, account string,
		assistantEnabled *bool,
		offset, limit int,
	) ([]model.App, error)

	// SubscriptionsByRoomIDs returns the caller's subscription meta for the given
	// rooms, keyed by roomID. Rooms with no subscription are omitted.
	SubscriptionsByRoomIDs(ctx context.Context, account string, roomIDs []string) (map[string]SubscriptionMeta, error)

	// UsersByAccounts returns HR/display projections keyed by account. Missing accounts are omitted.
	UsersByAccounts(ctx context.Context, accounts []string) (map[string]HRUser, error)

	// AppsByAssistantNames returns apps keyed by their assistant.name (bot account). Missing are omitted.
	AppsByAssistantNames(ctx context.Context, botAccounts []string) (map[string]model.App, error)
}

// RoomInfoClient fetches canonical room metadata from room-service on a given
// site (the RoomsInfoBatch server↔server RPC), used only for channel/discussion
// room names (cross-site safe: caller groups by the hit's siteID).
type RoomInfoClient interface {
	GetRoomsInfo(ctx context.Context, siteID string, roomIDs []string) ([]model.RoomInfo, error)
}

// SearchUsersClient is the outbound HTTP interface for user search.
// It wraps the third-party HR endpoint; the handler tests inject a fake
// implementation so no real HTTP call is needed in unit tests.
type SearchUsersClient interface {
	SearchUsers(ctx context.Context, query string, offset, limit int) ([]model.SearchUser, error)
}
