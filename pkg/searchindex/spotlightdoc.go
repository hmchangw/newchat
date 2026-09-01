package searchindex

import "time"

// SpotlightDoc is the Elasticsearch document shape for the spotlight
// (name-typeahead) index. Shared by search-sync-worker and
// data-migration/es-index-migrator.
type SpotlightDoc struct {
	UserAccount string    `json:"userAccount" es:"keyword"`
	RoomID      string    `json:"roomId"      es:"keyword"`
	RoomName    string    `json:"roomName"    es:"search_as_you_type,custom_analyzer"`
	RoomType    string    `json:"roomType"    es:"keyword"`
	SiteID      string    `json:"siteId"      es:"keyword"`
	JoinedAt    time.Time `json:"joinedAt"    es:"date"`
	// RoomNameUpdatedAt is the event-millis under which RoomName is valid — the
	// LWW clock a room_renamed update-by-query guards on so an out-of-order rename
	// can't overwrite a newer name. Stamped = the member/rename event timestamp.
	RoomNameUpdatedAt int64 `json:"roomNameUpdatedAt" es:"long"`
	// Origin marks provenance (e.g. model.OriginTeams for Teams-migrated
	// rooms); empty for natively-created rooms. Filtered by search-service
	// when SHOW_TEAMS_ROOM is false.
	Origin string `json:"origin,omitempty" es:"keyword"`
}

// SpotlightFields is the minimal, source-agnostic input to NewSpotlightDoc.
type SpotlightFields struct {
	UserAccount       string
	RoomID            string
	RoomName          string
	RoomType          string
	SiteID            string
	JoinedAt          time.Time
	RoomNameUpdatedAt int64
	Origin            string
}

// NewSpotlightDoc builds the ES document for the spotlight index from f.
//
//nolint:gocritic // hugeParam: f is passed by value to satisfy the builder interface; struct copy is negligible for 100 bytes
func NewSpotlightDoc(f SpotlightFields) SpotlightDoc {
	return SpotlightDoc(f)
}
