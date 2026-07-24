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
}

// SpotlightFields is the minimal, source-agnostic input to NewSpotlightDoc.
type SpotlightFields struct {
	UserAccount string
	RoomID      string
	RoomName    string
	RoomType    string
	SiteID      string
	JoinedAt    time.Time
}

// NewSpotlightDoc builds the ES document for the spotlight index from f.
func NewSpotlightDoc(f SpotlightFields) SpotlightDoc {
	return SpotlightDoc{
		UserAccount: f.UserAccount,
		RoomID:      f.RoomID,
		RoomName:    f.RoomName,
		RoomType:    f.RoomType,
		SiteID:      f.SiteID,
		JoinedAt:    f.JoinedAt,
	}
}
