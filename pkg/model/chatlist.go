package model

// Built-in (product-defined) chatlist section ids. Their membership is derived
// client-side from subscription flags — favorites from subscription.favorite,
// apps from isBot, teams from subscription.sectionId == "teams", chats from
// everything else. There is no server-stored member list for a built-in.
const (
	SectionFavorites = "favorites"
	SectionApps      = "apps"
	SectionTeams     = "teams"
	SectionChats     = "chats"
)

// SortMode values. custom honors each chat's subscription.sectionOrder;
// mostRecent sorts by last message (the default and fallback).
const (
	SortModeCustom     = "custom"
	SortModeMostRecent = "mostRecent"
)

// MaxSectionName bounds a section name in runes.
const MaxSectionName = 50

// IsBuiltinSection reports whether id is a product-defined built-in section.
func IsBuiltinSection(id string) bool {
	switch id {
	case SectionFavorites, SectionApps, SectionTeams, SectionChats:
		return true
	}
	return false
}

// ChatlistSection is one section definition. Membership and per-chat order live
// on the subscriptions (sectionId/sectionOrder), NOT here — the overlay carries
// definitions only, so chatlist.update stays O(sections) not O(chats).
type ChatlistSection struct {
	ID       string `json:"id"       bson:"id"`
	Name     string `json:"name"     bson:"name"`
	BuiltIn  bool   `json:"builtIn"  bson:"builtIn"`
	SortMode string `json:"sortMode" bson:"sortMode"`
}

// ChatlistState is the per-user section-definition overlay on the user record;
// nil means never customized — the client falls back to DefaultChatlistState.
// SectionOrder always covers every section id (built-in + custom).
type ChatlistState struct {
	SectionOrder  []string          `json:"sectionOrder"  bson:"sectionOrder"`
	Sections      []ChatlistSection `json:"sections"      bson:"sections"`
	LastUpdatedAt int64             `json:"lastUpdatedAt" bson:"lastUpdatedAt"`
}

// DefaultChatlistState is the server-owned starting state: the built-in sections
// in display order, no custom sections. All built-ins default to mostRecent.
func DefaultChatlistState() *ChatlistState {
	return &ChatlistState{
		SectionOrder: []string{SectionFavorites, SectionApps, SectionTeams, SectionChats},
		Sections: []ChatlistSection{
			{ID: SectionFavorites, Name: "Favorites", BuiltIn: true, SortMode: SortModeMostRecent},
			{ID: SectionApps, Name: "Apps", BuiltIn: true, SortMode: SortModeMostRecent},
			{ID: SectionTeams, Name: "Teams", BuiltIn: true, SortMode: SortModeMostRecent},
			{ID: SectionChats, Name: "Chats", BuiltIn: true, SortMode: SortModeMostRecent},
		},
	}
}

// ChatlistUpdateEvent is the client-facing chatlist.update fanout payload — the
// full post-update state, so other devices replace rather than merge.
type ChatlistUpdateEvent struct {
	Timestamp int64         `json:"timestamp"`
	Chatlist  ChatlistState `json:"chatlist"`
}
