package models

// Request bodies for the chatlist section-definition RPCs. Each handler returns
// the full post-update model.ChatlistState. Membership/order is NOT here — that
// rides the room-service moveChat RPC onto the subscription.

// ChatlistSectionCreateRequest creates a custom section. SortMode is optional
// (defaults to custom); built-in ids and derived membership are never client-sent.
type ChatlistSectionCreateRequest struct {
	Name     string `json:"name"`
	SortMode string `json:"sortMode,omitempty"`
}

// ChatlistSectionDeleteRequest removes a custom section by id.
type ChatlistSectionDeleteRequest struct {
	SectionID string `json:"sectionId"`
}

// ChatlistSectionRenameRequest renames a custom section.
type ChatlistSectionRenameRequest struct {
	SectionID string `json:"sectionId"`
	Name      string `json:"name"`
}

// ChatlistSectionReorderRequest replaces the section display order; SectionOrder
// must be a permutation of every current section id (built-in + custom).
type ChatlistSectionReorderRequest struct {
	SectionOrder []string `json:"sectionOrder"`
}

// ChatlistSectionSetSortModeRequest sets one section's sortMode (custom|mostRecent).
type ChatlistSectionSetSortModeRequest struct {
	SectionID string `json:"sectionId"`
	SortMode  string `json:"sortMode"`
}
