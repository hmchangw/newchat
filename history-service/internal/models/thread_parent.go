package models

// ThreadFilter selects which threads' parent messages to return.
type ThreadFilter string

const (
	ThreadFilterAll       ThreadFilter = "all"
	ThreadFilterFollowing ThreadFilter = "following"
	ThreadFilterUnread    ThreadFilter = "unread"
)

type GetThreadParentMessagesRequest struct {
	Filter ThreadFilter `json:"filter"`
	Offset int          `json:"offset"`
	Limit  int          `json:"limit"`
}

type GetThreadParentMessagesResponse struct {
	ParentMessages []Message `json:"parentMessages"` // ordered by most recent reply activity
	Total          int64     `json:"total"`          // raw MongoDB count before post-hydration access filtering; use for pagination math only, not slice length
}
