package mongoutil

type OffsetPageRequest struct {
	Offset int64
	Limit  int64
}

type OffsetPage[T any] struct {
	Data  []T
	Total int64
}

// EmptyPage returns a zero-result page with non-nil Data so JSON marshals to [] not null.
func EmptyPage[T any]() OffsetPage[T] {
	return OffsetPage[T]{Data: []T{}}
}

// OffsetPageHasMore signals whether more rows follow instead of a full count — the
// result of over-fetching by one (limit+1). Cheaper than OffsetPage (no $count /
// $facet), at the cost of not knowing the total.
//
// HasMore reflects whether the DATABASE page had an extra row; it is computed
// before any post-fetch filtering the caller applies. A caller that drops rows
// after paging (e.g. cross-site soft-deleted rooms) may return fewer than the
// requested limit while HasMore stays true — it means "the DB has more", not
// "the client has more".
type OffsetPageHasMore[T any] struct {
	Data    []T
	HasMore bool
}

// NewOffsetPageRequest validates offset+limit. Default limit 20, max 100, negative offset clamped to 0.
func NewOffsetPageRequest(offset, limit int) OffsetPageRequest {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return OffsetPageRequest{Offset: int64(offset), Limit: int64(limit)}
}
