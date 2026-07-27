package main

import (
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildUserRoomAction builds the ES bulk action for one subscription row.
// Always ActionUpdate against the add-room script — every subscription
// this job reads is a current, active membership (see buildSpotlightAction
// and Global Constraints). Bot subscriptions are skipped, matching
// search-sync-worker's live BuildAction (bots don't search; a bot's
// membership would only inflate the per-user access-control view). A
// skipped subscription returns a zero-value BulkAction and a nil error —
// callers (runner.go) must check for the zero value before flushing it.
//
//nolint:gocritic // hugeParam: sub is passed by value to match the task API contract; struct copy overhead is acceptable in the migration context (not hot path)
func buildUserRoomAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error) {
	if sub.User.IsBot {
		return searchengine.BulkAction{}, nil
	}
	if sub.RoomID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build user-room action: empty roomId for subscription %s", sub.ID)
	}
	if sub.User.Account == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build user-room action: empty account for subscription %s", sub.ID)
	}

	var hss int64
	if sub.HistorySharedSince != nil {
		hss = sub.HistorySharedSince.UnixMilli()
	}

	return searchengine.BulkAction{
		Action: searchengine.ActionUpdate,
		Index:  indexName,
		DocID:  sub.User.Account,
		Doc:    searchindex.BuildAddRoomUpdateBody(sub.User.Account, sub.RoomID, sub.JoinedAt.UnixMilli(), hss),
	}, nil
}
