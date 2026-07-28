package main

import (
	"encoding/json"
	"fmt"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildSpotlightAction builds the ES bulk action for one subscription row.
// Always ActionIndex — every subscription this job reads is a current,
// active membership (see Global Constraints: subscriptions are hard-deleted
// on leave, so there is no closed-row/delete path to migrate here, unlike
// the live worker's per-INBOX-event add/remove branches).
//
//nolint:gocritic // hugeParam: sub is passed by value to match the task API contract; struct copy overhead is acceptable in the migration context (not hot path)
func buildSpotlightAction(sub model.Subscription, indexName string) (searchengine.BulkAction, error) {
	if sub.RoomID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build spotlight action: empty roomId for subscription %s", sub.ID)
	}
	if sub.User.Account == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build spotlight action: empty account for subscription %s", sub.ID)
	}

	doc := searchindex.NewSpotlightDoc(searchindex.SpotlightFields{
		UserAccount: sub.User.Account,
		RoomID:      sub.RoomID,
		RoomName:    sub.Name,
		RoomType:    string(sub.RoomType),
		SiteID:      sub.SiteID,
		JoinedAt:    sub.JoinedAt,
	})

	body, err := json.Marshal(doc)
	if err != nil {
		return searchengine.BulkAction{}, fmt.Errorf("marshal spotlight doc for subscription %s: %w", sub.ID, err)
	}

	return searchengine.BulkAction{
		Action:  searchengine.ActionIndex,
		Index:   indexName,
		DocID:   sub.User.Account + "_" + sub.RoomID,
		Version: sub.JoinedAt.UnixMilli(),
		Doc:     body,
	}, nil
}
