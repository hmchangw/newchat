package main

import (
	"encoding/json"
	"fmt"
)

type subChangeOp int

const (
	subOpen subChangeOp = iota
	subClose
)

// subChange is one subscription mutation the connection must apply.
type subChange struct {
	Op     subChangeOp
	RoomID string
	Global bool // meaningful for subOpen
}

// subUpdateEnvelope covers both wire shapes on the subscription.update
// subject: the full SubscriptionUpdateEvent ("added", "role_updated", ...)
// and the lean SubscriptionRemovedEvent ("removed", subscription carries
// only roomId/roomType/u). subRow's fields are the intersection we need.
type subUpdateEnvelope struct {
	Action       string `json:"action"`
	Subscription subRow `json:"subscription"`
}

// applySubscriptionUpdate mutates plan per one subscription.update event and
// returns the changes to apply (0, 1, or 2 — a namespace flip closes the old
// sub then opens the new one, mirroring the frontend's openChannelSub).
func applySubscriptionUpdate(plan map[string]bool, data []byte) ([]subChange, error) {
	var evt subUpdateEnvelope
	if err := json.Unmarshal(data, &evt); err != nil {
		return nil, fmt.Errorf("decode subscription.update event: %w", err)
	}
	roomID := evt.Subscription.RoomID
	switch evt.Action {
	case "added":
		if evt.Subscription.RoomType != "channel" || roomID == "" {
			return nil, nil
		}
		global := roomGlobal(evt.Subscription.Room)
		if have, open := plan[roomID]; open {
			if have == global {
				return nil, nil // already subscribed on the right namespace
			}
			plan[roomID] = global
			return []subChange{{Op: subClose, RoomID: roomID}, {Op: subOpen, RoomID: roomID, Global: global}}, nil
		}
		plan[roomID] = global
		return []subChange{{Op: subOpen, RoomID: roomID, Global: global}}, nil
	case "removed":
		if _, open := plan[roomID]; !open {
			return nil, nil
		}
		delete(plan, roomID)
		return []subChange{{Op: subClose, RoomID: roomID}}, nil
	default:
		// role_updated, mute_toggled, favorite_toggled, read, ... — state
		// the real client tracks, none of it changes what we subscribe to.
		return nil, nil
	}
}

// diffPlans returns the changes that move a connection from oldPlan to
// newPlan — the post-reconnect resync: events during the disconnect window
// are gone, so the fresh bootstrap walk is reconciled against what the
// connection still holds.
func diffPlans(oldPlan, newPlan map[string]bool) []subChange {
	var changes []subChange
	for roomID, oldGlobal := range oldPlan {
		newGlobal, still := newPlan[roomID]
		switch {
		case !still:
			changes = append(changes, subChange{Op: subClose, RoomID: roomID})
		case newGlobal != oldGlobal:
			changes = append(changes,
				subChange{Op: subClose, RoomID: roomID},
				subChange{Op: subOpen, RoomID: roomID, Global: newGlobal})
		}
	}
	for roomID, global := range newPlan {
		if _, had := oldPlan[roomID]; !had {
			changes = append(changes, subChange{Op: subOpen, RoomID: roomID, Global: global})
		}
	}
	return changes
}
