package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// sourceRoomMember mirrors one legacy company_room_members doc (SOURCE_DATA §7): one doc per
// (room, member) pair, insert + hard delete only.
type sourceRoomMember struct {
	ID     string `bson:"_id"`
	RoomID string `bson:"rid"`
	Member struct {
		Type     string `bson:"type"` // org | individual | app | user (only first two are mapped)
		ID       string `bson:"id"`   // HR org id (org) or legacy user _id (individual)
		Username string `bson:"username"`
	} `bson:"member"`
	Ts time.Time `bson:"ts"`
	// federation.origin is informational only — the collections lane migrates every doc in this site's DB.
}

// handleRoomMember migrates one company_room_members event into target room_members (direct write).
// Target _id = source _id, so deletes route without a lookup and skipped-type deletes no-op.
//
//nolint:gocritic // ev passed by value to mirror handle's signature; off the hot path.
func (h *handler) handleRoomMember(ctx context.Context, ev oplogEvent) error {
	if ev.Op == "delete" {
		id, err := documentKeyID(ev.DocumentKey)
		if err != nil {
			return fmt.Errorf("delete room member: %w", err) // poison: unaddressable delete
		}
		deleted, derr := h.target.DeleteRoomMember(ctx, id)
		if derr != nil {
			return fmt.Errorf("delete room member: %w", derr)
		}
		if deleted {
			h.metrics.onWrite(ctx, h.roomMembersColl, "delete")
		} else {
			h.metrics.onWrite(ctx, h.roomMembersColl, "delete_noop")
		}
		return nil
	}

	// Warn/Error, never Debug: skips must be visible at the default info log level.
	if ev.Op != "insert" && ev.Op != "replace" && ev.Op != "update" {
		slog.Warn("skipping room-member event with unhandled op",
			"op", ev.Op, "eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "unknown_op")
		return migration.ErrSkipped
	}

	doc, skip, err := h.resolveDoc(ctx, ev)
	if err != nil {
		return err
	}
	if skip { // update/degraded-insert whose source doc vanished before the re-read
		slog.Warn("room-member source doc vanished before re-read — skipping (delete event follows)",
			"op", ev.Op, "eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "room_member_gone")
		return migration.ErrSkipped
	}
	if ev.Op == "update" {
		// Contract violation: legacy is insert+hard-delete only (SOURCE_DATA §7); applied defensively, loudly.
		slog.Warn("unexpected room-member update — legacy contract says insert/delete only; applied defensively",
			"eventId", ev.EventID, "request_id", natsutil.RequestIDFromContext(ctx))
	}

	var srm sourceRoomMember
	if uerr := bson.UnmarshalExtJSON(doc, false, &srm); uerr != nil {
		return fmt.Errorf("%w: decode room member: %v", migration.ErrPoison, uerr) //nolint:errorlint // single-%w sentinel wrap; decode err is informational
	}

	rm, mapped, merr := h.mapRoomMember(ctx, &srm)
	if merr != nil {
		return merr
	}
	if !mapped { // unmapped member.type — catch-all skip, decision in SOURCE_DATA §7 finding
		slog.Error("unmapped room-member type — skipping (revisit once app/user semantics are decided)",
			"member_type", srm.Member.Type, "rid", srm.RoomID, "eventId", ev.EventID,
			"request_id", natsutil.RequestIDFromContext(ctx))
		h.metrics.onSkipped(ctx, "room_member_type_unmapped")
		return migration.ErrSkipped
	}
	if uerr := h.target.UpsertRoomMember(ctx, rm); uerr != nil {
		return fmt.Errorf("upsert room member: %w", uerr)
	}
	h.metrics.onWrite(ctx, h.roomMembersColl, "upsert")
	return nil
}

// mapRoomMember maps a source doc to the target model; mapped=false ⇒ unconfirmed member.type, the
// caller skips loudly. Individual ids resolve to the NEW-stack user id; unresolved ⇒ Nak until seeded.
func (h *handler) mapRoomMember(ctx context.Context, srm *sourceRoomMember) (model.RoomMember, bool, error) {
	// A blank _id or rid would upsert a junk row (or a delete that can never match) — structurally
	// invalid source data that redelivery can't fix. Poison.
	if strings.TrimSpace(srm.ID) == "" {
		return model.RoomMember{}, false, fmt.Errorf("%w: room member has no _id", migration.ErrPoison)
	}
	if strings.TrimSpace(srm.RoomID) == "" {
		return model.RoomMember{}, false, fmt.Errorf("%w: room member %q has no rid", migration.ErrPoison, srm.ID)
	}

	rm := model.RoomMember{ID: srm.ID, RoomID: srm.RoomID, Ts: srm.Ts}
	switch srm.Member.Type {
	case "org":
		// A blank org id would upsert a junk row keyed by an empty enrichment join key — mirrors
		// the blank-username defence below. Poison: redelivery can't fix it.
		if strings.TrimSpace(srm.Member.ID) == "" {
			return model.RoomMember{}, false, fmt.Errorf("%w: org room member %q has no member.id", migration.ErrPoison, srm.ID)
		}
		rm.Member = model.RoomMemberEntry{ID: srm.Member.ID, Type: model.RoomMemberOrg}
	case "individual":
		// A blank username would always miss FindUserID and Nak-storm to MAX_DELIVER — or worse,
		// silently resolve to a target user with account:"". Poison: redelivery can't fix it.
		if strings.TrimSpace(srm.Member.Username) == "" {
			return model.RoomMember{}, false, fmt.Errorf("%w: individual room member %q has no username", migration.ErrPoison, srm.ID)
		}
		// Deliberately uncached: sequential bounded-lifetime tail; add an account→id cache only if volume demands.
		userID, found, err := h.target.FindUserID(ctx, srm.Member.Username)
		if err != nil {
			return model.RoomMember{}, false, fmt.Errorf("resolve room-member user %q: %w", srm.Member.Username, err)
		}
		if !found {
			h.metrics.onResolveMiss(ctx, "room_member_user")
			return model.RoomMember{}, false, fmt.Errorf("room-member user %q not seeded yet — retrying", srm.Member.Username)
		}
		rm.Member = model.RoomMemberEntry{ID: userID, Type: model.RoomMemberIndividual, Account: srm.Member.Username}
	default:
		return model.RoomMember{}, false, nil
	}
	return rm, true, nil
}
