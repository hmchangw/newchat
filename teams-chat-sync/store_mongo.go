package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoStore implements TeamsUserStore and TeamsChatStore over two databases:
// readDB (the teams_user scan, typically a secondary-preferred read client)
// and writeDB (the teams_user watermark update and teams_chat upserts). Every
// collection goes through mongoutil.Collection so projection and cursor
// handling stay in the shared helper.
type mongoStore struct {
	readUsers  *mongoutil.Collection[model.TeamsUser]
	writeUsers *mongoutil.Collection[model.TeamsUser]
	writeChats *mongoutil.Collection[model.TeamsChat]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readUsers:  mongoutil.NewCollection[model.TeamsUser](readDB.Collection("teams_user")),
		writeUsers: mongoutil.NewCollection[model.TeamsUser](writeDB.Collection("teams_user")),
		writeChats: mongoutil.NewCollection[model.TeamsChat](writeDB.Collection("teams_chat")),
	}
}

// ListUsers returns every teams_user projected to the sync fields
// (_id, siteId, account, from). Served by the read client.
func (s *mongoStore) ListUsers(ctx context.Context) ([]model.TeamsUser, error) {
	users, err := s.readUsers.FindMany(ctx, bson.M{}, mongoutil.WithProjection(bson.M{
		"_id": 1, "siteId": 1, "account": 1, "from": 1,
	}))
	if err != nil {
		return nil, fmt.Errorf("list teams users: %w", err)
	}
	return users, nil
}

// SetFrom advances one user's sync watermark. Written by the write client.
func (s *mongoStore) SetFrom(ctx context.Context, userID string, from time.Time) error {
	if _, err := s.writeUsers.Raw().UpdateByID(ctx, userID, bson.M{"$set": bson.M{"from": from}}); err != nil {
		return fmt.Errorf("set teams user watermark: %w", err)
	}
	return nil
}

// UpsertChats bulk-upserts chats keyed on _id. oneOnOne chats are insert-only
// (all fields $setOnInsert); a small non-oneOnOne chat is finalized inline
// (members + needCreateRoom refreshed in $set); a large non-oneOnOne chat is
// deferred to member-sync (members untouched). createdDateTime and siteID are
// always $setOnInsert-only.
func (s *mongoStore) UpsertChats(ctx context.Context, chats []model.TeamsChat) error {
	models := make([]mongo.WriteModel, 0, len(chats))
	//nolint:gocritic // rangeValCopy: c is heavy but using index-range would be less idiomatic
	for _, c := range chats {
		models = append(models, chatUpsertModel(c))
	}
	if _, err := s.writeChats.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("upsert teams chats: %w", err)
	}
	return nil
}

// chatUpsertModel builds the upsert for one chat. createdDateTime and siteID
// are $setOnInsert-only — once a chat has a siteID it never changes. Three
// branches, keyed on chatType and needMemberSync:
//   - oneOnOne: every field under $setOnInsert — they never change after
//     creation, so an existing document is never modified (the "ignore oneOnOne
//     update" rule enforced atomically, without a read).
//   - non-oneOnOne, needMemberSync=false: a small chat with a complete inline
//     roster — finalize it here ($set members + needCreateRoom=true).
//   - non-oneOnOne, needMemberSync=true: a large chat — defer members and room
//     creation to teams-chat-member-sync.
//
//nolint:gocritic // hugeParam: c is heavy but unavoidable in this builder pattern
func chatUpsertModel(c model.TeamsChat) mongo.WriteModel {
	filter := bson.M{"_id": c.ID}
	if c.ChatType == model.TeamsChatTypeOneOnOne {
		// A oneOnOne chat is complete on first sight (exactly two known members,
		// no separate member sync), so it is immediately ready for room creation.
		return mongoutil.UpsertModel(filter, bson.M{"$setOnInsert": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"createdDateTime":     c.CreatedDateTime,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"members":             c.Members,
			"siteId":              c.SiteID,
			"updatedAt":           c.UpdatedAt,
			"needMemberSync":      c.NeedMemberSync,
			"needCreateRoom":      true,
		}})
	}
	if !c.NeedMemberSync {
		// Small non-oneOnOne chat (fewer than inlineMemberThreshold members): the
		// list-chats $expand=members roster is already complete, so this sync
		// finalizes the chat itself instead of deferring to teams-chat-member-sync.
		// members and needCreateRoom move into $set — exactly what member-sync
		// would write on a resolve: every re-sync re-writes the fresh roster and
		// re-flags needCreateRoom, so each chat change yields one create-or-sync
		// event downstream (room-creation's compare-and-set on updatedAt clears the
		// flag). needMemberSync is forced false so member-sync skips the chat, and
		// (unlike the defer path) needCreateRoom must NOT be $setOnInsert here.
		return mongoutil.UpsertModel(filter, bson.M{
			"$setOnInsert": bson.M{
				"createdDateTime": c.CreatedDateTime,
				"siteId":          c.SiteID,
			},
			"$set": bson.M{
				"name":                c.Name,
				"chatType":            c.ChatType,
				"lastUpdatedDateTime": c.LastUpdatedDateTime,
				"updatedAt":           c.UpdatedAt,
				"members":             c.Members,
				"needMemberSync":      false,
				"needCreateRoom":      true,
			},
		})
	}
	// Large non-oneOnOne chat (roster at/above inlineMemberThreshold): defer room
	// creation to teams-chat-member-sync. The two pipeline flags sit on opposite
	// sides on purpose:
	//   - needMemberSync in $set: re-set true on every re-sync. A chat is
	//     re-listed whenever its lastUpdatedDateTime moves (any Teams activity,
	//     including a membership change), so this re-triggers member-sync to
	//     re-resolve the roster — keeping the room's members in sync over time.
	//   - needCreateRoom in $setOnInsert: written only on insert, so a re-sync
	//     can never clobber the true that member-sync sets. member-sync flips it
	//     true after each roster resolve and room-creation clears it, so every
	//     membership change yields exactly one create-or-sync event downstream.
	// members is likewise owned by member-sync and never written here.
	return mongoutil.UpsertModel(filter, bson.M{
		"$setOnInsert": bson.M{
			"createdDateTime": c.CreatedDateTime,
			"siteId":          c.SiteID,
			"needCreateRoom":  false,
		},
		"$set": bson.M{
			"name":                c.Name,
			"chatType":            c.ChatType,
			"lastUpdatedDateTime": c.LastUpdatedDateTime,
			"updatedAt":           c.UpdatedAt,
			"needMemberSync":      c.NeedMemberSync,
		},
	})
}
