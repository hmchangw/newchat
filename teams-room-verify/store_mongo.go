package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

// mongoStore implements TeamsChatStore over two databases: readChats (the
// flagged-chat scan, typically a secondary-preferred read client) and
// writeChats (the needVerify flag clear, a primary client).
type mongoStore struct {
	readChats  *mongoutil.Collection[model.TeamsChat]
	writeChats *mongoutil.Collection[model.TeamsChat]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readChats:  mongoutil.NewCollection[model.TeamsChat](readDB.Collection("teams_chat")),
		writeChats: mongoutil.NewCollection[model.TeamsChat](writeDB.Collection("teams_chat")),
	}
}

// ListChatsNeedingVerify returns every teams_chat with needVerify=true,
// projected to exactly the fields verification needs. Served by the read client.
func (s *mongoStore) ListChatsNeedingVerify(ctx context.Context) ([]model.TeamsChat, error) {
	// Stable _id sort so batch composition is deterministic across runs.
	// updatedAt is carried as the compare-and-set token for MarkVerified.
	chats, err := s.readChats.FindMany(ctx, bson.M{"needVerify": true},
		mongoutil.WithProjection(bson.M{"_id": 1, "members": 1, "siteId": 1, "updatedAt": 1}),
		mongoutil.WithSort(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list chats needing verify: %w", err)
	}
	return chats, nil
}

// MarkVerified clears needVerify for the given refs. Written by the primary
// client. A nil/empty ref slice is a no-op.
func (s *mongoStore) MarkVerified(ctx context.Context, refs []VerifiedRef) error {
	if len(refs) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(refs))
	for _, r := range refs {
		// Compare-and-set on updatedAt: only clear the flag if member-sync has not
		// re-written the chat since we read it. A stale ref matches nothing, so the
		// chat is re-verified next run against its new roster.
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": r.ID, "updatedAt": r.UpdatedAt}).
			SetUpdate(bson.M{"$set": bson.M{"needVerify": false}}))
	}
	if _, err := s.writeChats.BulkWrite(ctx, models); err != nil {
		return fmt.Errorf("mark verified: %w", err)
	}
	return nil
}
