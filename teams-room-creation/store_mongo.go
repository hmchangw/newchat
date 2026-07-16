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
// writeChats (the needCreateRoom flag clear, a primary client).
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

// ListChatsNeedingRoom returns every teams_chat with needCreateRoom=true,
// projected to exactly the fields the event needs. Served by the read client.
func (s *mongoStore) ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error) {
	chats, err := s.readChats.FindMany(ctx, bson.M{"needCreateRoom": true}, mongoutil.WithProjection(bson.M{
		"_id": 1, "name": 1, "members": 1, "createdDateTime": 1, "siteId": 1,
	}))
	if err != nil {
		return nil, fmt.Errorf("list chats needing room: %w", err)
	}
	return chats, nil
}

// MarkRoomsCreated clears needCreateRoom for the given ids. Written by the
// primary client. A nil/empty id slice is a no-op.
func (s *mongoStore) MarkRoomsCreated(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.writeChats.Raw().UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		bson.M{"$set": bson.M{"needCreateRoom": false}})
	if err != nil {
		return fmt.Errorf("mark rooms created: %w", err)
	}
	return nil
}
