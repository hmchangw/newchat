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

// mongoStore implements TeamsChatStore and TeamsUserStore over two databases:
// readDB (the teams_chat scan + teams_user resolution, typically a
// secondary-preferred client) and writeDB (the teams_chat member update).
type mongoStore struct {
	readChats  *mongoutil.Collection[model.TeamsChat]
	writeChats *mongoutil.Collection[model.TeamsChat]
	readUsers  *mongoutil.Collection[model.TeamsUser]
}

func newMongoStore(readDB, writeDB *mongo.Database) *mongoStore {
	return &mongoStore{
		readChats:  mongoutil.NewCollection[model.TeamsChat](readDB.Collection("teams_chat")),
		writeChats: mongoutil.NewCollection[model.TeamsChat](writeDB.Collection("teams_chat")),
		readUsers:  mongoutil.NewCollection[model.TeamsUser](readDB.Collection("teams_user")),
	}
}

// ListChatsToSync returns the _id of every teams_chat with needMemberSync=true.
// Served by the read client. FindMany decodes into TeamsChat with only _id
// projected (other fields zero); we read just the ID.
func (s *mongoStore) ListChatsToSync(ctx context.Context) ([]string, error) {
	chats, err := s.readChats.FindMany(ctx, bson.M{"needMemberSync": true},
		mongoutil.WithProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find chats needing member sync: %w", err)
	}
	ids := make([]string, 0, len(chats))
	for i := range chats {
		ids = append(ids, chats[i].ID)
	}
	return ids, nil
}

// SetMembersSynced writes the resolved member list and advances the chat to the
// room-creation stage. Written by the write client.
func (s *mongoStore) SetMembersSynced(ctx context.Context, chatID string, members []model.TeamsChatMember, now time.Time) error {
	if _, err := s.writeChats.Raw().UpdateByID(ctx, chatID, setMembersSyncedUpdate(members, now)); err != nil {
		return fmt.Errorf("set chat members synced: %w", err)
	}
	return nil
}

// setMembersSyncedUpdate builds the $set document for a completed member sync.
func setMembersSyncedUpdate(members []model.TeamsChatMember, now time.Time) bson.M {
	return bson.M{"$set": bson.M{
		"members":        members,
		"needCreateRoom": true,
		"needMemberSync": false,
		"updatedAt":      now,
	}}
}

// AccountsByIDs resolves userIds to accounts from teams_user (read client),
// projecting _id and account. Ids without a record are absent from the map.
func (s *mongoStore) AccountsByIDs(ctx context.Context, ids []string) (map[string]string, error) {
	out := make(map[string]string, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	users, err := s.readUsers.FindMany(ctx, bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(bson.M{"_id": 1, "account": 1}))
	if err != nil {
		return nil, fmt.Errorf("find teams users by id: %w", err)
	}
	for _, u := range users {
		out[u.ID] = u.Account
	}
	return out, nil
}
