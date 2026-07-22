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

// ListChatsToSync returns the id and updatedAt of every teams_chat with
// needMemberSync=true. Served by the read client. updatedAt is carried
// forward as the optimistic-concurrency token for SetMembersSynced.
func (s *mongoStore) ListChatsToSync(ctx context.Context) ([]ChatToSync, error) {
	chats, err := s.readChats.FindMany(ctx, bson.M{"needMemberSync": true},
		mongoutil.WithProjection(bson.M{"_id": 1, "updatedAt": 1}))
	if err != nil {
		return nil, fmt.Errorf("find chats needing member sync: %w", err)
	}
	out := make([]ChatToSync, 0, len(chats))
	for i := range chats {
		out = append(out, ChatToSync{ID: chats[i].ID, UpdatedAt: chats[i].UpdatedAt})
	}
	return out, nil
}

// SetMembersSyncedBatch writes every resolved member list in one unordered
// bulk write and advances the matched chats to the room-creation stage. Each
// chat's update is conditional on its updatedAt still equaling SeenUpdatedAt
// (optimistic conditional write); unmatched chats were rewritten concurrently
// and keep needMemberSync=true. Returns the matched count — possibly partial
// alongside an error, since an unordered bulk write can land some operations
// and fail others. Written by the write client.
func (s *mongoStore) SetMembersSyncedBatch(ctx context.Context, updates []ChatMembersUpdate, now time.Time) (int64, error) {
	res, err := s.writeChats.BulkWrite(ctx, setMembersSyncedModels(updates, now))
	var matched int64
	if res != nil {
		matched = res.Matched
	}
	if err != nil {
		return matched, fmt.Errorf("bulk set chat members synced: %w", err)
	}
	return matched, nil
}

// setMembersSyncedModels builds one conditional single-document update per
// chat for the bulk write, each carrying its own optimistic-concurrency token.
func setMembersSyncedModels(updates []ChatMembersUpdate, now time.Time) []mongo.WriteModel {
	models := make([]mongo.WriteModel, 0, len(updates))
	for i := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": updates[i].ChatID, "updatedAt": updates[i].SeenUpdatedAt}).
			SetUpdate(setMembersSyncedUpdate(updates[i].Members, now)))
	}
	return models
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
