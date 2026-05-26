// Package main: Mongo write helpers.
package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

func usersIDs() []string {
	users := BuildUsers()
	out := make([]string, 0, len(users))
	for i := range users {
		out = append(out, users[i].ID)
	}
	return out
}

func roomIDs() []string {
	rooms := BuildRooms()
	out := make([]string, 0, len(rooms))
	for i := range rooms {
		out = append(out, rooms[i].ID)
	}
	return out
}

func subscriptionIDs() []string {
	subs := BuildSubscriptions()
	out := make([]string, 0, len(subs))
	for i := range subs {
		out = append(out, subs[i].ID)
	}
	return out
}

func roomMemberIDs() []string {
	members := BuildRoomMembers()
	out := make([]string, 0, len(members))
	for i := range members {
		out = append(out, members[i].ID)
	}
	return out
}

func messageIDs() []string {
	msgs := BuildMessages()
	out := make([]string, 0, len(msgs))
	for i := range msgs {
		out = append(out, msgs[i].ID)
	}
	return out
}

func threadRoomIDs() []string {
	trs := BuildThreadRooms()
	out := make([]string, 0, len(trs))
	for i := range trs {
		out = append(out, trs[i].ID)
	}
	return out
}

func threadSubscriptionIDs() []string {
	tsubs := BuildThreadSubscriptions()
	out := make([]string, 0, len(tsubs))
	for i := range tsubs {
		out = append(out, tsubs[i].ID)
	}
	return out
}

// mongoCounts captures per-collection upsert counts for the final
// "seed complete" log line.
type mongoCounts struct {
	Users               int64
	Rooms               int64
	Subscriptions       int64
	RoomMembers         int64
	Messages            int64
	ThreadRooms         int64
	ThreadSubscriptions int64
}

// upsertAll writes every Mongo collection idempotently via BulkUpsertByID.
func upsertAll(ctx context.Context, db *mongo.Database) (mongoCounts, error) {
	var c mongoCounts

	users := mongoutil.NewCollection[model.User](db.Collection("users"))
	res, err := users.BulkUpsertByID(ctx, BuildUsers(), func(u model.User) string { return u.ID })
	if err != nil {
		return c, fmt.Errorf("seed users: %w", err)
	}
	c.Users = touched(res)

	rooms := mongoutil.NewCollection[model.Room](db.Collection("rooms"))
	res, err = rooms.BulkUpsertByID(ctx, BuildRoomsWithLastMsg(), func(r model.Room) string { return r.ID })
	if err != nil {
		return c, fmt.Errorf("seed rooms: %w", err)
	}
	c.Rooms = touched(res)

	subs := mongoutil.NewCollection[model.Subscription](db.Collection("subscriptions"))
	res, err = subs.BulkUpsertByID(ctx, BuildSubscriptions(), func(s model.Subscription) string { return s.ID })
	if err != nil {
		return c, fmt.Errorf("seed subscriptions: %w", err)
	}
	c.Subscriptions = touched(res)

	members := mongoutil.NewCollection[model.RoomMember](db.Collection("room_members"))
	res, err = members.BulkUpsertByID(ctx, BuildRoomMembers(), func(m model.RoomMember) string { return m.ID })
	if err != nil {
		return c, fmt.Errorf("seed room_members: %w", err)
	}
	c.RoomMembers = touched(res)

	msgs := mongoutil.NewCollection[model.Message](db.Collection("messages"))
	res, err = msgs.BulkUpsertByID(ctx, BuildMessages(), func(m model.Message) string { return m.ID })
	if err != nil {
		return c, fmt.Errorf("seed messages: %w", err)
	}
	c.Messages = touched(res)

	trs := mongoutil.NewCollection[model.ThreadRoom](db.Collection("thread_rooms"))
	res, err = trs.BulkUpsertByID(ctx, BuildThreadRooms(), func(t model.ThreadRoom) string { return t.ID })
	if err != nil {
		return c, fmt.Errorf("seed thread_rooms: %w", err)
	}
	c.ThreadRooms = touched(res)

	tsubs := mongoutil.NewCollection[model.ThreadSubscription](db.Collection("thread_subscriptions"))
	res, err = tsubs.BulkUpsertByID(ctx, BuildThreadSubscriptions(), func(t model.ThreadSubscription) string { return t.ID })
	if err != nil {
		return c, fmt.Errorf("seed thread_subscriptions: %w", err)
	}
	c.ThreadSubscriptions = touched(res)

	return c, nil
}

// touched returns docs affected (upserted + modified). A pure re-run
// reports 0 modified — that's the idempotent no-op path.
func touched(res *mongoutil.BulkResult) int64 {
	if res == nil {
		return 0
	}
	return res.Upserted + res.Modified
}

// deleteAll wipes only the seed records from each collection, identified
// by stable IDs. Never DROP — that would nuke hand-added dev data.
func deleteAll(ctx context.Context, db *mongo.Database) error {
	type del struct {
		name string
		ids  []string
	}
	tasks := []del{
		{"users", usersIDs()},
		{"rooms", roomIDs()},
		{"subscriptions", subscriptionIDs()},
		{"room_members", roomMemberIDs()},
		{"messages", messageIDs()},
		{"thread_rooms", threadRoomIDs()},
		{"thread_subscriptions", threadSubscriptionIDs()},
	}
	for _, t := range tasks {
		if len(t.ids) == 0 {
			continue
		}
		_, err := db.Collection(t.name).DeleteMany(ctx, bson.M{"_id": bson.M{"$in": t.ids}})
		if err != nil {
			return fmt.Errorf("reset %s: %w", t.name, err)
		}
	}
	return nil
}
