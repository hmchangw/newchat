package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeRoom implements RoomInfoClient.
type fakeRoom struct {
	bySite map[string][]model.RoomInfo
	err    error
	calls  int
}

func (f *fakeRoom) GetRoomsInfo(_ context.Context, siteID string, _ []string) ([]model.RoomInfo, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.bySite[siteID], nil
}

func enrichHandler(m MongoStore, r RoomInfoClient) *handler {
	h := newTestHandler(&fakeStore{}, m, nil, newFakeCache())
	h.room = r
	return h
}

func hit(id, room, site, sender string) messageSearchHit {
	return messageSearchHit{MessageID: id, RoomID: room, SiteID: site, UserAccount: sender, CreatedAt: time.Now().UTC()}
}

func TestEnrichMessages_DM(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rDM": {RoomType: model.RoomTypeDM, Name: "bob"}},
		users: map[string]HRUser{"bob": {Account: "bob", EngName: "Bob Chan", ChineseName: "陳"}, "alice": {Account: "alice", EngName: "Alice", ChineseName: ""}},
	}
	h := enrichHandler(m, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rDM", "site-a", "alice")})
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeDM, out[0].Room.Type)
	assert.Equal(t, "Bob Chan 陳", out[0].Room.Name)
	require.NotNil(t, out[0].Room.HRInfo)
	assert.Equal(t, "陳", out[0].Room.HRInfo.Name)
	assert.Equal(t, "Bob Chan", out[0].Room.HRInfo.EngName)
	require.NotNil(t, out[0].Sender)
	assert.Equal(t, "alice", out[0].Sender.Account)
	assert.Equal(t, "Alice", out[0].Sender.DisplayName)
}

func TestEnrichMessages_BotDM(t *testing.T) {
	m := &fakeMongo{
		subs: map[string]SubscriptionMeta{"rBot": {RoomType: model.RoomTypeBotDM, Name: "helper.bot"}},
		apps: map[string]model.App{"helper.bot": {ID: "app1", Name: "Helper", Assistant: &model.AppAssistant{Name: "helper.bot"}}},
	}
	h := enrichHandler(m, &fakeRoom{})
	// sender is the bot itself
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rBot", "site-a", "helper.bot")})
	require.NotNil(t, out[0].Room)
	assert.Equal(t, model.RoomTypeBotDM, out[0].Room.Type)
	assert.Equal(t, "Helper", out[0].Room.Name)
	assert.Nil(t, out[0].Room.HRInfo)
	// the room carries only the app's name — no appInfo enrichment
	b, err := json.Marshal(out[0].Room)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "appInfo")
	// bot sender display name = app name
	assert.Equal(t, "Helper", out[0].Sender.DisplayName)
}

func TestEnrichMessages_ChannelUsesRoomBatch(t *testing.T) {
	m := &fakeMongo{
		subs:  map[string]SubscriptionMeta{"rCh": {RoomType: model.RoomTypeChannel, Name: "ignored"}},
		users: map[string]HRUser{"alice": {Account: "alice", EngName: "Alice"}},
	}
	r := &fakeRoom{bySite: map[string][]model.RoomInfo{
		"site-b": {{RoomID: "rCh", Found: true, Name: "General"}},
	}}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-b", "alice")})
	assert.Equal(t, model.RoomTypeChannel, out[0].Room.Type)
	assert.Equal(t, "General", out[0].Room.Name)
	assert.Nil(t, out[0].Room.HRInfo)
	assert.Equal(t, 1, r.calls)
}

func TestEnrichMessages_MissingSubscriptionFallsBackToRoomBatch(t *testing.T) {
	m := &fakeMongo{subs: map[string]SubscriptionMeta{}} // no sub for the room
	r := &fakeRoom{bySite: map[string][]model.RoomInfo{
		"site-a": {{RoomID: "rX", Found: true, Name: "Mystery"}},
	}}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rX", "site-a", "alice")})
	assert.Equal(t, "Mystery", out[0].Room.Name)
	assert.Equal(t, model.RoomType(""), out[0].Room.Type) // type unknown without a sub
}

func TestEnrichMessages_DegradesOnAllErrors(t *testing.T) {
	m := &fakeMongo{subsErr: errors.New("db down"), usersErr: errors.New("db down"), appsErr: errors.New("db down")}
	r := &fakeRoom{err: errors.New("rpc down")}
	h := enrichHandler(m, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-a", "alice")})
	require.Len(t, out, 1)
	// still returns the base message; room present with id, sender falls back to account
	require.NotNil(t, out[0].Room)
	assert.Equal(t, "rCh", out[0].Room.ID)
	assert.Equal(t, "alice", out[0].Sender.DisplayName) // fallback to account
}

func TestEnrichMessages_Empty(t *testing.T) {
	h := enrichHandler(&fakeMongo{}, &fakeRoom{})
	out := h.enrichMessages(context.Background(), "alice", nil)
	assert.Empty(t, out)
}

// A handler with no MongoStore wired (as the base message-search integration
// tests construct it) must not panic — enrichment is best-effort and degrades
// to base projections, still resolving channel names via the room RPC.
func TestEnrichMessages_NilMongoDegrades(t *testing.T) {
	r := &fakeRoom{bySite: map[string][]model.RoomInfo{
		"site-a": {{RoomID: "rCh", Found: true, Name: "General"}},
	}}
	h := enrichHandler(nil, r)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-a", "alice")})
	require.Len(t, out, 1)
	require.NotNil(t, out[0].Room)
	assert.Equal(t, "rCh", out[0].Room.ID)
	assert.Equal(t, "General", out[0].Room.Name) // room-name RPC still works without mongo
	require.NotNil(t, out[0].Sender)
	assert.Equal(t, "alice", out[0].Sender.DisplayName) // sender falls back to account
}

// Neither dependency wired: base projections survive, no panic, no names.
func TestEnrichMessages_NilMongoAndRoomDegrades(t *testing.T) {
	h := enrichHandler(nil, nil)
	out := h.enrichMessages(context.Background(), "alice", []messageSearchHit{hit("m1", "rCh", "site-a", "alice")})
	require.Len(t, out, 1)
	assert.Equal(t, "m1", out[0].MessageID)
	require.NotNil(t, out[0].Room)
	assert.Equal(t, "rCh", out[0].Room.ID)
	assert.Equal(t, "alice", out[0].Sender.DisplayName)
}
