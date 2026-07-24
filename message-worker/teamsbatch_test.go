package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/teamsmigrate"
)

// fakeTransformer errors on a message whose id is in errIDs; otherwise echoes content.
// It does not resolve a sender — migrateOne resolves that separately (via the handler's
// resolver), so batch tests pair it with echoResolver.
type fakeTransformer struct{ errIDs map[string]bool }

func (f fakeTransformer) Transform(_ context.Context, raw json.RawMessage) (model.Message, error) {
	var tm teamsmigrate.Message
	if err := json.Unmarshal(raw, &tm); err != nil {
		return model.Message{}, fmt.Errorf("decode fake teams message: %w", err)
	}
	if f.errIDs[tm.ID] {
		return model.Message{}, errors.New("boom")
	}
	return model.Message{RoomID: tm.RoomID, Content: tm.Body.Content}, nil
}

// echoResolver maps a teams user id straight onto a sender, so batch tests need no store.
type echoResolver struct{}

func (echoResolver) resolve(_ context.Context, teamsUserID, displayName string) (resolvedSender, error) {
	if teamsUserID == "" {
		return resolvedSender{}, errors.New("empty teams user id")
	}
	return resolvedSender{Account: teamsUserID, UserID: teamsUserID, DisplayName: displayName}, nil
}

// captureStore records each SaveMessage; err (if set) fails every call. Only SaveMessage
// is exercised by the batch path — the rest satisfy the Store interface.
type captureStore struct {
	saved   []*model.Message
	senders []*cassParticipant
	err     error
}

func (c *captureStore) SaveMessage(_ context.Context, msg *model.Message, sender *cassParticipant, _ string) error {
	if c.err != nil {
		return c.err
	}
	c.saved = append(c.saved, msg)
	c.senders = append(c.senders, sender)
	return nil
}

func (*captureStore) SaveThreadMessage(context.Context, *model.Message, *cassParticipant, string, string) (*int, error) {
	panic("unused")
}
func (*captureStore) GetMessageSender(context.Context, string) (*cassParticipant, error) {
	panic("unused")
}
func (*captureStore) GetQuotedParentSnapshot(context.Context, string) (*cassandra.QuotedParentMessage, bool, error) {
	panic("unused")
}
func (*captureStore) GetMessageCreatedAt(context.Context, string) (time.Time, bool, error) {
	panic("unused")
}
func (*captureStore) UpdateParentMessageThreadRoomID(context.Context, string, string, time.Time, string) error {
	panic("unused")
}

func newTestHandler(store Store, tr MessageTransformer) *teamsBatchHandler {
	return &teamsBatchHandler{store: store, siteID: "s1", resolver: echoResolver{}, tr: tr}
}

func TestMigrateOne_PerMessageStatus(t *testing.T) {
	store := &captureStore{}
	h := newTestHandler(store, fakeTransformer{errIDs: map[string]bool{"bad": true}})

	from := teamsmigrate.User{ID: "u1"}
	cases := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{"ok", mustJSON(teamsmigrate.Message{ID: "ok1", RoomID: "r1", From: from, Body: teamsmigrate.Body{Content: "a"}}), model.TeamsBatchPersisted},
		{"transform error", mustJSON(teamsmigrate.Message{ID: "bad", RoomID: "r1", From: from}), model.TeamsBatchError},
		{"malformed", json.RawMessage(`{`), model.TeamsBatchError},
		{"no id skipped", mustJSON(teamsmigrate.Message{ID: "", RoomID: "r1"}), model.TeamsBatchSkipped},
		{"no roomId skipped", mustJSON(teamsmigrate.Message{ID: "ok2", Body: teamsmigrate.Body{Content: "b"}}), model.TeamsBatchSkipped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.migrateOne(context.Background(), tc.raw)
			require.NoError(t, err) // per-message errors are logged, never Nak the batch
			assert.Equal(t, tc.want, res.Status)
		})
	}

	// Only the good, roomId-bearing message was written — with the roomId-scoped deterministic id and resolved sender.
	require.Len(t, store.saved, 1)
	assert.Equal(t, teamsmigrate.DeterministicMessageID("r1", "ok1"), store.saved[0].ID)
	assert.Equal(t, "u1", store.senders[0].Account)
}

func TestHandleBatch_SaveErrorNaks(t *testing.T) {
	store := &captureStore{err: errors.New("cassandra down")}
	h := newTestHandler(store, fakeTransformer{})
	req := model.TeamsBatchRequest{Messages: []json.RawMessage{
		mustJSON(teamsmigrate.Message{ID: "x", RoomID: "r1", From: teamsmigrate.User{ID: "u1"}, Body: teamsmigrate.Body{Content: "a"}}),
	}}
	require.Error(t, h.handleBatch(context.Background(), req), "an infra failure must surface so the consumer Naks")
}

func TestHandleBatch_TransformErrorDoesNotNak(t *testing.T) {
	store := &captureStore{}
	h := newTestHandler(store, fakeTransformer{errIDs: map[string]bool{"bad": true}})
	req := model.TeamsBatchRequest{Messages: []json.RawMessage{
		mustJSON(teamsmigrate.Message{ID: "bad", RoomID: "r1", From: teamsmigrate.User{ID: "u1"}}),
		mustJSON(teamsmigrate.Message{ID: "ok", RoomID: "r1", From: teamsmigrate.User{ID: "u1"}, Body: teamsmigrate.Body{Content: "a"}}),
	}}
	require.NoError(t, h.handleBatch(context.Background(), req)) // bad message logged; batch Acks
	require.Len(t, store.saved, 1)                               // the good one still written
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
