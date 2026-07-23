package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeTransformer errors on a message whose id is in errIDs; otherwise echoes content.
type fakeTransformer struct{ errIDs map[string]bool }

func (f fakeTransformer) Transform(_ context.Context, raw json.RawMessage) (model.Message, error) {
	var tm teamsMessage
	_ = json.Unmarshal(raw, &tm)
	if f.errIDs[tm.ID] {
		return model.Message{}, errors.New("boom")
	}
	return model.Message{RoomID: tm.RoomID, Content: tm.Body.Content}, nil
}

// captureProcessor records each event fed to the persist pipeline; err (if set) fails every call.
type captureProcessor struct {
	events      []model.MessageEvent
	isMigration []bool
	err         error
}

func (c *captureProcessor) process(_ context.Context, data []byte, isMigration bool) error {
	if c.err != nil {
		return c.err
	}
	var evt model.MessageEvent
	_ = sonic.Unmarshal(data, &evt)
	c.events = append(c.events, evt)
	c.isMigration = append(c.isMigration, isMigration)
	return nil
}

func newTestHandler(proc *captureProcessor, tr MessageTransformer) *teamsBatchHandler {
	h := newTeamsBatchHandler(nil, "s1", proc.process)
	h.newTransformer = func(identityResolver) MessageTransformer { return tr }
	return h
}

func TestMigrateOne_PerMessageStatus(t *testing.T) {
	proc := &captureProcessor{}
	h := newTestHandler(proc, fakeTransformer{errIDs: map[string]bool{"bad": true}})
	tr := h.newTransformer(nil)

	cases := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{"ok", mustJSON(teamsMessage{ID: "ok1", RoomID: "r1", Body: teamsBody{Content: "a"}}), model.TeamsBatchPersisted},
		{"transform error", mustJSON(teamsMessage{ID: "bad", RoomID: "r1"}), model.TeamsBatchError},
		{"malformed", json.RawMessage(`{`), model.TeamsBatchError},
		{"no id skipped", mustJSON(teamsMessage{ID: "", RoomID: "r1"}), model.TeamsBatchSkipped},
		{"no roomId skipped", mustJSON(teamsMessage{ID: "ok2", Body: teamsBody{Content: "b"}}), model.TeamsBatchSkipped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := h.migrateOne(context.Background(), tr, tc.raw)
			require.NoError(t, err) // per-message errors are logged, never Nak the batch
			assert.Equal(t, tc.want, res.Status)
		})
	}

	// Only the good, roomId-bearing message reached the pipeline — with the deterministic id + migration flag.
	require.Len(t, proc.events, 1)
	assert.Equal(t, deterministicMessageID("r1", "ok1"), proc.events[0].Message.ID)
	assert.True(t, proc.isMigration[0])
}

func TestHandleBatch_ProcessErrorNaks(t *testing.T) {
	proc := &captureProcessor{err: errors.New("cassandra down")}
	h := newTestHandler(proc, fakeTransformer{})
	req := model.TeamsBatchRequest{Messages: []json.RawMessage{
		mustJSON(teamsMessage{ID: "x", RoomID: "r1", Body: teamsBody{Content: "a"}}),
	}}
	require.Error(t, h.handleBatch(context.Background(), req), "an infra failure must surface so the consumer Naks")
}

func TestHandleBatch_TransformErrorDoesNotNak(t *testing.T) {
	proc := &captureProcessor{}
	h := newTestHandler(proc, fakeTransformer{errIDs: map[string]bool{"bad": true}})
	req := model.TeamsBatchRequest{Messages: []json.RawMessage{
		mustJSON(teamsMessage{ID: "bad", RoomID: "r1"}),
		mustJSON(teamsMessage{ID: "ok", RoomID: "r1", Body: teamsBody{Content: "a"}}),
	}}
	require.NoError(t, h.handleBatch(context.Background(), req)) // bad message logged; batch Acks
	require.Len(t, proc.events, 1)                               // the good one still processed
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
