package main

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// stubMsg implements jetstream.Msg for testing.
type stubMsg struct {
	data   []byte
	acked  bool
	nacked bool
}

func (m *stubMsg) Data() []byte                              { return m.data }
func (m *stubMsg) Ack() error                                { m.acked = true; return nil }
func (m *stubMsg) Nak() error                                { m.nacked = true; return nil }
func (m *stubMsg) NakWithDelay(time.Duration) error          { return nil }
func (m *stubMsg) InProgress() error                         { return nil }
func (m *stubMsg) Term() error                               { return nil }
func (m *stubMsg) TermWithReason(string) error               { return nil }
func (m *stubMsg) Metadata() (*jetstream.MsgMetadata, error) { return nil, nil }
func (m *stubMsg) Subject() string                           { return "" }
func (m *stubMsg) Reply() string                             { return "" }
func (m *stubMsg) Headers() nats.Header                      { return nil }
func (m *stubMsg) DoubleAck(context.Context) error           { return nil }

func makeStubMsg(t *testing.T, evt *model.MessageEvent) *stubMsg {
	t.Helper()
	data, err := json.Marshal(evt)
	require.NoError(t, err)
	return &stubMsg{data: data}
}

func TestHandler_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)

	evt := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC),
		},
		SiteID: "site-a", Timestamp: 100,
	}
	msg := makeStubMsg(t, &evt)

	h.Add(msg)
	assert.Equal(t, 1, h.MessageCount())
}

func TestHandler_Add_MalformedJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)
	h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)

	msg := &stubMsg{data: []byte("{invalid")}
	h.Add(msg)
	assert.Equal(t, 0, h.MessageCount())
	assert.True(t, msg.acked)
}

func TestHandler_Flush(t *testing.T) {
	ts := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	baseEvt := model.MessageEvent{
		Event: model.EventCreated,
		Message: model.Message{
			ID: "m1", RoomID: "r1", UserID: "u1", UserAccount: "alice",
			Content: "hello", CreatedAt: ts,
		},
		SiteID: "site-a", Timestamp: 100,
	}

	t.Run("all items succeed — all acked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{Status: 201}}, nil)

		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		msg := makeStubMsg(t, &baseEvt)
		h.Add(msg)
		h.Flush(context.Background())

		assert.True(t, msg.acked)
		assert.False(t, msg.nacked)
		assert.Equal(t, 0, h.MessageCount())
	})

	t.Run("version conflict (409) — acked not nacked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{Status: 409, Error: "version conflict"}}, nil)

		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		msg := makeStubMsg(t, &baseEvt)
		h.Add(msg)
		h.Flush(context.Background())

		assert.True(t, msg.acked)
		assert.False(t, msg.nacked)
	})

	t.Run("item failure — nacked for redelivery", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{Status: 500, Error: "internal"}}, nil)

		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		msg := makeStubMsg(t, &baseEvt)
		h.Add(msg)
		h.Flush(context.Background())

		assert.False(t, msg.acked)
		assert.True(t, msg.nacked)
	})

	t.Run("total bulk failure — all nacked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(2)).
			Return(nil, fmt.Errorf("connection refused"))

		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		msg1 := makeStubMsg(t, &baseEvt)
		evt2 := baseEvt
		evt2.Message.ID = "m2"
		msg2 := makeStubMsg(t, &evt2)

		h.Add(msg1)
		h.Add(msg2)
		h.Flush(context.Background())

		assert.True(t, msg1.nacked)
		assert.True(t, msg2.nacked)
		assert.Equal(t, 0, h.MessageCount())
	})

	t.Run("empty flush is no-op", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		h.Flush(context.Background())
		assert.Equal(t, 0, h.MessageCount())
	})

	t.Run("mixed results — per-item ack/nak", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(3)).
			Return([]searchengine.BulkResult{
				{Status: 201},
				{Status: 409, Error: "version conflict"},
				{Status: 500, Error: "shard failure"},
			}, nil)

		h := NewHandler(store, newMessageCollection("msgs-v1", time.Time{}), 500)
		msgs := make([]*stubMsg, 3)
		for i := range msgs {
			evt := baseEvt
			evt.Message.ID = fmt.Sprintf("m%d", i)
			msgs[i] = makeStubMsg(t, &evt)
			h.Add(msgs[i])
		}
		h.Flush(context.Background())

		assert.True(t, msgs[0].acked, "201 should be acked")
		assert.True(t, msgs[1].acked, "409 should be acked")
		assert.True(t, msgs[2].nacked, "500 should be nacked")
	})
}

func TestIsBulkItemSuccess(t *testing.T) {
	tests := []struct {
		name      string
		action    searchengine.ActionType
		status    int
		errorType string
		want      bool
	}{
		// 2xx is always success.
		{"index 200", searchengine.ActionIndex, 200, "", true},
		{"index 201", searchengine.ActionIndex, 201, "", true},
		{"delete 200", searchengine.ActionDelete, 200, "", true},
		{"update 200", searchengine.ActionUpdate, 200, "", true},

		// 409 is success only for externally-versioned writes (index, delete) —
		// external versioning rejected a stale write and the desired state is
		// already reached. Update 409 is a version_conflict_engine_exception
		// from concurrent writers; the painless script did NOT execute, so we
		// NAK to let JetStream redeliver and retry the scripted update.
		{"index 409", searchengine.ActionIndex, 409, "version_conflict_engine_exception", true},
		{"delete 409", searchengine.ActionDelete, 409, "version_conflict_engine_exception", true},
		{"update 409", searchengine.ActionUpdate, 409, "version_conflict_engine_exception", false},

		// Delete 404 benign path: doc already absent, no error block.
		{"delete 404 not_found (empty errorType)", searchengine.ActionDelete, 404, "", true},
		// Delete 404 fatal path: the target INDEX is missing — config error.
		{"delete 404 index_not_found", searchengine.ActionDelete, 404, "index_not_found_exception", false},

		// Update 404 benign path: doc missing (user-room remove on empty doc).
		{"update 404 document_missing", searchengine.ActionUpdate, 404, "document_missing_exception", true},
		// Update 404 fatal path: the target INDEX is missing — config error.
		{"update 404 index_not_found", searchengine.ActionUpdate, 404, "index_not_found_exception", false},
		// Update 404 with an unfamiliar error type → fail closed.
		{"update 404 unknown error type", searchengine.ActionUpdate, 404, "some_other_exception", false},

		// Index 404 is always a failure (indexing should create the doc).
		{"index 404 index_not_found", searchengine.ActionIndex, 404, "index_not_found_exception", false},
		{"index 404 empty error", searchengine.ActionIndex, 404, "", false},

		// Server errors are failures on every action.
		{"index 500", searchengine.ActionIndex, 500, "", false},
		{"delete 500", searchengine.ActionDelete, 500, "", false},
		{"update 500", searchengine.ActionUpdate, 500, "", false},
		{"index 503", searchengine.ActionIndex, 503, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isBulkItemSuccess(tt.action, searchengine.BulkResult{
				Status:    tt.status,
				ErrorType: tt.errorType,
			})
			assert.Equal(t, tt.want, got)
		})
	}
}

// stubCollection is a minimal Collection that emits a single action of the
// configured type. Only BuildAction is exercised by the handler tests; the
// rest of the interface methods return zero values.
type stubCollection struct {
	action searchengine.ActionType
}

func (c stubCollection) StreamConfig(string) jetstream.StreamConfig { return jetstream.StreamConfig{} }
func (c stubCollection) ConsumerName() string                       { return "stub" }
func (c stubCollection) FilterSubjects(string) []string             { return nil }
func (c stubCollection) TemplateName() string                       { return "" }
func (c stubCollection) TemplateBody() json.RawMessage              { return nil }
func (c stubCollection) BuildAction([]byte) ([]searchengine.BulkAction, error) {
	return []searchengine.BulkAction{{Action: c.action, Index: "stub", DocID: "id-1"}}, nil
}

type (
	stubDeleteCollection struct{ stubCollection }
	stubUpdateCollection struct{ stubCollection }
	stubIndexCollection  struct{ stubCollection }
)

func newStubDeleteCollection() stubDeleteCollection {
	return stubDeleteCollection{stubCollection{action: searchengine.ActionDelete}}
}
func newStubUpdateCollection() stubUpdateCollection {
	return stubUpdateCollection{stubCollection{action: searchengine.ActionUpdate}}
}
func newStubIndexCollection() stubIndexCollection {
	return stubIndexCollection{stubCollection{action: searchengine.ActionIndex}}
}

// fanOutCollection emits N ActionIndex actions per source message,
// simulating a bulk-invite style fan-out collection.
type fanOutCollection struct {
	actionsPerMessage int
}

func (c fanOutCollection) StreamConfig(string) jetstream.StreamConfig {
	return jetstream.StreamConfig{}
}
func (c fanOutCollection) ConsumerName() string           { return "fanout" }
func (c fanOutCollection) FilterSubjects(string) []string { return nil }
func (c fanOutCollection) TemplateName() string           { return "" }
func (c fanOutCollection) TemplateBody() json.RawMessage  { return nil }
func (c fanOutCollection) BuildAction([]byte) ([]searchengine.BulkAction, error) {
	actions := make([]searchengine.BulkAction, 0, c.actionsPerMessage)
	for i := 0; i < c.actionsPerMessage; i++ {
		actions = append(actions, searchengine.BulkAction{
			Action: searchengine.ActionIndex,
			Index:  "fanout",
			DocID:  fmt.Sprintf("id-%d", i),
			Doc:    json.RawMessage(`{}`),
		})
	}
	return actions, nil
}

func TestHandler_Flush_404OnDeleteAndUpdate(t *testing.T) {
	t.Run("404 on delete with no error block (doc missing) is acked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		// ES delete-on-missing-doc sets status=404 + result=not_found with
		// NO error block — ErrorType stays empty in our adapter mapping.
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{Status: 404, ErrorType: "", Error: ""}}, nil)

		coll := newStubDeleteCollection()
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.True(t, msg.acked, "404 on delete with no error block should be acked (already deleted)")
		assert.False(t, msg.nacked)
	})

	t.Run("404 on delete with index_not_found is NACKED", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{
				Status:    404,
				ErrorType: "index_not_found_exception",
				Error:     "no such index [spotlight-site-a-v1-chat]",
			}}, nil)

		coll := newStubDeleteCollection()
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.False(t, msg.acked, "404 on delete with index_not_found is a fatal config error")
		assert.True(t, msg.nacked)
	})

	t.Run("404 on update with document_missing_exception is acked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{
				Status:    404,
				ErrorType: "document_missing_exception",
				Error:     "[charlie]: document missing",
			}}, nil)

		coll := newStubUpdateCollection()
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.True(t, msg.acked, "404 on update with document_missing_exception should be acked")
		assert.False(t, msg.nacked)
	})

	t.Run("404 on update with index_not_found is NACKED", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{
				Status:    404,
				ErrorType: "index_not_found_exception",
				Error:     "no such index [user-room-site-a]",
			}}, nil)

		coll := newStubUpdateCollection()
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.False(t, msg.acked, "404 on update with index_not_found is a fatal config error")
		assert.True(t, msg.nacked)
	})

	t.Run("404 on index is nacked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(1)).
			Return([]searchengine.BulkResult{{
				Status:    404,
				ErrorType: "index_not_found_exception",
				Error:     "no such index",
			}}, nil)

		coll := newStubIndexCollection()
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.False(t, msg.acked, "404 on index should be nacked")
		assert.True(t, msg.nacked)
	})
}

// TestHandler_FanOut exercises the per-message action-range bookkeeping with
// a fan-out collection (one message → N actions). The handler must:
//   - track message count and action count independently
//   - ack the source message only when ALL of its actions succeeded
//   - nak the source message if ANY action failed
func TestHandler_FanOut(t *testing.T) {
	coll := fanOutCollection{actionsPerMessage: 3}

	t.Run("message count and action count diverge", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		// One message produces 3 actions.
		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)

		assert.Equal(t, 1, h.MessageCount(), "one source message buffered")
		assert.Equal(t, 3, h.ActionCount(), "three actions produced by fan-out")
	})

	t.Run("all fan-out actions succeed → source message acked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(3)).
			Return([]searchengine.BulkResult{
				{Status: 201},
				{Status: 201},
				{Status: 201},
			}, nil)

		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.True(t, msg.acked, "all 3 fan-out actions succeeded → source message acked")
		assert.False(t, msg.nacked)
		assert.Equal(t, 0, h.MessageCount())
		assert.Equal(t, 0, h.ActionCount())
	})

	t.Run("any fan-out action fails → source message nakked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(3)).
			Return([]searchengine.BulkResult{
				{Status: 201}, // success
				{Status: 500}, // failure — second action in the fan-out
				{Status: 201}, // success (wouldn't matter, one failure is enough)
			}, nil)

		h := NewHandler(store, coll, 500)
		msg := &stubMsg{data: []byte(`{}`)}
		h.Add(msg)
		h.Flush(context.Background())

		assert.False(t, msg.acked)
		assert.True(t, msg.nacked, "one failed fan-out action should nak the whole source message")
	})

	t.Run("multiple messages, one fan-out fails → only that source nakked", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		store := NewMockStore(ctrl)
		// Two messages × 3 actions/message = 6 bulk actions.
		// Message 0 actions (index 0-2) all succeed.
		// Message 1 actions (index 3-5) — middle one fails.
		store.EXPECT().
			Bulk(gomock.Any(), gomock.Len(6)).
			Return([]searchengine.BulkResult{
				{Status: 201}, {Status: 201}, {Status: 201}, // msg 0
				{Status: 201}, {Status: 500}, {Status: 201}, // msg 1
			}, nil)

		h := NewHandler(store, coll, 500)
		msg0 := &stubMsg{data: []byte(`{}`)}
		msg1 := &stubMsg{data: []byte(`{}`)}
		h.Add(msg0)
		h.Add(msg1)
		require.Equal(t, 2, h.MessageCount())
		require.Equal(t, 6, h.ActionCount())
		h.Flush(context.Background())

		assert.True(t, msg0.acked, "msg 0's 3 actions all succeeded → acked")
		assert.False(t, msg0.nacked)
		assert.False(t, msg1.acked)
		assert.True(t, msg1.nacked, "msg 1 had one failing action → nakked")
	})
}
