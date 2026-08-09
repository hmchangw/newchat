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
)

// historyReply builds the {"messages": [...]} envelope history-service's
// msg.get.ids RPC actually replies with (models.GetMessagesByIDsResponse).
func historyReply(t *testing.T, msgs ...map[string]any) []byte {
	t.Helper()
	list := make([]map[string]any, 0, len(msgs))
	list = append(list, msgs...)
	b, err := json.Marshal(map[string]any{"messages": list})
	require.NoError(t, err)
	return b
}

// msg builds one entry of the reply using the real cassandra.Message wire
// field names (messageId, roomId, sender.id, threadParentId) rather than the
// flat id/userId/threadParentMessageId shape the original brief assumed.
func msg(id, roomID, senderID, parent string) map[string]any {
	return map[string]any{
		"messageId":      id,
		"roomId":         roomID,
		"sender":         map[string]string{"id": senderID},
		"threadParentId": parent,
	}
}

// requestIDs decodes the messageIds sent in a msg.get.ids request body, so
// tests can assert on exactly what was batched.
func requestIDs(t *testing.T, data []byte) []string {
	t.Helper()
	var body struct {
		MessageIDs []string `json:"messageIds"`
	}
	require.NoError(t, json.Unmarshal(data, &body))
	return body.MessageIDs
}

func TestReadback_AllPresent_NoViolations(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs)
}

func TestReadback_Missing_AfterRetries_IsPersistenceMiss(t *testing.T) {
	calls := 0
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		calls++
		return historyReply(t), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMiss, vs[0].Kind)
	assert.Equal(t, 3, calls, "must exhaust the configured attempts before declaring loss")
}

func TestReadback_LateArrival_IsNotAViolation(t *testing.T) {
	calls := 0
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		calls++
		if calls < 2 {
			return historyReply(t), nil // message-worker hasn't caught up yet
		}
		return historyReply(t, msg("m1", "r1", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs, "storage lag is not storage loss")
}

// TestReadback_RoomFieldNotCompared_NoViolation pins the deliberate absence of
// a RoomID check in compareStored: history-service's msg.get.ids scopes its
// fetch to the roomID baked into the subject and silently drops any row whose
// actual RoomID doesn't match, so a wrong-room write can only ever come back
// absent, never present-with-a-mismatched-roomId. A reply that (in this test
// only, to exercise the code path) carries a different RoomID must therefore
// NOT be reported as persistence_mismatch.
func TestReadback_RoomFieldNotCompared_NoViolation(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "WRONG", "u-1", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs, "roomId is not compared -- history-service already scopes the fetch by room")
}

func TestReadback_WrongSender_IsMismatch(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "SOMEONE-ELSE", "")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMismatch, vs[0].Kind)
}

func TestReadback_WrongThreadParent_IsMismatch(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return historyReply(t, msg("m1", "r1", "u-1", "OTHER-PARENT")), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1", ThreadParentID: "p1"},
	})
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, KindPersistenceMismatch, vs[0].Kind)
}

func TestReadback_QueryError_ReturnsErrorNotViolation(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return nil, errors.New("timeout")
	}
	rb := NewReadback(req, "site-test", 2, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.Error(t, err, "an unreachable history-service is INCONCLUSIVE, never FAIL")
	assert.Empty(t, vs)
}

// TestReadback_ErrcodeEnvelopeReply_ReturnsErrorNotViolation pins Critical-1:
// a request-level failure at history-service (e.g. the probe's sender account
// no longer subscribed to the room, or a subscriptions-store hiccup collapsed
// to Internal) replies on the same reply subject with an ordinary errcode
// envelope body and err == nil from requestFn. That must never be read as
// "zero messages found" -- it says nothing about whether the write happened,
// so it must surface as a query error, not a persistence_miss violation.
func TestReadback_ErrcodeEnvelopeReply_ReturnsErrorNotViolation(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		b, err := json.Marshal(map[string]string{"code": "forbidden", "error": "not subscribed to room"})
		require.NoError(t, err)
		return b, nil // NATS-level success; the failure is in the payload
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.Error(t, err, "an errcode reply is INCONCLUSIVE, never a false persistence_miss")
	assert.Empty(t, vs)
}

// TestReadback_MalformedReply_ReturnsErrorNotViolation covers a reply that is
// neither a valid historyResponse nor a valid errcode envelope: decode
// failure must be an error, not a violation.
func TestReadback_MalformedReply_ReturnsErrorNotViolation(t *testing.T) {
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return []byte("not-json"), nil
	}
	rb := NewReadback(req, "site-test", 2, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.Error(t, err, "a malformed reply must not be treated as zero messages found")
	assert.Empty(t, vs)
}

// TestReadback_ContextCancelledMidRetry_AbortsWithoutBurningBudget pins the
// mid-retry cancellation branch specifically: a context cancelled between
// attempts must abort during the backoff wait rather than spend the rest of
// the configured retry budget. The backoff is deliberately long so the select
// in verifyRoom can only be won by ctx.Done(), never by time.After.
func TestReadback_ContextCancelledMidRetry_AbortsWithoutBurningBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	req := func(_ context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		calls++
		if calls == 1 {
			cancel() // cancel once the first attempt has been made, still nothing found
		}
		return historyReply(t), nil
	}
	rb := NewReadback(req, "site-test", 5, time.Hour)

	vs, err := rb.Verify(ctx, "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
	})
	require.Error(t, err)
	assert.Empty(t, vs)
	assert.Equal(t, 1, calls, "must abort during the backoff wait, not burn the remaining retry budget")
}

func TestReadback_BatchesByRoom(t *testing.T) {
	queried := map[string]int{}
	req := func(_ context.Context, subj string, _ []byte, _ time.Duration) ([]byte, error) {
		queried[subj]++
		return historyReply(t,
			msg("m1", "r1", "u-1", ""),
			msg("m2", "r1", "u-1", ""),
			msg("m3", "r1", "u-1", ""),
		), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", []ReadbackTarget{
		{MsgID: "m1", RoomID: "r1", SenderID: "u-1"},
		{MsgID: "m2", RoomID: "r1", SenderID: "u-1"},
		{MsgID: "m3", RoomID: "r1", SenderID: "u-1"},
	})
	require.NoError(t, err)
	assert.Empty(t, vs)
	assert.Len(t, queried, 1, "three probes in one room must cost one query")
}

// TestReadback_ChunksAt100 pins history-service's maxGetByIDsBatchSize=100: a
// room with more probes than the cap must be split into multiple msg.get.ids
// requests, and every probe must still be verified.
func TestReadback_ChunksAt100(t *testing.T) {
	const n = 150
	targets := make([]ReadbackTarget, n)
	for i := 0; i < n; i++ {
		targets[i] = ReadbackTarget{MsgID: fmt.Sprintf("m-%03d", i), RoomID: "r1", SenderID: "u-1"}
	}

	var calls int
	var chunkSizes []int
	req := func(_ context.Context, _ string, data []byte, _ time.Duration) ([]byte, error) {
		calls++
		ids := requestIDs(t, data)
		chunkSizes = append(chunkSizes, len(ids))
		require.LessOrEqual(t, len(ids), 100, "must not exceed history-service's maxGetByIDsBatchSize")

		msgs := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			msgs = append(msgs, msg(id, "r1", "u-1", ""))
		}
		return historyReply(t, msgs...), nil
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	vs, err := rb.Verify(context.Background(), "user-1", targets)
	require.NoError(t, err)
	assert.Empty(t, vs)
	assert.Equal(t, 2, calls, "150 probes must chunk into two requests of at most 100 ids")
	assert.ElementsMatch(t, []int{100, 50}, chunkSizes)
}

func TestReadback_ContextCancelled_Aborts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := func(c context.Context, _ string, _ []byte, _ time.Duration) ([]byte, error) {
		return nil, c.Err()
	}
	rb := NewReadback(req, "site-test", 3, time.Millisecond)

	_, err := rb.Verify(ctx, "user-1", []ReadbackTarget{{MsgID: "m1", RoomID: "r1", SenderID: "u-1"}})
	require.Error(t, err)
}
