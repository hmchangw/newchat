package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
)

// fakeCQLError is a gocql.RequestError double. gocql v1.7.0 returns its
// unexported errorFrame value for the codes these tests care about (Invalid,
// Unauthorized, Config, …) — there is no exported concrete type to construct —
// so the failures are built through a local implementation of the exported
// RequestError interface that cassutil.ClassifyCQL matches on.
type fakeCQLError struct {
	code int
	msg  string
}

func (e fakeCQLError) Code() int       { return e.code }
func (e fakeCQLError) Message() string { return e.msg }
func (e fakeCQLError) Error() string   { return e.msg }

// fakeJetStreamMsg is a jetstream.Msg double recording ack/nak dispositions.
type fakeJetStreamMsg struct {
	jetstream.Msg
	subject      string
	data         []byte
	numDelivered uint64
	streamSeq    uint64
	acked        bool
	naked        bool
	nakDelay     time.Duration
	// ackErr forces the Ack network call to fail, which means nothing was destroyed.
	ackErr error
}

func (m *fakeJetStreamMsg) Subject() string { return m.subject }
func (m *fakeJetStreamMsg) Data() []byte    { return m.data }
func (m *fakeJetStreamMsg) Ack() error {
	if m.ackErr != nil {
		return m.ackErr
	}
	m.acked = true
	return nil
}
func (m *fakeJetStreamMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *fakeJetStreamMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		NumDelivered: m.numDelivered,
		Sequence:     jetstream.SequencePair{Stream: m.streamSeq},
		Timestamp:    time.Now(),
	}, nil
}

// metadataErrMsg is a message whose delivery count cannot be read, so there is
// nothing to measure the retry window against.
type metadataErrMsg struct{ fakeJetStreamMsg }

func (m *metadataErrMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return nil, errors.New("metadata unavailable")
}

// newDegradeStateHandler builds a Handler, through the real constructor, whose
// degrade tracker reports the given state over an in-memory marker store. Shared by
// the settle, drop-cap, quote and thread-badge tests — pass nil for the store and
// noopPublish for the publish func when the test does not exercise them.
//
// For the settle tests the marker no longer selects the retry policy — the error
// class does — so the degraded argument there exists only to prove the marker is
// NOT consulted.
func newDegradeStateHandler(t *testing.T, store Store, publish PublishFunc, degraded bool, drop dropPolicy) *Handler {
	t.Helper()
	m, err := newMetrics()
	require.NoError(t, err)
	now := time.Unix(1700000000, 0).UTC()
	tr := newDegradeTracker(&fakeDegradeStore{}, "site-a",
		func(context.Context) (uint64, uint64, error) { return 1, 0, nil }, m,
		func() time.Time { return now })
	if degraded {
		tr.OnWriteFailure(context.Background())
	}
	return NewHandler(store, nil, nil, "site-a", publish, m, tr, drop)
}

func noopPublish(context.Context, string, []byte, string) error { return nil }

// testDropPolicy is the give-up policy handler tests run with: the production
// default window, well above the accumulated backoff of the delivery counts those
// tests seed, so they exercise the retry path.
func testDropPolicy() dropPolicy {
	return newDropPolicy(time.Hour, true, 10, nil)
}

// testDegradeTracker returns a healthy tracker over an in-memory marker store, for
// handler tests that do not exercise the retry policy. settle dereferences the
// tracker, so it must never be nil.
func testDegradeTracker() *degradeTracker {
	return newDegradeTracker(&fakeDegradeStore{}, "site-a",
		func(context.Context) (uint64, uint64, error) { return 0, 0, nil }, nil, nil)
}

// historyErr wraps a CQL failure the way handler.go's persist path does: tagged as
// a history write error, inside the store's own context wrapping.
func historyErr(cause error) error {
	return fmt.Errorf("save message m-1: %w", historyWriteError{fmt.Errorf("gocql: %w", cause)})
}

// requestClassErr / infraClassErr are the two inputs the whole policy turns on.
func requestClassErr() error {
	return historyErr(fakeCQLError{code: gocql.ErrCodeInvalid, msg: "Invalid mutation size"})
}

func infraClassErr() error {
	return historyErr(gocql.ErrNoConnections)
}

func TestHandler_Settle(t *testing.T) {
	// With jsretry.DefaultBackoff a message has accumulated 36s of retry wait by
	// its 4th delivery, so a 10s window is crossed there and not on the 3rd (6s).
	shortWindow := newDropPolicy(10*time.Second, true, 10, nil)

	t.Run("success acks", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, testDropPolicy())
		msg := &fakeJetStreamMsg{numDelivered: 1}
		h.settle(context.Background(), msg, nil)
		assert.True(t, msg.acked)
		assert.False(t, msg.naked)
	})

	t.Run("permanent decode failure acks", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, testDropPolicy())
		msg := &fakeJetStreamMsg{numDelivered: 1}
		h.settle(context.Background(), msg, errcode.Permanent(errcode.BadRequest("malformed message event")))
		assert.True(t, msg.acked, "poison bytes are dropped ahead of every other error case — unchanged")
		assert.False(t, msg.naked)
	})

	t.Run("non-history failure naks indefinitely and never drops", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 500}
		h.settle(context.Background(), msg, errors.New("lookup user: mongo down"))

		assert.True(t, msg.naked)
		assert.False(t, msg.acked,
			"a Mongo outage must not drop the live feed: nothing sets the degraded marker for it, so history-service would keep reporting complete history over the hole")
		assert.False(t, h.degrade.Degraded(), "a non-history failure says nothing about history")
	})

	t.Run("infra-class failure naks indefinitely", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 500}
		h.settle(context.Background(), msg, infraClassErr())
		assert.True(t, msg.naked)
		assert.False(t, msg.acked)
		// The shared schedule jitters each delay into [half, full] of its entry, so
		// the tail is a range rather than a point. What matters here is that an
		// infra-class failure keeps NAKing on the tail, not which draw it got.
		assert.GreaterOrEqual(t, msg.nakDelay, 5*time.Minute, "backoff tail, jittered floor")
		assert.LessOrEqual(t, msg.nakDelay, 10*time.Minute, "backoff tail, jittered ceiling")
		assert.True(t, h.degrade.Degraded(), "a history write failure marks the site degraded")
	})

	t.Run("request-class failure naks inside the retry window", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 3} // 6s accumulated, window is 10s
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.naked)
		assert.False(t, msg.acked)
	})

	t.Run("request-class failure drops past the retry window", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 4} // 36s accumulated, window is 10s
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.acked, "a message Cassandra rejects deterministically is retired")
		assert.False(t, msg.naked)
	})

	t.Run("the kill switch turns a drop back into a nak", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, false, 10, nil))
		msg := &fakeJetStreamMsg{numDelivered: 400}
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.naked, "HISTORY_DROP_ENABLED=false is the operator's brake on a migration-wide wave")
		assert.False(t, msg.acked)
	})

	t.Run("the degraded marker no longer selects the policy", func(t *testing.T) {
		// The marker read is what wedged the earlier park policy shut: a history
		// failure re-degrades the site, so a message's own failure destroyed the
		// evidence needed to condemn it. The error class answers directly.
		h := newDegradeStateHandler(t, nil, noopPublish, true, shortWindow)
		require.True(t, h.degrade.Degraded())
		msg := &fakeJetStreamMsg{numDelivered: 4}
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.acked, "a request-class failure is dropped on its own evidence, degraded or not")
	})

	t.Run("unknown delivery count retries instead of dropping", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(time.Nanosecond, true, 10, nil))
		msg := &metadataErrMsg{fakeJetStreamMsg{numDelivered: 99}}
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.naked, "without a delivery count there is no retry time to measure")
		assert.False(t, msg.acked)
	})

	t.Run("cassandra read failure marks the site degraded and naks", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		// The shape handler.go returns when the thread parent's createdAt cannot be
		// read: a Cassandra read on the persist path, tagged like the writes. It
		// carries no CQL code at all, so it must classify infra.
		readErr := fmt.Errorf("resolve thread parent createdAt: %w",
			historyWriteError{errors.New("gocql: no hosts available")})
		msg := &fakeJetStreamMsg{numDelivered: 1}
		h.settle(context.Background(), msg, readErr)

		assert.True(t, h.degrade.Degraded(),
			"a read failure at the onset of an outage must set the marker before any write has failed")
		assert.True(t, msg.naked)
		assert.False(t, msg.acked)
	})

	t.Run("a request-class failure does not degrade the site", func(t *testing.T) {
		// The marker is site-wide and drives incompleteSince for every room plus
		// thread-badge suppression. A request-class verdict is the classifier saying
		// "this one row is unwritable", which says nothing about the site's history.
		// Marking on it turns one bad message into a site-wide "history is incomplete".
		h := newDegradeStateHandler(t, nil, noopPublish, false, testDropPolicy())
		msg := &fakeJetStreamMsg{numDelivered: 2}
		h.settle(context.Background(), msg, requestClassErr())
		assert.True(t, msg.naked, "inside the window it still retries")
		assert.False(t, h.degrade.Degraded(),
			"one unwritable row must not tell every client on the site that history is incomplete")
	})

	t.Run("an untagged parent-not-yet-persisted failure naks", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 900}
		h.settle(context.Background(), msg,
			historyWriteError{errors.New("thread parent p-1 not yet persisted in messages_by_id")})
		assert.True(t, msg.naked, "a parent still replaying is infra class: unrecognised errors retry")
		assert.False(t, msg.acked)
	})

	t.Run("the production default window drops on the 11th delivery", func(t *testing.T) {
		// Pins the documented mapping: 1h of accumulated backoff is first reached
		// on delivery 11 (36s + 2m + 6 × 10m). This number moved from 34 when the
		// shared schedule grew a 10m tail, which is exactly why the config is a
		// duration and not a delivery count.
		under := newDegradeStateHandler(t, nil, noopPublish, false, testDropPolicy())
		msg10 := &fakeJetStreamMsg{numDelivered: 10}
		under.settle(context.Background(), msg10, requestClassErr())
		assert.True(t, msg10.naked)
		assert.False(t, msg10.acked)

		over := newDegradeStateHandler(t, nil, noopPublish, false, testDropPolicy())
		msg11 := &fakeJetStreamMsg{numDelivered: 11}
		over.settle(context.Background(), msg11, requestClassErr())
		assert.True(t, msg11.acked)
		assert.False(t, msg11.naked)
	})
}

// TestHandler_Settle_InfraClassNeverDrops is the regression guard that replaces the
// deleted park-path guard. Dropping is permanent destruction, and an outage is
// exactly when a message accumulates an enormous delivery count, so the one thing
// that must never regress is: no infra-class failure is ever acked, no matter how
// long it has been retrying or how small the window is.
func TestHandler_Settle_InfraClassNeverDrops(t *testing.T) {
	infraErrors := map[string]error{
		"no connections":     historyErr(gocql.ErrNoConnections),
		"connection closed":  historyErr(gocql.ErrConnectionClosed),
		"no response":        historyErr(gocql.ErrTimeoutNoResponse),
		"unavailable":        historyErr(fakeCQLError{code: gocql.ErrCodeUnavailable, msg: "unavailable"}),
		"overloaded":         historyErr(fakeCQLError{code: gocql.ErrCodeOverloaded, msg: "overloaded"}),
		"write timeout":      historyErr(fakeCQLError{code: gocql.ErrCodeWriteTimeout, msg: "write timeout"}),
		"read timeout":       historyErr(fakeCQLError{code: gocql.ErrCodeReadTimeout, msg: "read timeout"}),
		"bootstrapping":      historyErr(fakeCQLError{code: gocql.ErrCodeBootstrapping, msg: "bootstrapping"}),
		"server error":       historyErr(fakeCQLError{code: gocql.ErrCodeServer, msg: "server error"}),
		"unprepared":         historyErr(fakeCQLError{code: gocql.ErrCodeUnprepared, msg: "unprepared"}),
		"deadline exceeded":  historyErr(context.DeadlineExceeded),
		"unrecognised error": historyErr(errors.New("something nobody has classified")),
		// The two that would cost the entire feed if they were ever reclassified.
		"unauthorized (rotated credential)": historyErr(fakeCQLError{code: gocql.ErrCodeUnauthorized, msg: "unauthorized"}),
		"config error (missing keyspace)":   historyErr(fakeCQLError{code: gocql.ErrCodeConfig, msg: "keyspace does not exist"}),
	}

	// A one-nanosecond window makes every delivery past the deadline, so nothing but
	// the class itself can be keeping these messages alive. The drop rate cap is NOT
	// what holds them: settle's infra-class early return precedes the drop switch
	// entirely, so the limiter is never evaluated here at any cap value. A fresh policy
	// per subtest is for isolation only.
	policy := func() dropPolicy { return newDropPolicy(time.Nanosecond, true, 1000, nil) }

	for name, err := range infraErrors {
		t.Run(name, func(t *testing.T) {
			for _, numDelivered := range []uint64{1, 5, 500, 100000} {
				h := newDegradeStateHandler(t, nil, noopPublish, false, policy())
				msg := &fakeJetStreamMsg{numDelivered: numDelivered}
				h.settle(context.Background(), msg, err)

				require.False(t, msg.acked,
					"numDelivered=%d: an infra-class failure must never be dropped — acking here is the silent message loss this branch exists to remove", numDelivered)
				require.True(t, msg.naked, "numDelivered=%d: infra-class failures NAK forever", numDelivered)
			}
		})
	}
}

func TestDeliveriesToDrop(t *testing.T) {
	tests := []struct {
		name   string
		window time.Duration
		want   uint64
	}{
		{name: "the production default", window: time.Hour, want: 11},
		{name: "the first redelivery covers a sub-second window", window: time.Millisecond, want: 2},
		{name: "exactly the third delivery's accumulated wait", window: 6 * time.Second, want: 3},
		{name: "just past the third delivery's accumulated wait", window: 7 * time.Second, want: 4},
		{name: "a zero window drops on the first delivery", window: 0, want: 1},
		{name: "a negative window drops on the first delivery", window: -time.Second, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, deliveriesToDrop(tt.window))
		})
	}
}

func TestDeliveriesToDrop_SaturatesOnAnAbsurdWindow(t *testing.T) {
	// A window nobody would configure must not spin the startup log's estimate.
	assert.Equal(t, maxReportedDeliveries, deliveriesToDrop(time.Duration(1)<<62))
}

// TestHandler_Settle_DropLogNamesTheMessageWithoutLeakingIt guards the only log
// line that records a permanently destroyed message. It must be reconstructable
// from — and must never carry content, because a CQL "Invalid" message can echo the
// offending value back into the server log.
func TestHandler_Settle_DropLogNamesTheMessageWithoutLeakingIt(t *testing.T) {
	rec := installRecorder(t)

	h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, true, 10, nil))
	payload, err := json.Marshal(model.MessageEvent{
		Message: model.Message{ID: "m-poison", RoomID: "r-42", Content: "the secret message body"},
		SiteID:  "site-a",
	})
	require.NoError(t, err)

	msg := &fakeJetStreamMsg{subject: "chat.msg.canonical.site-a.created", data: payload, numDelivered: 40}
	h.settle(context.Background(), msg, requestClassErr())
	require.True(t, msg.acked)

	fields := rec.fieldsOf(slog.LevelError,
		"dropping message: Cassandra rejects it as a request error and the retry window has elapsed")
	require.NotNil(t, fields, "a destroyed message must leave exactly one ERROR record behind")
	assert.Equal(t, "m-poison", fields["message_id"])
	assert.Equal(t, "r-42", fields["room_id"])
	assert.Equal(t, "site-a", fields["site"])
	assert.Equal(t, "invalid", fields["drop_code"])
	assert.Equal(t, "5h52m36s", fields["retried_for"], "40 deliveries: 36s + 2m + 35 × 10m of accumulated backoff")
	assert.Equal(t, "10s", fields["retry_window"])

	for _, r := range rec.all() {
		assert.NotContains(t, fmt.Sprint(r), "the secret message body",
			"no log record on the drop path may carry message content")
	}
}

func TestDroppedIdentity(t *testing.T) {
	tests := []struct {
		name       string
		data       []byte
		wantID     string
		wantRoomID string
	}{
		{name: "reads the ids off a canonical event",
			data: []byte(`{"message":{"id":"m-1","roomId":"r-1","content":"hi"}}`), wantID: "m-1", wantRoomID: "r-1"},
		{name: "undecodable payload still yields a log line", data: []byte(`not json`)},
		{name: "empty payload", data: nil},
		{name: "event with no message", data: []byte(`{"siteId":"site-a"}`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, roomID := droppedIdentity(tt.data)
			assert.Equal(t, tt.wantID, id)
			assert.Equal(t, tt.wantRoomID, roomID)
		})
	}
}

// TestHandler_Settle_SuppressedDropsNameTheirReason covers the two ways a drop is
// withheld. An operator deciding whether it is safe to release the kill switch needs
// to tell them apart, so each carries the same bounded reason the metric is labelled
// with.
func TestHandler_Settle_SuppressedDropsNameTheirReason(t *testing.T) {
	tests := []struct {
		name       string
		policy     dropPolicy
		settles    int
		wantReason string
	}{
		{
			name:       "the kill switch reports disabled",
			policy:     newDropPolicy(10*time.Second, false, 10, nil),
			settles:    1,
			wantReason: dropSuppressedDisabled,
		},
		{
			// One drop allowed, so the second settle is the one the cap refuses.
			name:       "the rate cap reports rate_limited",
			policy:     newDropPolicy(10*time.Second, true, 1, nil),
			settles:    2,
			wantReason: dropSuppressedRateLimited,
		},
	}

	// A CQL "Invalid" message shaped the way Cassandra actually reports a rejected
	// value: the offending value is echoed back inside the error text, so the error
	// string is message content and must never be logged.
	leakyErr := historyErr(fakeCQLError{
		code: gocql.ErrCodeInvalid,
		msg:  `Invalid string constant (the secret message body) for "content" of type int`,
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := installRecorder(t)
			h := newDegradeStateHandler(t, nil, noopPublish, false, tt.policy)

			var msg *fakeJetStreamMsg
			for range tt.settles {
				rec.reset()
				msg = &fakeJetStreamMsg{numDelivered: 40, data: []byte(`{"message":{"id":"m-1","roomId":"r-1"}}`)}
				h.settle(context.Background(), msg, leakyErr)
			}

			assert.True(t, msg.naked, "a suppressed drop must retry")
			assert.False(t, msg.acked)
			fields := rec.fieldsOf(slog.LevelWarn, "history drop suppressed — retrying instead")
			require.NotNil(t, fields, "a suppressed drop must be visible in the log")
			assert.Equal(t, tt.wantReason, fields["reason"])
			assert.Equal(t, "invalid", fields["drop_code"])

			// The suppression paths run on every re-evaluation of the message, and a
			// schema-drift wave re-evaluates constantly, so an error field here would
			// pour untrusted CQL text into the server log at volume. drop_code above
			// carries the whole diagnostic without the content. This mirrors
			// TestHandler_Settle_DropLogNamesTheMessageWithoutLeakingIt for the drop log.
			assert.NotContains(t, fields, "error",
				"the suppression log must not carry the raw error: a CQL Invalid message echoes the offending value")
			for _, r := range rec.all() {
				assert.NotContains(t, fmt.Sprint(r), "the secret message body",
					"no record on the suppression path may carry message content")
			}
		})
	}
}

// TestHandler_Settle_FailedAckOnDropIsNotADrop guards the branch the drop counter now
// sits behind. A failed Ack leaves the message alive and JetStream redelivers it, so
// nothing was destroyed — the log must say so, and the destruction counter must not be
// incremented for a message that is still in the stream.
func TestHandler_Settle_FailedAckOnDropIsNotADrop(t *testing.T) {
	rec := installRecorder(t)
	h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, true, 10, nil))

	msg := &fakeJetStreamMsg{numDelivered: 40, data: []byte(`{"message":{"id":"m-1","roomId":"r-1"}}`),
		ackErr: errors.New("nats: connection closed")}
	h.settle(context.Background(), msg, requestClassErr())

	assert.False(t, msg.acked)
	fields := rec.fieldsOf(slog.LevelError, "failed to ack dropped message — it will be redelivered")
	require.NotNil(t, fields, "an unacked drop must be visible as a non-drop")
	assert.Equal(t, "m-1", fields["message_id"])
}

// TestHandler_Settle_OrphanedParent covers the second give-up path.
//
// MaxDeliver=-1 means JetStream never retires a message, so every error that can
// be permanent needs a deadline of its own or it holds a MaxAckPending slot for
// the life of the pod. A parent that will never land is exactly that: unlike a
// Mongo outage it does not resolve when a dependency recovers, and this service
// manufactures the condition itself — dropping a parent orphans every reply to it.
func TestHandler_Settle_OrphanedParent(t *testing.T) {
	// Same arithmetic as the CQL tests: 36s of accumulated backoff by the 4th
	// delivery, 6s by the 3rd, so a 10s window is crossed at 4 and not at 3.
	shortWindow := newDropPolicy(10*time.Second, true, 10, nil)
	orphan := func() error {
		return fmt.Errorf("resolve thread parent: %w", orphanedParentError{parentID: "p-1"})
	}

	t.Run("naks while the site is degraded, however long it has retried", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, true, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 5000}
		h.settle(context.Background(), msg, orphan())
		assert.True(t, msg.naked, "during a replay the parent is very likely still in the backlog")
		assert.False(t, msg.acked, "a drain must never be mistaken for a permanently missing parent")
	})

	t.Run("naks inside the window", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 3}
		h.settle(context.Background(), msg, orphan())
		assert.True(t, msg.naked)
		assert.False(t, msg.acked)
	})

	t.Run("drops once the window elapses on a healthy site", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &fakeJetStreamMsg{numDelivered: 4}
		h.settle(context.Background(), msg, orphan())
		assert.True(t, msg.acked, "a parent absent from a healthy site's history is not coming back")
		assert.False(t, msg.naked)
	})

	t.Run("the kill switch withholds the drop", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, false, 10, nil))
		msg := &fakeJetStreamMsg{numDelivered: 4}
		h.settle(context.Background(), msg, orphan())
		assert.True(t, msg.naked)
		assert.False(t, msg.acked, "HISTORY_DROP_ENABLED=false must brake every give-up path, not just the CQL one")
	})

	t.Run("shares the drop rate cap with the CQL path", func(t *testing.T) {
		// One budget bounds destruction per pod regardless of which give-up path
		// asked for it — the cap exists to bound loss, not to bound a cause.
		h := newDegradeStateHandler(t, nil, noopPublish, false, newDropPolicy(10*time.Second, true, 1, nil))
		first := &fakeJetStreamMsg{numDelivered: 4}
		h.settle(context.Background(), first, requestClassErr())
		require.True(t, first.acked, "the CQL drop consumes the pod's only slot")

		second := &fakeJetStreamMsg{numDelivered: 4}
		h.settle(context.Background(), second, orphan())
		assert.True(t, second.naked)
		assert.False(t, second.acked, "the orphan path must not have a second budget of its own")
	})

	t.Run("an unmeasurable message is never destroyed", func(t *testing.T) {
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		msg := &metadataErrMsg{fakeJetStreamMsg{numDelivered: 4}}
		h.settle(context.Background(), msg, orphan())
		assert.True(t, msg.naked)
		assert.False(t, msg.acked)
	})

	t.Run("never sets the site-wide marker", func(t *testing.T) {
		// A missing parent is a per-message data condition. Marking on it would make
		// every room report incompleteSince because one reply outran one parent.
		h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
		h.settle(context.Background(), &fakeJetStreamMsg{numDelivered: 4}, orphan())
		assert.False(t, h.degrade.Degraded())
	})

	t.Run("regression: the retry loop is bounded, so an ack-pending slot cannot leak", func(t *testing.T) {
		// The failure this whole path exists to prevent: with MaxDeliver=-1 an
		// unbounded NAK loop pins one of MaxAckPending slots for the life of the pod,
		// and enough of them stop the consumer delivering anything at all.
		for _, delivered := range []uint64{4, 100, 10_000, 1_000_000} {
			h := newDegradeStateHandler(t, nil, noopPublish, false, shortWindow)
			msg := &fakeJetStreamMsg{numDelivered: delivered}
			h.settle(context.Background(), msg, orphan())
			assert.True(t, msg.acked, "delivery %d must settle, not retry forever", delivered)
		}
	})
}
