package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/model"
)

// fakeJSMsg is a minimal jetstream.Msg for disposition testing — it embeds the interface (unused
// methods panic) and overrides only Data and the three ack flavors processOne uses.
type fakeJSMsg struct {
	jetstream.Msg
	data         []byte
	numDelivered uint64
	metaErr      error
	acked        bool
	termed       bool
	nakDelays    []time.Duration
}

func (m *fakeJSMsg) Data() []byte { return m.data }

// Headers/Subject feed natsutil.StampRequestID at the top of processOne; nil/"" → a fresh id is minted.
func (m *fakeJSMsg) Headers() nats.Header { return nil }
func (m *fakeJSMsg) Subject() string      { return "chat.migration.oplog.site1.rocketchat_message.insert" }

func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}

func (m *fakeJSMsg) Ack() error {
	m.acked = true
	return nil
}

func (m *fakeJSMsg) Term() error {
	m.termed = true
	return nil
}

func (m *fakeJSMsg) NakWithDelay(d time.Duration) error {
	m.nakDelays = append(m.nakDelays, d)
	return nil
}

// errHistory returns an error from Edit/Delete to drive the transient (Nak) disposition.
type errHistory struct{ err error }

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (e errHistory) Edit(_ context.Context, _ model.MigrationEditRequest) error { return e.err }

//nolint:gocritic // value param required to satisfy the historyClient interface.
func (e errHistory) Delete(_ context.Context, _ model.MigrationDeleteRequest) error { return e.err }

//nolint:gocritic // ev passed by value in a test helper; mirrors the handler's oplogEvent signature.
func mustJSON(t *testing.T, ev oplogEvent) []byte {
	t.Helper()
	b, err := json.Marshal(ev)
	require.NoError(t, err)
	return b
}

func TestProcessOne_Dispositions(t *testing.T) {
	const maxDeliver = 1000
	const deleteMaxDeliver = 60
	tests := []struct {
		name         string
		data         []byte
		numDelivered uint64
		handler      func(t *testing.T) *handler
		wantAck      bool
		wantTerm     bool
		wantNakLen   int
		wantNakWait  time.Duration
	}{
		{
			name: "valid insert acks",
			data: mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "insert.json")}),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantAck: true,
		},
		{
			// A deliberate skip (system message) is Acked — same disposition as success — but the
			// handler returns errSkipped so processOne does NOT count it as processed.
			name: "system message skip acks (not nak/term)",
			data: mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "insert", FullDocument: loadDoc(t, "system.json")}),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantAck: true,
		},
		{
			// An unknown op is skipped → Ack, not Nak/Term.
			name: "unknown op skip acks",
			data: mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "rename"}),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantAck: true,
		},
		{
			// A non-message collection is skipped → Ack.
			name: "non-message collection skip acks",
			data: mustJSON(t, oplogEvent{Collection: "users", Op: "insert", FullDocument: []byte(`{}`)}),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantAck: true,
		},
		{
			// A present-but-corrupt fullDocument (valid JSON envelope, malformed inner doc) is
			// poison → Term. Built as a raw envelope so the bad doc survives marshaling.
			name: "poison insert terms",
			data: []byte(`{"coll":"rocketchat_message","op":"insert","fullDocument":{"ts":"not-a-date"}}`),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantTerm: true,
		},
		{
			name: "transient history failure naks",
			data: mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: json.RawMessage(`{"_id":"abc123def456ghi78"}`)}),
			handler: func(t *testing.T) *handler {
				look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
				return newTestHandler(&recordPublisher{}, errHistory{err: errors.New("history down")}, look)
			},
			wantNakLen:  1,
			wantNakWait: 2 * time.Second,
		},
		{
			name: "undecodable message body terms",
			data: []byte(`{not valid json`),
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, &recordHistory{}, fakeLookup{})
			},
			wantTerm: true,
		},
		{
			// Same transient failure as above, but at the delivery cap: instead of a silent
			// Nak that JetStream won't redeliver, processOne Terms it (exhaustion → visible drop).
			name:         "transient failure at MaxDeliver terms (exhaustion)",
			data:         mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: json.RawMessage(`{"_id":"abc123def456ghi78"}`)}),
			numDelivered: maxDeliver,
			handler: func(t *testing.T) *handler {
				look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
				return newTestHandler(&recordPublisher{}, errHistory{err: errors.New("history down")}, look)
			},
			wantTerm:   true,
			wantNakLen: 0,
		},
		{
			// A delete exhausts at the SHORTER delete cap (a foreign hard-delete never converges;
			// the local race needs only seconds, so no need to churn to the global cap).
			name:         "delete at DELETE_MAX_DELIVER terms early",
			data:         mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "delete", DocumentKey: json.RawMessage(`{"_id":"abc123def456ghi78"}`), ClusterTime: 1700000000000}),
			numDelivered: deleteMaxDeliver,
			handler: func(t *testing.T) *handler {
				return newTestHandler(&recordPublisher{}, errHistory{err: errors.New("not yet persisted")}, fakeLookup{})
			},
			wantTerm:   true,
			wantNakLen: 0,
		},
		{
			// An UPDATE at the same delivery count still Naks — the shorter cap is delete-only,
			// the global MaxDeliver still applies to non-delete ops.
			name:         "update at delete-cap count still naks (op-scoped cap)",
			data:         mustJSON(t, oplogEvent{Collection: "rocketchat_message", Op: "update", DocumentKey: json.RawMessage(`{"_id":"abc123def456ghi78"}`)}),
			numDelivered: deleteMaxDeliver,
			handler: func(t *testing.T) *handler {
				look := fakeLookup{"abc123def456ghi78": loadDoc(t, "edit.json")}
				return newTestHandler(&recordPublisher{}, errHistory{err: errors.New("history down")}, look)
			},
			wantNakLen:  1,
			wantNakWait: 2 * time.Second,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := tc.handler(t)
			m := &fakeJSMsg{data: tc.data, numDelivered: tc.numDelivered}
			processOne(context.Background(), h, m, nil, maxDeliver, deleteMaxDeliver)
			assert.Equal(t, tc.wantAck, m.acked, "ack")
			assert.Equal(t, tc.wantTerm, m.termed, "term")
			require.Len(t, m.nakDelays, tc.wantNakLen, "nak count")
			if tc.wantNakLen > 0 {
				assert.Equal(t, tc.wantNakWait, m.nakDelays[0], "nak delay")
			}
		})
	}
}

func TestIsFinalDelivery(t *testing.T) {
	// migration.IsFinalDelivery takes (numDelivered uint64, maxDeliver int) directly;
	// metadata extraction (and the error-tolerant fallback to 0) now lives in processOne.
	assert.True(t, migration.IsFinalDelivery(1000, 1000), "at the cap")
	assert.True(t, migration.IsFinalDelivery(1001, 1000), "past the cap")
	assert.False(t, migration.IsFinalDelivery(999, 1000), "below the cap")
	assert.False(t, migration.IsFinalDelivery(5000, 0), "maxDeliver<=0 means unlimited")
	// A metadata error makes processOne pass numDelivered=0, which is never ≥ maxDeliver > 0 → not final.
	assert.False(t, migration.IsFinalDelivery(0, 1000), "metadata error → processOne passes 0 → prefer Nak")
}
