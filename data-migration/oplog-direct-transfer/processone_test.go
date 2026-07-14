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
)

// fakeJSMsg is a minimal jetstream.Msg for disposition testing — embeds the interface (unused
// methods panic) and overrides only Data/Headers/Subject/Metadata and the three ack flavors.
type fakeJSMsg struct {
	jetstream.Msg
	data         []byte
	numDelivered uint64
	acked        bool
	termed       bool
	nakDelays    []time.Duration
}

func (m *fakeJSMsg) Data() []byte         { return m.data }
func (m *fakeJSMsg) Headers() nats.Header { return nil }
func (m *fakeJSMsg) Subject() string      { return "chat.migration.oplog.site1.rocketchat_avatar.insert" }
func (m *fakeJSMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: m.numDelivered}, nil
}
func (m *fakeJSMsg) Ack() error  { m.acked = true; return nil }
func (m *fakeJSMsg) Term() error { m.termed = true; return nil }
func (m *fakeJSMsg) NakWithDelay(d time.Duration) error {
	m.nakDelays = append(m.nakDelays, d)
	return nil
}

func TestProcessOne_Insert_Acks(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "insert", Collection: testColl,
		DocumentKey: json.RawMessage(`{"_id":"a1"}`), FullDocument: json.RawMessage(`{"_id":"a1"}`)}
	data, _ := json.Marshal(ev)
	m := &fakeJSMsg{data: data}
	processOne(context.Background(), h, m, nil, 1000)
	require.Len(t, tgt.upserts, 1)
	assert.True(t, m.acked)
	assert.False(t, m.termed)
}

func TestProcessOne_BadJSON_Terms(t *testing.T) {
	tgt := &fakeTarget{}
	h := newTestHandler(tgt, &fakeLookup{})
	m := &fakeJSMsg{data: []byte(`{bad`)}
	processOne(context.Background(), h, m, nil, 1000)
	assert.True(t, m.termed)
	assert.False(t, m.acked)
}

func TestProcessOne_TargetError_Naks(t *testing.T) {
	tgt := &fakeTarget{deleteErr: errors.New("mongo down")}
	h := newTestHandler(tgt, &fakeLookup{})
	ev := oplogEvent{Op: "delete", Collection: testColl, DocumentKey: json.RawMessage(`{"_id":"a1"}`)}
	data, _ := json.Marshal(ev)
	m := &fakeJSMsg{data: data}
	processOne(context.Background(), h, m, nil, 1000)
	require.Len(t, m.nakDelays, 1)
	assert.Equal(t, 2*time.Second, m.nakDelays[0])
}
