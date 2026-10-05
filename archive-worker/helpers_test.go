package main

import (
	"bytes"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/auditarchive"
)

// fakeMsg is the minimal jetstream.Msg used by builder tests; only Metadata,
// Subject and Data matter here. Ack/Nak/Term record what the lane decided.
type fakeMsg struct {
	jetstream.Msg
	subject       string
	data          []byte
	seq           uint64
	stream        string
	acked, termed bool
	nakDelay      time.Duration
	naked         bool
	headers       nats.Header
	delivered     uint64
}

func (m *fakeMsg) Subject() string      { return m.subject }
func (m *fakeMsg) Data() []byte         { return m.data }
func (m *fakeMsg) Headers() nats.Header { return m.headers }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Stream: m.stream, Sequence: jetstream.SequencePair{Stream: m.seq}, NumDelivered: m.delivered}, nil
}
func (m *fakeMsg) Ack() error                         { m.acked = true; return nil }
func (m *fakeMsg) Term() error                        { m.termed = true; return nil }
func (m *fakeMsg) NakWithDelay(d time.Duration) error { m.naked, m.nakDelay = true, d; return nil }
func (m *fakeMsg) InProgress() error                  { return nil }

func testDEK() []byte { return bytes.Repeat([]byte{0x4b}, auditarchive.DEKSize) }
