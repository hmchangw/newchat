package main

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

// fakeMsg is a sentinel jetstream.Msg used only for identity comparison. It
// embeds the interface (nil) so it satisfies jetstream.Msg without implementing
// every method; the tests never call those methods.
type fakeMsg struct {
	jetstream.Msg
	id string
}

// fakeOtelBatch is a stand-in oteljetstream.MessageBatch yielding a fixed slice.
type fakeOtelBatch struct {
	msgs []oteljetstream.Msg
}

func (f fakeOtelBatch) Messages() <-chan oteljetstream.Msg {
	ch := make(chan oteljetstream.Msg, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func (f fakeOtelBatch) Error() error { return nil }

// fakeOtelConsumer embeds oteljetstream.Consumer (nil) and overrides Fetch.
type fakeOtelConsumer struct {
	oteljetstream.Consumer
	batch oteljetstream.MessageBatch
	err   error
}

func (f fakeOtelConsumer) Fetch(int, ...jetstream.FetchOpt) (oteljetstream.MessageBatch, error) {
	return f.batch, f.err
}

// fakeRawBatch is a stand-in jetstream.MessageBatch yielding a fixed slice.
type fakeRawBatch struct {
	msgs []jetstream.Msg
}

func (f fakeRawBatch) Messages() <-chan jetstream.Msg {
	ch := make(chan jetstream.Msg, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func (f fakeRawBatch) Error() error { return nil }

// fakeRawConsumer embeds jetstream.Consumer (nil) and overrides Fetch.
type fakeRawConsumer struct {
	jetstream.Consumer
	batch jetstream.MessageBatch
	err   error
}

func (f fakeRawConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return f.batch, f.err
}

func TestOtelConsumerAdapter_Fetch_UnwrapsRawMsgInOrder(t *testing.T) {
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	m3 := &fakeMsg{id: "3"}
	adapter := otelConsumerAdapter{c: fakeOtelConsumer{batch: fakeOtelBatch{msgs: []oteljetstream.Msg{
		{Msg: m1}, {Msg: m2}, {Msg: m3},
	}}}}

	batch, err := adapter.Fetch(10)
	require.NoError(t, err)

	var got []jetstream.Msg
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []jetstream.Msg{m1, m2, m3}, got)
}

func TestOtelConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := otelConsumerAdapter{c: fakeOtelConsumer{err: wantErr}}

	_, err := adapter.Fetch(10)
	require.ErrorIs(t, err, wantErr)
}

func TestRawConsumerAdapter_Fetch_PassesThroughRawMsg(t *testing.T) {
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	adapter := rawConsumerAdapter{c: fakeRawConsumer{batch: fakeRawBatch{msgs: []jetstream.Msg{m1, m2}}}}

	batch, err := adapter.Fetch(10)
	require.NoError(t, err)

	var got []jetstream.Msg
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []jetstream.Msg{m1, m2}, got)
}

func TestRawConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := rawConsumerAdapter{c: fakeRawConsumer{err: wantErr}}

	_, err := adapter.Fetch(10)
	require.ErrorIs(t, err, wantErr)
}
