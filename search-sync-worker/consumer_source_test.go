package main

import (
	"context"
	"errors"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// fakeMsg is a sentinel jetstream.Msg used only for identity comparison. It
// embeds the interface (nil) so it satisfies jetstream.Msg without implementing
// every method; the tests never call those methods.
type fakeMsg struct {
	jetstream.Msg
	id string
}

// fakeO11yBatch is a stand-in o11ynats.MessageBatch yielding a fixed slice.
type fakeO11yBatch struct {
	msgs []o11ynats.FetchedMessage
}

func (f fakeO11yBatch) Messages() <-chan o11ynats.FetchedMessage {
	ch := make(chan o11ynats.FetchedMessage, len(f.msgs))
	for _, m := range f.msgs {
		ch <- m
	}
	close(ch)
	return ch
}

func (f fakeO11yBatch) Error() error { return nil }

// fakeO11yConsumer embeds o11ynats.Consumer (nil) and overrides Fetch.
type fakeO11yConsumer struct {
	o11ynats.Consumer
	batch o11ynats.MessageBatch
	err   error
}

func (f fakeO11yConsumer) Fetch(context.Context, int, ...jetstream.FetchOpt) (o11ynats.MessageBatch, error) {
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

func TestO11yConsumerAdapter_Fetch_PassesThroughFetchedMessagesInOrder(t *testing.T) {
	ctx1 := context.WithValue(context.Background(), testContextKey("id"), "ctx1")
	ctx2 := context.WithValue(context.Background(), testContextKey("id"), "ctx2")
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	m3 := &fakeMsg{id: "3"}
	adapter := o11yConsumerAdapter{c: fakeO11yConsumer{batch: fakeO11yBatch{msgs: []o11ynats.FetchedMessage{
		{Ctx: ctx1, Msg: m1}, {Ctx: ctx2, Msg: m2}, {Ctx: ctx1, Msg: m3},
	}}}}

	batch, err := adapter.Fetch(context.Background(), 10)
	require.NoError(t, err)

	var got []o11ynats.FetchedMessage
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []o11ynats.FetchedMessage{
		{Ctx: ctx1, Msg: m1}, {Ctx: ctx2, Msg: m2}, {Ctx: ctx1, Msg: m3},
	}, got)
}

func TestO11yConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := o11yConsumerAdapter{c: fakeO11yConsumer{err: wantErr}}

	_, err := adapter.Fetch(context.Background(), 10)
	require.ErrorIs(t, err, wantErr)
}

func TestRawConsumerAdapter_Fetch_PassesThroughRawMsg(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey("id"), "raw")
	m1 := &fakeMsg{id: "1"}
	m2 := &fakeMsg{id: "2"}
	adapter := rawConsumerAdapter{c: fakeRawConsumer{batch: fakeRawBatch{msgs: []jetstream.Msg{m1, m2}}}}

	batch, err := adapter.Fetch(ctx, 10)
	require.NoError(t, err)

	var got []o11ynats.FetchedMessage
	for m := range batch.Messages() {
		got = append(got, m)
	}
	require.Equal(t, []o11ynats.FetchedMessage{
		{Ctx: ctx, Msg: m1}, {Ctx: ctx, Msg: m2},
	}, got)
}

func TestRawConsumerAdapter_Fetch_ReturnsError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	adapter := rawConsumerAdapter{c: fakeRawConsumer{err: wantErr}}

	_, err := adapter.Fetch(context.Background(), 10)
	require.ErrorIs(t, err, wantErr)
}

type testContextKey string
