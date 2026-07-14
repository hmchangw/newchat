package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/Marz32onE/instrumentation-go/otel-nats/oteljetstream"
)

func TestReadPreference(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    readpref.Mode
		wantErr bool
	}{
		{name: "primary", in: "primary", want: readpref.PrimaryMode},
		{name: "primary uppercase trimmed", in: "  PRIMARY  ", want: readpref.PrimaryMode},
		{name: "primaryPreferred", in: "primaryPreferred", want: readpref.PrimaryPreferredMode},
		{name: "empty defaults to primaryPreferred", in: "", want: readpref.PrimaryPreferredMode},
		{name: "secondary", in: "secondary", want: readpref.SecondaryMode},
		{name: "secondaryPreferred", in: "secondaryPreferred", want: readpref.SecondaryPreferredMode},
		{name: "nearest", in: "nearest", want: readpref.NearestMode},
		{name: "invalid errors", in: "bogus", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rp, err := readPreference(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				assert.Nil(t, rp)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, rp)
			assert.Equal(t, tc.want, rp.Mode())
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want slog.Level
	}{
		{name: "debug", in: "debug", want: slog.LevelDebug},
		{name: "info", in: "info", want: slog.LevelInfo},
		{name: "warn", in: "warn", want: slog.LevelWarn},
		{name: "error", in: "error", want: slog.LevelError},
		{name: "uppercase trimmed", in: "  ERROR  ", want: slog.LevelError},
		{name: "unknown defaults to info", in: "trace", want: slog.LevelInfo},
		{name: "empty defaults to info", in: "", want: slog.LevelInfo},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, parseLevel(tc.in))
		})
	}
}

func TestNowMs(t *testing.T) {
	a := nowMs()
	b := nowMs()
	assert.GreaterOrEqual(t, b, a, "nowMs is monotonic non-decreasing in unix millis")
	assert.Positive(t, a)
}

// fakeJetStream embeds the oteljetstream.JetStream interface (unimplemented methods panic if
// called, which these tests never do) and overrides only CreateOrUpdateConsumer.
type fakeJetStream struct {
	oteljetstream.JetStream
	calls int
	err   error
}

//nolint:gocritic // cfg by value matches the oteljetstream.JetStream interface signature.
func (f *fakeJetStream) CreateOrUpdateConsumer(_ context.Context, _ string, _ jetstream.ConsumerConfig) (oteljetstream.Consumer, error) {
	f.calls++
	return nil, f.err
}

func TestCreateConsumerWithRetry_ImmediateSuccess(t *testing.T) {
	js := &fakeJetStream{err: nil}
	cons, err := createConsumerWithRetry(context.Background(), js, "MIGRATION_OPLOG_site1", jetstream.ConsumerConfig{})
	require.NoError(t, err)
	assert.Nil(t, cons, "fake returns a nil consumer on success")
	assert.Equal(t, 1, js.calls, "success on the first attempt — no retry")
}

func TestCreateConsumerWithRetry_NonRecoverableError(t *testing.T) {
	js := &fakeJetStream{err: errors.New("boom")} // not ErrStreamNotFound → returned immediately
	_, err := createConsumerWithRetry(context.Background(), js, "MIGRATION_OPLOG_site1", jetstream.ConsumerConfig{})
	require.Error(t, err)
	assert.Equal(t, 1, js.calls, "a non-stream-not-found error is not retried")
}

func TestCreateConsumerWithRetry_ContextCancelledDuringWait(t *testing.T) {
	js := &fakeJetStream{err: jetstream.ErrStreamNotFound} // would retry, but ctx is cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := createConsumerWithRetry(ctx, js, "MIGRATION_OPLOG_site1", jetstream.ConsumerConfig{})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewMetricsServer(t *testing.T) {
	srv := newMetricsServer()
	require.NotNil(t, srv)
	assert.NotNil(t, srv.Handler)
	assert.Positive(t, srv.ReadHeaderTimeout)
}
