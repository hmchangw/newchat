package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func TestConnector_Fatal(t *testing.T) {
	c := &connector{fatal: make(chan error, 1)}
	c.fatal <- errors.New("boom")
	require.Error(t, <-c.Fatal())
}

func TestConnector_BeginShutdownIdempotent(t *testing.T) {
	cancels := 0
	c := &connector{done: make(chan struct{}), cancel: func() { cancels++ }}
	c.beginShutdown()
	c.beginShutdown() // idempotent — once.Do guards the second call
	select {
	case <-c.Done():
	default:
		t.Fatal("Done() channel not closed after beginShutdown")
	}
	assert.Equal(t, 1, cancels)
}

func TestConnector_AwaitWatchers_ReturnsWhenDone(t *testing.T) {
	c := &connector{}
	require.NoError(t, c.awaitWatchers(context.Background()))
}

func TestConnector_AwaitWatchers_TimesOut(t *testing.T) {
	c := &connector{}
	c.wg.Add(1) // never Done → drain can't complete
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.awaitWatchers(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
	c.wg.Done() // release the waiting goroutine
}

func TestNewMetricsServer_Healthz(t *testing.T) {
	srv := newMetricsServer()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "ok", rec.Body.String())
}

func TestNewMetricsServer_Metrics(t *testing.T) {
	srv := newMetricsServer()
	rec := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestReadPreference(t *testing.T) {
	tests := []struct {
		in   string
		want readpref.Mode
	}{
		{"primary", readpref.PrimaryMode},
		{"primaryPreferred", readpref.PrimaryPreferredMode},
		{"secondary", readpref.SecondaryMode},
		{"", readpref.SecondaryMode}, // default
		{"secondaryPreferred", readpref.SecondaryPreferredMode},
		{"NEAREST", readpref.NearestMode}, // case-insensitive
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			rp, err := readPreference(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rp.Mode())
		})
	}
}

func TestReadPreference_Invalid(t *testing.T) {
	_, err := readPreference("quorum")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid READ_PREFERENCE")
}

func TestParseLevel(t *testing.T) {
	assert.Equal(t, slog.LevelDebug, parseLevel("debug"))
	assert.Equal(t, slog.LevelWarn, parseLevel("WARN"))
	assert.Equal(t, slog.LevelError, parseLevel("error"))
	assert.Equal(t, slog.LevelInfo, parseLevel("info"))
	assert.Equal(t, slog.LevelInfo, parseLevel("bogus")) // default
}
