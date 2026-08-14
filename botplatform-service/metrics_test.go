package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// swapBypassCounter points controlBypassed at an in-memory reader and restores the
// original on cleanup so the tests stay independent.
func swapBypassCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	c, err := mp.Meter("botplatform").Int64Counter("bot_control_bypassed_total")
	require.NoError(t, err)
	old := controlBypassed
	controlBypassed = c
	t.Cleanup(func() { controlBypassed = old })
	return reader
}

// bypassCount returns the counter value for one control label, and whether it was recorded.
func bypassCount(t *testing.T, reader *sdkmetric.ManualReader, control string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "bot_control_bypassed_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok, "bot_control_bypassed_total must be an int64 sum")
			for _, dp := range sum.DataPoints {
				if v, found := dp.Attributes.Value(attribute.Key("control")); found && v.AsString() == control {
					return dp.Value, true
				}
			}
		}
	}
	return 0, false
}

func TestControlBypassedMetric(t *testing.T) {
	t.Run("rate-limit caller fail-open records a bypass", func(t *testing.T) {
		reader := swapBypassCounter(t)
		client := newFakeIncr()
		client.err = errors.New("boom")
		r, _ := mountRLTest(t, client, 10, 100)

		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/bot", nil))

		n, ok := bypassCount(t, reader, "rate_limit_caller")
		require.True(t, ok, "expected a bypass recorded for control=rate_limit_caller")
		assert.Equal(t, int64(1), n)
	})

	t.Run("rate-limit global fail-open records a bypass", func(t *testing.T) {
		reader := swapBypassCounter(t)
		client := newFakeIncr()
		client.err = errors.New("boom")
		client.errKey = "botrl:global"
		r, _ := mountRLTest(t, client, 10, 100)

		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/bot", nil))

		n, ok := bypassCount(t, reader, "rate_limit_global")
		require.True(t, ok, "expected a bypass recorded for control=rate_limit_global")
		assert.Equal(t, int64(1), n)
	})

	t.Run("idempotency fail-open records a bypass", func(t *testing.T) {
		reader := swapBypassCounter(t)
		client := newFakeSentinel()
		client.setNXErr = errors.New("boom")
		r, _, _ := mountIdemTest(t, client, &stubTime{ns: time.Second.Nanoseconds()}, "site-a")

		r.ServeHTTP(httptest.NewRecorder(), idemPost("/api/v1/rooms/r1/messages", []byte(`{"content":"hi"}`)))

		n, ok := bypassCount(t, reader, "idempotency")
		require.True(t, ok, "expected a bypass recorded for control=idempotency")
		assert.Equal(t, int64(1), n)
	})

	t.Run("healthy Valkey records no bypass", func(t *testing.T) {
		reader := swapBypassCounter(t)
		r, _ := mountRLTest(t, newFakeIncr(), 10, 100)

		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/bot", nil))

		_, ok := bypassCount(t, reader, "rate_limit_caller")
		assert.False(t, ok, "no bypass may be recorded while the limiter is healthy")
	})
}
