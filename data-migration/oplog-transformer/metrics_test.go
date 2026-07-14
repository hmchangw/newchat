package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMetrics(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)
	require.NotNil(t, m)
	// Recording must not panic with a real (no-op exporter) meter.
	m.onProcessed(context.Background(), "insert")
	m.onNak(context.Background(), "update")
	m.onTerm(context.Background(), "insert")
	m.onSkipped(context.Background(), "system_message")
	m.onThreadLinkDropped(context.Background(), "missing")
	m.onRecovered(context.Background(), "rocketchat_message")
	m.onHistoryRejected(context.Background(), "bad_request")
	m.onExhausted(context.Background(), "update")
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics // the unit-test case: processOne's mtr is nil
	require.NotPanics(t, func() {
		m.onProcessed(context.Background(), "insert")
		m.onNak(context.Background(), "update")
		m.onTerm(context.Background(), "insert")
		m.onSkipped(context.Background(), "system_message")
		m.onThreadLinkDropped(context.Background(), "corrupt")
		m.onRecovered(context.Background(), "rocketchat_message")
		m.onHistoryRejected(context.Background(), "bad_request")
		m.onExhausted(context.Background(), "update")
	})
}
