package natsutil_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

func TestConnect_MissingCredsFileFailsFast(t *testing.T) {
	_, err := natsutil.Connect(context.Background(), "nats://127.0.0.1:1", "/definitely/does/not/exist.creds",
		noop.NewTracerProvider(), propagation.TraceContext{})
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// unsetForTest removes an env var for the duration of the test and restores the
// original value on cleanup (t.Setenv snapshots it), so a Connect that mutates
// the NATS-gate env can't leak into sibling tests.
func unsetForTest(t *testing.T, k string) {
	t.Helper()
	t.Setenv(k, "")
	_ = os.Unsetenv(k)
}

// enableNATSTracing runs at the top of Connect, before the connect attempt — so
// a Connect that fails to dial still exercises the gate logic. With the master
// switch off (default), the NATS trace gate must be left unset so otelnats
// skips per-message span work (native hot-path cost).
func TestConnect_NATSGate_MasterOff_LeavesGateUnset(t *testing.T) {
	unsetForTest(t, "OTEL_NATS_TRACING_ENABLED")
	unsetForTest(t, "OTEL_INSTRUMENTATION_GO_TRACING_ENABLED")
	t.Setenv("O11Y_ENABLED", "false")

	_, _ = natsutil.Connect(context.Background(), "nats://127.0.0.1:1", "",
		noop.NewTracerProvider(), propagation.TraceContext{})

	_, ok := os.LookupEnv("OTEL_NATS_TRACING_ENABLED")
	require.False(t, ok, "master off must not force the NATS trace gate on")
}

func TestConnect_NATSGate_MasterOn_SetsGate(t *testing.T) {
	unsetForTest(t, "OTEL_NATS_TRACING_ENABLED")
	unsetForTest(t, "OTEL_INSTRUMENTATION_GO_TRACING_ENABLED")
	t.Setenv("O11Y_ENABLED", "true")

	_, _ = natsutil.Connect(context.Background(), "nats://127.0.0.1:1", "",
		noop.NewTracerProvider(), propagation.TraceContext{})

	require.Equal(t, "true", os.Getenv("OTEL_NATS_TRACING_ENABLED"))
	require.Equal(t, "true", os.Getenv("OTEL_INSTRUMENTATION_GO_TRACING_ENABLED"))
}

func TestConnect_PresentCredsFilePassesPrecheck(t *testing.T) {
	// A real connect would still fail (invalid creds content, bogus URL), but
	// the pre-check must succeed when the file exists. We assert by checking
	// the error did NOT come from the missing-file precondition.
	dir := t.TempDir()
	path := filepath.Join(dir, "fake.creds")
	require.NoError(t, os.WriteFile(path, []byte("not-a-real-creds-file"), 0o600))

	_, err := natsutil.Connect(context.Background(), "nats://127.0.0.1:1", path,
		noop.NewTracerProvider(), propagation.TraceContext{})
	require.False(t, errors.Is(err, os.ErrNotExist), "precheck should pass when file exists, got: %v", err)
}
