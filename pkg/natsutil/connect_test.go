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
