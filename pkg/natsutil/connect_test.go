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
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.Error(t, err)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// The caller passes the SDK's already-resolved trace toggle. Connect must relay
// it exactly so tracing-off uses the direct/native upstream path and tracing-on
// retains propagation and spans.
func TestConnect_TracingEnabledFollowsResolvedToggle(t *testing.T) {
	native := startTestNATS(t)

	for _, tc := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled selects direct path", enabled: false},
		{name: "enabled selects traced path", enabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := natsutil.Connect(
				context.Background(),
				native.ConnectedUrl(),
				"",
				noop.NewTracerProvider(),
				propagation.TraceContext{},
				tc.enabled,
			)
			require.NoError(t, err)
			t.Cleanup(conn.Close)
			require.Equal(t, tc.enabled, conn.TracingEnabled())
		})
	}
}

func TestConnect_PresentCredsFilePassesPrecheck(t *testing.T) {
	// A real connect would still fail (invalid creds content, bogus URL), but
	// the pre-check must succeed when the file exists. We assert by checking
	// the error did NOT come from the missing-file precondition.
	dir := t.TempDir()
	path := filepath.Join(dir, "fake.creds")
	require.NoError(t, os.WriteFile(path, []byte("not-a-real-creds-file"), 0o600))

	_, err := natsutil.Connect(context.Background(), "nats://127.0.0.1:1", path,
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.False(t, errors.Is(err, os.ErrNotExist), "precheck should pass when file exists, got: %v", err)
}
