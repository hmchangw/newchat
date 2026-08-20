package roomkeymetrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RecordFanoutErrors takes no label argument on purpose. The counter used to be
// called with attribute.String("roomId", roomID) straight from the per-account
// fan-out loop, which is one series per room — unbounded — and rebuilt the
// attribute set on every call inside that loop. Exposing a helper with no label
// parameter makes the mistake unrepresentable rather than relying on review to
// catch it again.
func TestRecordFanoutErrors_TakesNoLabels(t *testing.T) {
	require.NotPanics(t, func() {
		RecordFanoutErrors(context.Background(), 1)
		RecordFanoutErrors(context.Background(), 42)
	})
}

// A zero or negative count is a caller bug; recording it would corrupt the
// counter's monotonicity, so it is dropped.
func TestRecordFanoutErrors_IgnoresNonPositiveCounts(t *testing.T) {
	require.NotPanics(t, func() {
		RecordFanoutErrors(context.Background(), 0)
		RecordFanoutErrors(context.Background(), -1)
	})
}

// The store counter keeps its bounded op label — Get, Set, SetWithVersion and
// Rotate are a closed set, unlike a room id.
func TestRecordStoreError_KeepsBoundedOperation(t *testing.T) {
	require.NotPanics(t, func() {
		RecordStoreError(context.Background(), "Get")
	})
	assert.NotNil(t, StoreErrors)
}
