package reconcile

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/tools/loadgen/internal/soak/search"
)

func TestReconcilePolicy_BackoffAndClosedOutcomes(t *testing.T) {
	now := time.Unix(100, 0)
	assert.Equal(t, now.Add(5*time.Second), NextProbe(now, now, now.Add(time.Minute), 5*time.Second))
	assert.Equal(t, ClaimUnavailable, ProbeOutcome(errors.New("unavailable")))
	assert.Equal(t, ClaimRetried, ProbeOutcome(nil))
	assert.Equal(t, ClaimDeferred, SearchProbeOutcome(search.IndexTooEarly, false, nil))
	assert.Equal(t, ClaimUnavailable, SearchProbeOutcome(search.IndexUnknown, false, nil))
	assert.Equal(t, ClaimRetried, SearchProbeOutcome(search.IndexMissing, true, nil))
}

func TestShareGate_RefundsAndBoundsAdmission(t *testing.T) {
	gate := NewShareGate(0.5)
	assert.False(t, gate.Allow())
	assert.True(t, gate.Allow())
	assert.False(t, gate.Allow())
	gate.Refund()
	assert.True(t, gate.Allow())
}
