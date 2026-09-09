package roomverify

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

func TestSoakRoomVerification_ClassifiesAuthoritativeState(t *testing.T) {
	assert.Equal(t, ResultAbsent, Resolve(ResultMatched, ResultAbsent))
	assert.Equal(t, ResultMatched, Resolve(ResultMatched, ResultUnknown))
	assert.Equal(t, ResultUnknown, Resolve(ResultAbsent, ResultUnknown))
	assert.Equal(t, ResultAbsent, ClassifyName(true, "old", "new", "old"))
	assert.Equal(t, ResultMismatch, ClassifyName(true, "other", "new", "old"))
	assert.Equal(t, failure.ReasonRoomStateMissing, ReasonFor(ResultAbsent, failure.ReasonRoomNameMismatch))
}

func TestSoakRoomVerification_ClassifiesReadCursor(t *testing.T) {
	baseline := time.Unix(100, 0).UTC()
	after := baseline.Add(time.Second)
	before := baseline.Add(-time.Second)
	assert.Equal(t, ResultMatched, ClassifyReadCursor(&after, baseline, true))
	assert.Equal(t, ResultMismatch, ClassifyReadCursor(&before, baseline, true))
	assert.Equal(t, ResultAbsent, ClassifyReadCursor(&baseline, baseline, true))
}
