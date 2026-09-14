package roomstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/tools/loadgen/internal/failure"
)

func TestSoakMemberIntent_MapsMutationDirection(t *testing.T) {
	assert.Equal(t, failure.OperationMemberAdd, (MemberIntent{Add: true}).OperationType())
	assert.Equal(t, failure.OperationMemberRemove, (MemberIntent{Add: false}).OperationType())
}

func TestSoakStateOperations_PreserveVerificationInputs(t *testing.T) {
	baseline := time.Unix(123, 0).UTC()
	assert.Equal(t, baseline, (ReadIntent{Baseline: baseline}).Baseline)
	assert.True(t, (RoomProbe{Mute: true}).Mute)
}
