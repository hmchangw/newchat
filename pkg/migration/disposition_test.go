package migration

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		isFinal bool
		want    Action
	}{
		{"success", nil, false, ActionAck},
		{"poison", ErrPoison, false, ActionTerm},
		{"poison wrapped", fmt.Errorf("map doc: %w", ErrPoison), false, ActionTerm},
		{"skipped", ErrSkipped, false, ActionAckSkip},
		{"skipped wrapped", fmt.Errorf("other collection: %w", ErrSkipped), false, ActionAckSkip},
		{"transient not final", errors.New("source down"), false, ActionNak},
		{"transient final", errors.New("source down"), true, ActionTermExhausted},
		{"poison takes precedence over final", ErrPoison, true, ActionTerm},
		{"skipped takes precedence over final", ErrSkipped, true, ActionAckSkip},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.err, tc.isFinal))
		})
	}
}

func TestIsFinalDelivery(t *testing.T) {
	assert.False(t, IsFinalDelivery(1, 0), "maxDeliver<=0 means unlimited — never final")
	assert.False(t, IsFinalDelivery(100, 0), "unlimited")
	assert.False(t, IsFinalDelivery(4, 5), "below cap")
	assert.True(t, IsFinalDelivery(5, 5), "at cap is final")
	assert.True(t, IsFinalDelivery(6, 5), "past cap is final")
}
