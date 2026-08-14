package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name       string
		hours      int
		wantErrSub string
	}{
		{name: "default window", hours: 72},
		{name: "one hour window", hours: 1},
		// msgbucket.Sizer divides by the window, so a non-positive value panics
		// on the first message unless startup rejects it.
		{name: "zero window", hours: 0, wantErrSub: "MESSAGE_BUCKET_HOURS"},
		{name: "negative window", hours: -1, wantErrSub: "MESSAGE_BUCKET_HOURS"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config{MessageBucketHours: tt.hours}

			err := validateConfig(&cfg)

			if tt.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrSub)
		})
	}
}
