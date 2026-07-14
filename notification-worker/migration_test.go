package main

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// The skip predicate now lives in pkg/natsutil (IsMigrationLiveHeader); this
// test pins the notification-worker's reliance on it for the migrated-event skip.
func TestSkipMigrated(t *testing.T) {
	tests := []struct {
		name   string
		header nats.Header
		want   bool
	}{
		{
			name:   "live migration header is skipped",
			header: nats.Header{natsutil.HeaderMigration: []string{natsutil.MigrationLive}},
			want:   true,
		},
		{
			name:   "absent header is not skipped",
			header: nats.Header{},
			want:   false,
		},
		{
			name:   "nil header is not skipped",
			header: nil,
			want:   false,
		},
		{
			name:   "other migration value is not skipped",
			header: nats.Header{natsutil.HeaderMigration: []string{"backfill"}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, natsutil.IsMigrationLiveHeader(tt.header))
		})
	}
}
