package natsutil

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestMigrationHeader(t *testing.T) {
	msg := &nats.Msg{Subject: "x", Header: nats.Header{}}
	assert.False(t, IsMigrationLive(msg))
	SetMigrationLive(msg)
	assert.Equal(t, "live", msg.Header.Get(HeaderMigration))
	assert.True(t, IsMigrationLive(msg))
}

func TestIsMigrationLive_NilHeader(t *testing.T) {
	assert.False(t, IsMigrationLive(&nats.Msg{Subject: "x"}))
}

func TestIsMigrationLiveHeader(t *testing.T) {
	tests := []struct {
		name   string
		header nats.Header
		want   bool
	}{
		{name: "nil header", header: nil, want: false},
		{name: "absent header", header: nats.Header{}, want: false},
		{name: "live value", header: nats.Header{HeaderMigration: []string{MigrationLive}}, want: true},
		{name: "other value", header: nats.Header{HeaderMigration: []string{"backfill"}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsMigrationLiveHeader(tt.header))
		})
	}
}
