package stream_test

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

func TestCheckPlacement(t *testing.T) {
	tests := []struct {
		name     string
		info     *jetstream.StreamInfo
		expected string
		wantErr  string
	}{
		{
			name: "hosted by the expected cluster",
			info: &jetstream.StreamInfo{
				Config:  jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"},
				Cluster: &jetstream.ClusterInfo{Name: "site-b"},
			},
			expected: "site-b",
		},
		{
			name: "hosted by the wrong cluster",
			info: &jetstream.StreamInfo{
				Config:  jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"},
				Cluster: &jetstream.ClusterInfo{Name: "site-a"},
			},
			expected: "site-b",
			wantErr:  `hosted by cluster "site-a", want "site-b"`,
		},
		{
			name:     "no cluster info",
			info:     &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "INBOX-FAILOVER-site-a"}},
			expected: "site-b",
			wantErr:  "no cluster info",
		},
		{
			name:     "nil info",
			info:     nil,
			expected: "site-b",
			wantErr:  "no cluster info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stream.CheckPlacement(tt.info, tt.expected)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
