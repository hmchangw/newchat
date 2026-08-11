package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeCDCEvent(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    CDCEvent
		wantErr string
	}{
		{
			name:    "insert with string id",
			payload: `{"eventId":"e1","op":"insert","db":"rocketchat","coll":"rocketchat_message","documentKey":{"_id":"msg123"},"clusterTime":1700000000000}`,
			want:    CDCEvent{Collection: "rocketchat_message", Op: "insert", DocID: "msg123"},
		},
		{
			name:    "delete carries only documentKey",
			payload: `{"eventId":"e2","op":"delete","coll":"rocketchat_room","documentKey":{"_id":"r1"}}`,
			want:    CDCEvent{Collection: "rocketchat_room", Op: "delete", DocID: "r1"},
		},
		{
			name:    "non-string id rejected",
			payload: `{"op":"insert","coll":"c","documentKey":{"_id":{"$oid":"64f0"}}}`,
			wantErr: "documentKey._id is not a string",
		},
		{
			name:    "missing documentKey rejected",
			payload: `{"op":"insert","coll":"c"}`,
			wantErr: "documentKey",
		},
		{
			name:    "invalid json",
			payload: `{`,
			wantErr: "unmarshal oplog event",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCDCEvent([]byte(tt.payload))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
