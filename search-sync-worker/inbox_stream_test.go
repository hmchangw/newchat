package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// TestParseMemberEvent covers the INBOX envelope contract every internal- and
// external-lane publisher must satisfy. The unwrapped case is the one that
// matters: a bare InboxMemberEvent shares `timestamp` and `siteId` with
// InboxEvent, so it decodes far enough to clear a timestamp-only guard and
// then fails deep in the payload decode with an opaque JSON error.
func TestParseMemberEvent(t *testing.T) {
	const ts int64 = 1735689600000

	inner := model.InboxMemberEvent{
		RoomID:    "r-eng",
		RoomName:  "engineering",
		RoomType:  model.RoomTypeChannel,
		SiteID:    "site-a",
		Accounts:  []string{"alice", "bob"},
		JoinedAt:  ts,
		Timestamp: ts,
	}
	innerData, err := json.Marshal(inner)
	require.NoError(t, err)

	envelope := func(mutate func(*model.InboxEvent)) []byte {
		evt := model.InboxEvent{
			Type:       model.InboxMemberAdded,
			SiteID:     "site-a",
			DestSiteID: "site-a",
			Payload:    innerData,
			Timestamp:  ts,
		}
		if mutate != nil {
			mutate(&evt)
		}
		data, mErr := json.Marshal(evt)
		require.NoError(t, mErr)
		return data
	}

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{
			name: "valid envelope",
			data: envelope(nil),
		},
		{
			name:    "unwrapped inner event",
			data:    innerData,
			wantErr: "missing payload envelope",
		},
		{
			name:    "missing event type",
			data:    envelope(func(e *model.InboxEvent) { e.Type = "" }),
			wantErr: "missing event type",
		},
		{
			name:    "empty payload",
			data:    envelope(func(e *model.InboxEvent) { e.Payload = nil }),
			wantErr: "missing payload envelope",
		},
		{
			name:    "missing timestamp",
			data:    envelope(func(e *model.InboxEvent) { e.Timestamp = 0 }),
			wantErr: "missing timestamp",
		},
		{
			name:    "malformed inner payload",
			data:    envelope(func(e *model.InboxEvent) { e.Payload = []byte("{") }),
			wantErr: "unmarshal inbox member event",
		},
		{
			name:    "malformed envelope",
			data:    []byte("{"),
			wantErr: "unmarshal inbox event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt, payload, err := parseMemberEvent(tt.data)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, evt)
			require.NotNil(t, payload)
			assert.Equal(t, model.InboxMemberAdded, evt.Type)
			assert.Equal(t, ts, evt.Timestamp)
			assert.Equal(t, "r-eng", payload.RoomID)
			assert.Equal(t, []string{"alice", "bob"}, payload.Accounts)
		})
	}
}

// TestParseMemberEvent_UnwrappedInnerEventClearsTimestampGuard pins the exact
// reason a timestamp-only guard cannot catch an unwrapped publish: `timestamp`
// is a field on BOTH structs, so it survives the mis-decode with a valid value
// while `type` and `payload` are silently lost.
func TestParseMemberEvent_UnwrappedInnerEventClearsTimestampGuard(t *testing.T) {
	const ts int64 = 1735689600000
	innerData, err := json.Marshal(model.InboxMemberEvent{
		RoomID:    "r-eng",
		SiteID:    "site-a",
		Accounts:  []string{"alice"},
		Timestamp: ts,
	})
	require.NoError(t, err)

	var evt model.InboxEvent
	require.NoError(t, json.Unmarshal(innerData, &evt))
	assert.Equal(t, ts, evt.Timestamp, "timestamp survives the mis-decode, so it cannot gate on it")
	assert.Equal(t, "site-a", evt.SiteID, "siteId survives too")
	assert.Empty(t, evt.Type, "type is lost")
	assert.Empty(t, evt.Payload, "payload is lost — this is what must be gated on")
}
