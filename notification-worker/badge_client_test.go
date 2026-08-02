package main

import (
	"context"
	"encoding/json"
	"testing"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestNatsBadgeClient_Counts(t *testing.T) {
	const (
		siteID = "site-a"
		roomID = "room-1"
	)
	accounts := []string{"alice", "bob"}

	t.Run("happy path — returns the response's counts map", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			var req model.BadgeCountBatchRequest
			require.NoError(t, json.Unmarshal(m.Data, &req))
			assert.Equal(t, roomID, req.RoomID)
			assert.Equal(t, accounts, req.Accounts)

			data, _ := json.Marshal(model.BadgeCountBatchResponse{Counts: map[string]int{"alice": 3, "bob": 7}})
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc)
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"alice": 3, "bob": 7}, got)
	})

	t.Run("remote errcode error envelope — returns typed error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.Internal("badge service down"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc)
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeInternal, ee.Code)
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		client := newNatsBadgeClient(nc)
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("malformed response body — returns unmarshal error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte("not json"))
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc)
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unmarshal badge count batch response")
	})
}
