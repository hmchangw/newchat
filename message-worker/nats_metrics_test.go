package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

func TestPersistenceMetrics_BoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newPersistenceMetrics(mp.Meter("test"))
	m.Record(context.Background(), "user", "success")
	m.Record(context.Background(), "dynamic-id", "raw error")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "message_worker_persistence_total" {
				continue
			}
			for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["message_kind"]+"/"+values["result"]] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"user/success": 1, "unknown/error": 1}, got)
}

func TestHandler_HandleJetStreamMsg_RecordsPersistenceOutcomes(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	message := model.Message{
		ID: "msg-1", RoomID: "r1", UserID: "u-1", UserAccount: "alice",
		Content: "hello", CreatedAt: now,
	}
	event := model.MessageEvent{Message: message, SiteID: "site-a", Timestamp: now.UnixMilli()}
	data, err := json.Marshal(event)
	require.NoError(t, err)
	user := &model.User{ID: "u-1", Account: "alice", SiteID: "site-a", EngName: "Alice"}
	sender := cassParticipant{ID: user.ID, EngName: user.EngName, Account: user.Account}

	for _, tt := range []struct {
		name       string
		storeError error
		wantResult string
	}{
		{name: "success", wantResult: "success"},
		{name: "store error", storeError: errors.New("cassandra unavailable"), wantResult: "error"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newPersistenceMetrics(mp.Meter("test"))
			ctrl := gomock.NewController(t)
			store := NewMockStore(ctrl)
			users := NewMockUserStore(ctrl)
			threads := NewMockThreadStore(ctrl)
			threads.EXPECT().AddReplyAccounts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
			stubHistoryAllMembers(threads)
			users.EXPECT().FindUserByAccount(gomock.Any(), "alice").Return(user, nil)
			store.EXPECT().SaveMessage(gomock.Any(), &message, &sender, "site-a").Return(tt.storeError)
			h := NewHandler(store, users, threads, "site-a", func(context.Context, string, []byte, string) error { return nil }, withPersistenceMetrics(metrics))
			msg := &fakeJSMsg{data: data}

			h.HandleJetStreamMsg(context.Background(), msg)

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			got := map[string]int64{}
			for _, scope := range rm.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if metric.Name != "message_worker_persistence_total" {
						continue
					}
					for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
						attrs := map[string]string{}
						for _, attr := range point.Attributes.ToSlice() {
							attrs[string(attr.Key)] = attr.Value.AsString()
						}
						got[attrs["message_kind"]+"/"+attrs["result"]] = point.Value
					}
				}
			}
			assert.Equal(t, map[string]int64{"user/" + tt.wantResult: 1}, got)
		})
	}
}

func TestPersistenceMetrics_Record_NilReceiverIsSafe(t *testing.T) {
	var metrics *persistenceMetrics
	metrics.Record(context.Background(), kindUser, persistSuccess)
}
