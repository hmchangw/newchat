package main

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeCollection struct {
	name    string
	filters []string
}

func (f fakeCollection) ConsumerName() string             { return f.name }
func (f fakeCollection) FilterSubjects(_ string) []string { return f.filters }

func TestBuildConsumerConfig(t *testing.T) {
	defaultSettings := stream.ConsumerSettings{
		AckWait:       30 * time.Second,
		MaxDeliver:    6,
		MaxWaiting:    512,
		MaxAckPending: 1000,
		BackOffSteps:  5,
		BackOffFactor: 2,
		BackOffMax:    8 * time.Minute,
	}

	t.Run("propagates settings", func(t *testing.T) {
		tests := []struct {
			name        string
			coll        fakeCollection
			siteID      string
			wantFilters []string
		}{
			{
				name:        "with filters",
				coll:        fakeCollection{name: "message-sync", filters: []string{"chat.msg.canonical.site-a.created"}},
				siteID:      "site-a",
				wantFilters: []string{"chat.msg.canonical.site-a.created"},
			},
			{
				name:        "without filters",
				coll:        fakeCollection{name: "spotlight-sync", filters: nil},
				siteID:      "site-a",
				wantFilters: nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cc := buildConsumerConfig(defaultSettings, tt.coll, tt.siteID)

				assert.Equal(t, tt.coll.name, cc.Durable)
				assert.Equal(t, 1000, cc.MaxAckPending)
				assert.Equal(t, tt.wantFilters, cc.FilterSubjects)
				// A hardcoded BackOff{1s,...} used to set the server-side
				// AckWait to 1s (server/consumer.go:677-682).
				assert.Equal(t, stream.DurableConsumerDefaults(defaultSettings).BackOff, cc.BackOff)
				assert.Equal(t, jetstream.AckExplicitPolicy, cc.AckPolicy)
				assert.Equal(t, 30*time.Second, cc.AckWait)
				assert.Equal(t, 6, cc.MaxDeliver)
				assert.Equal(t, 512, cc.MaxWaiting)
				assert.Equal(t, jetstream.DeliverAllPolicy, cc.DeliverPolicy)
			})
		}
	})

	t.Run("overrides flow through", func(t *testing.T) {
		coll := fakeCollection{name: "message-sync", filters: []string{"chat.msg.canonical.site-a.created"}}
		cc := buildConsumerConfig(stream.ConsumerSettings{
			AckWait:       45 * time.Second,
			MaxDeliver:    3,
			MaxWaiting:    256,
			MaxAckPending: 500,
		}, coll, "site-a")

		assert.Equal(t, "message-sync", cc.Durable)
		assert.Equal(t, 500, cc.MaxAckPending)
		assert.Equal(t, 45*time.Second, cc.AckWait)
		assert.Equal(t, 3, cc.MaxDeliver)
		assert.Equal(t, 256, cc.MaxWaiting)
	})
}

func TestCheckBatchAckCoupling(t *testing.T) {
	t.Run("ok when bulk size below ack pending", func(t *testing.T) {
		assert.Empty(t, checkBatchAckCoupling(500, 1000))
	})

	t.Run("ok when bulk size equals ack pending", func(t *testing.T) {
		// At equality a 1:1 collection can still just reach the threshold
		// (the size trigger fires at ActionCount >= bulkBatchSize), so this
		// is not flagged.
		assert.Empty(t, checkBatchAckCoupling(1000, 1000))
	})

	t.Run("warns when bulk size exceeds ack pending", func(t *testing.T) {
		msg := checkBatchAckCoupling(2000, 1000)
		assert.NotEmpty(t, msg, "bulk size above ack pending must produce a warning")
	})
}
