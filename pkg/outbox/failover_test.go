package outbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/outbox"
)

func TestForwardWithFailover(t *testing.T) {
	tests := []struct {
		name         string
		primaryErr   error
		failoverErr  error
		wantSubjects []string
		wantErr      bool
	}{
		{
			name:         "primary succeeds, no fallback",
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
		},
		{
			name:       "no responders falls back",
			primaryErr: nats.ErrNoResponders,
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
		},
		{
			name:       "jetstream no stream response falls back",
			primaryErr: jetstream.ErrNoStreamResponse,
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
		},
		{
			name:         "timeout does NOT fall back",
			primaryErr:   nats.ErrTimeout,
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
			wantErr:      true,
		},
		{
			name:         "unknown error does NOT fall back",
			primaryErr:   errors.New("boom"),
			wantSubjects: []string{"chat.inbox.site-a.external.member_added"},
			wantErr:      true,
		},
		{
			name:        "fallback also fails",
			primaryErr:  nats.ErrNoResponders,
			failoverErr: errors.New("buddy down"),
			wantSubjects: []string{
				"chat.inbox.site-a.external.member_added",
				"chat.failover.inbox.site-a.external.member_added",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			publish := func(_ context.Context, subj string, _ []byte, msgID string) error {
				got = append(got, subj)
				assert.Equal(t, "dedup-1", msgID, "same dedup id on both lanes")
				if len(got) == 1 {
					return tt.primaryErr
				}
				return tt.failoverErr
			}

			err := outbox.ForwardWithFailover(context.Background(), publish,
				"site-a", "member_added", []byte(`{}`), "dedup-1")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantSubjects, got)
		})
	}
}

func TestPublishTo_SelectsLaneSubject(t *testing.T) {
	tests := []struct {
		name     string
		failover bool
		want     string
	}{
		{"live lane", false, "chat.outbox.site-a.site-b.member_added"},
		{"failover lane", true, "chat.failover.outbox.site-a.site-b.member_added"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			publish := func(_ context.Context, subj string, _ []byte, _ string) error {
				got = subj
				return nil
			}
			err := outbox.PublishTo(context.Background(), publish, "site-a", "r1", "site-b",
				model.InboxMemberAdded, []byte(`{}`), "d1", 1234, tt.failover)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// The partition guard must hold on both lanes: an unknown type on the failover
// stream would sit unconsumed exactly as it would on the live one.
func TestPublishTo_RejectsUnknownEventTypeOnFailoverLane(t *testing.T) {
	publish := func(_ context.Context, _ string, _ []byte, _ string) error { return nil }

	err := outbox.PublishTo(context.Background(), publish, "site-a", "r1", "site-b",
		model.InboxEventType("not_a_real_type"), []byte(`{}`), "d1", 1234, true)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "filter set")
}

// Publish must stay a thin wrapper so no existing caller changes behaviour.
func TestPublish_TargetsTheLiveLane(t *testing.T) {
	var got string
	publish := func(_ context.Context, subj string, _ []byte, _ string) error {
		got = subj
		return nil
	}
	err := outbox.Publish(context.Background(), publish, "site-a", "r1", "site-b",
		model.InboxMemberAdded, []byte(`{}`), "d1", 1234)
	require.NoError(t, err)
	assert.Equal(t, "chat.outbox.site-a.site-b.member_added", got)
}
