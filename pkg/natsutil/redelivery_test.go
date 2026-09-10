package natsutil

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type fakeMetaMsg struct {
	meta *jetstream.MsgMetadata
	err  error
}

func (f fakeMetaMsg) Metadata() (*jetstream.MsgMetadata, error) { return f.meta, f.err }

func TestIsRedelivery_PlainContext(t *testing.T) {
	assert.False(t, IsRedelivery(context.Background()),
		"an untracked context must not be reported as a redelivery")
}

func TestStampRedelivery(t *testing.T) {
	tests := []struct {
		name string
		msg  fakeMetaMsg
		want bool
	}{
		{
			name: "first delivery is not a redelivery",
			msg:  fakeMetaMsg{meta: &jetstream.MsgMetadata{NumDelivered: 1}},
		},
		{
			name: "second delivery is a redelivery",
			msg:  fakeMetaMsg{meta: &jetstream.MsgMetadata{NumDelivered: 2}},
			want: true,
		},
		{
			name: "later redelivery still counts",
			msg:  fakeMetaMsg{meta: &jetstream.MsgMetadata{NumDelivered: 6}},
			want: true,
		},
		{
			// Unknown delivery count must fail safe: claiming "not a redelivery"
			// risks double-counting, so the caller treats it as one.
			name: "metadata error is treated as a redelivery",
			msg:  fakeMetaMsg{err: errors.New("not a jetstream message")},
			want: true,
		},
		{
			name: "nil metadata is treated as a redelivery",
			msg:  fakeMetaMsg{},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsRedelivery(StampRedelivery(context.Background(), tt.msg)))
		})
	}
}

func TestWithRedelivery(t *testing.T) {
	assert.True(t, IsRedelivery(WithRedelivery(context.Background())))
}
