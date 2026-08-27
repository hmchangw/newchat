package jsiter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Disposition
	}{
		{"nil is transient", nil, Transient},
		{"missed heartbeat is recoverable", jetstream.ErrNoHeartbeat, Transient},
		{"wrapped missed heartbeat is recoverable", fmt.Errorf("pump: %w", jetstream.ErrNoHeartbeat), Transient},
		{"next timeout is recoverable", nats.ErrTimeout, Transient},
		{"consumer deleted needs a rebuild", jetstream.ErrConsumerDeleted, Fatal},
		{"consumer not found needs a rebuild", jetstream.ErrConsumerNotFound, Fatal},
		{"bad request needs a rebuild", jetstream.ErrBadRequest, Fatal},
		{"leadership change needs a rebuild", jetstream.ErrConsumerLeadershipChanged, Fatal},
		{"bare iterator closure needs a rebuild", jetstream.ErrMsgIteratorClosed, Fatal},
		{"unknown error needs a rebuild", errors.New("boom"), Fatal},
		{"jetstream connection closed ends consumption", jetstream.ErrConnectionClosed, Stopped},
		{"core connection closed ends consumption", nats.ErrConnectionClosed, Stopped},
		{"closed iterator on a closed connection ends consumption",
			fmt.Errorf("%w: %w", jetstream.ErrMsgIteratorClosed, jetstream.ErrConnectionClosed), Stopped},
		{"cancelled context ends consumption", context.Canceled, Stopped},
		{"deadline exceeded ends consumption", context.DeadlineExceeded, Stopped},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Classify(tt.err))
		})
	}
}
