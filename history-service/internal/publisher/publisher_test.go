package publisher

import (
	"context"
	"errors"
	"testing"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// recordingJetStream is embedded rather than generated: o11ynats.JetStream is
// an external interface this package does not own, and only PublishMsg is
// exercised. The metrics seam below is ours, so it uses a generated mock.
type recordingJetStream struct {
	o11ynats.JetStream
	msg *nats.Msg
	err error
}

func (j *recordingJetStream) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.msg = msg
	return &jetstream.PubAck{}, j.err
}

func TestPublisher_PublishRecordsActualJetStreamAttempt(t *testing.T) {
	publishErr := errors.New("publish unavailable")
	js := &recordingJetStream{err: publishErr}
	recorder := NewMockattemptRecorder(gomock.NewController(t))
	recorder.EXPECT().Attempt(gomock.Any(), natsmetrics.DestinationCanonical,
		natsmetrics.OperationCanonicalPublish, gomock.Cond(func(err error) bool {
			return errors.Is(err, publishErr)
		}))
	p := New(js, withAttemptRecorder(recorder))

	err := p.Publish(context.Background(), subject.MsgCanonicalUpdated("site-a"), []byte(`{"id":"m1"}`), "dedup-1")

	require.ErrorIs(t, err, publishErr)
}

func TestPublisher_PublishMigrationPreservesHeaderAndRecordsSuccess(t *testing.T) {
	js := &recordingJetStream{}
	recorder := NewMockattemptRecorder(gomock.NewController(t))
	recorder.EXPECT().Attempt(gomock.Any(), natsmetrics.DestinationCanonical,
		natsmetrics.OperationCanonicalPublish, gomock.Nil())
	p := New(js, withAttemptRecorder(recorder))
	subj := subject.MsgCanonicalDeleted("site-a")

	require.NoError(t, p.PublishMigration(context.Background(), subj, []byte(`{}`), "dedup-2"))

	require.NotNil(t, js.msg)
	assert.Equal(t, subj, js.msg.Subject)
	assert.True(t, natsutil.IsMigrationLive(js.msg))
}
