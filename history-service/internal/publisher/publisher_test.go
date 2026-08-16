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

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

type recordingJetStream struct {
	o11ynats.JetStream
	msg *nats.Msg
	err error
}

func (j *recordingJetStream) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	j.msg = msg
	return &jetstream.PubAck{}, j.err
}

type recordedAttempt struct {
	destination natsmetrics.DestinationKind
	operation   natsmetrics.Operation
	err         error
}

type recordingAttempts struct {
	attempts []recordedAttempt
}

func (r *recordingAttempts) Attempt(_ context.Context, destination natsmetrics.DestinationKind, operation natsmetrics.Operation, err error) {
	r.attempts = append(r.attempts, recordedAttempt{destination: destination, operation: operation, err: err})
}

func TestPublisher_PublishRecordsActualJetStreamAttempt(t *testing.T) {
	publishErr := errors.New("publish unavailable")
	js := &recordingJetStream{err: publishErr}
	recorder := &recordingAttempts{}
	p := New(js, withAttemptRecorder(recorder))

	err := p.Publish(context.Background(), subject.MsgCanonicalUpdated("site-a"), []byte(`{"id":"m1"}`), "dedup-1")

	require.ErrorIs(t, err, publishErr)
	require.Len(t, recorder.attempts, 1)
	assert.Equal(t, natsmetrics.DestinationCanonical, recorder.attempts[0].destination)
	assert.Equal(t, natsmetrics.OperationCanonicalPublish, recorder.attempts[0].operation)
	assert.ErrorIs(t, recorder.attempts[0].err, publishErr)
}

func TestPublisher_PublishMigrationPreservesHeaderAndRecordsSuccess(t *testing.T) {
	js := &recordingJetStream{}
	recorder := &recordingAttempts{}
	p := New(js, withAttemptRecorder(recorder))
	subj := subject.MsgCanonicalDeleted("site-a")

	require.NoError(t, p.PublishMigration(context.Background(), subj, []byte(`{}`), "dedup-2"))

	require.NotNil(t, js.msg)
	assert.Equal(t, subj, js.msg.Subject)
	assert.True(t, natsutil.IsMigrationLive(js.msg))
	require.Len(t, recorder.attempts, 1)
	assert.Equal(t, natsmetrics.DestinationCanonical, recorder.attempts[0].destination)
	assert.Equal(t, natsmetrics.OperationCanonicalPublish, recorder.attempts[0].operation)
	assert.NoError(t, recorder.attempts[0].err)
}
