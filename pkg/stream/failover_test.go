package stream_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/stream"
)

type fakeFailoverJS struct {
	created   []string
	looked    []string
	info      *jetstream.StreamInfo
	createErr error
	lookupErr error
}

//nolint:gocritic // signature is dictated by the FailoverStreamManager interface
func (f *fakeFailoverJS) CreateOrUpdateStream(_ context.Context, cfg jetstream.StreamConfig) error {
	f.created = append(f.created, cfg.Name)
	return f.createErr
}

func (f *fakeFailoverJS) StreamInfo(_ context.Context, name string) (*jetstream.StreamInfo, error) {
	f.looked = append(f.looked, name)
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return f.info, nil
}

func TestEnsureFailoverStream_BootstrapCreatesWithoutAsserting(t *testing.T) {
	js := &fakeFailoverJS{}
	cfg := stream.MessagesCanonicalFailover("site-a")

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, true, "site-b")

	require.NoError(t, err)
	assert.Equal(t, []string{cfg.Name}, js.created)
	assert.Empty(t, js.looked,
		"dev bootstrap must not assert placement — a single-server NATS has no cluster")
}

func TestEnsureFailoverStream_ProductionVerifiesAndAsserts(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeFailoverJS{info: &jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: cfg.Name},
		Cluster: &jetstream.ClusterInfo{Name: "site-b"},
	}}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.NoError(t, err)
	assert.Empty(t, js.created, "production must never create a stream")
	assert.Equal(t, []string{cfg.Name}, js.looked)
}

// The check that matters: a standby stream sitting on the cluster it exists to
// outlive resolves fine by name, so only the hosting cluster catches it.
func TestEnsureFailoverStream_WrongClusterIsAnError(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeFailoverJS{info: &jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: cfg.Name},
		Cluster: &jetstream.ClusterInfo{Name: "site-a"},
	}}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "site-a", "the error must name the cluster actually hosting it")
}

func TestEnsureFailoverStream_MissingStreamIsAnError(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeFailoverJS{lookupErr: errors.New("stream not found")}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, false, "site-b")

	require.Error(t, err)
	assert.Contains(t, err.Error(), cfg.Name)
}

func TestEnsureFailoverStream_CreateFailureIsAnError(t *testing.T) {
	cfg := stream.MessagesCanonicalFailover("site-a")
	js := &fakeFailoverJS{createErr: errors.New("nats down")}

	err := stream.EnsureFailoverStream(context.Background(), js, cfg, true, "site-b")

	require.Error(t, err)
	assert.Contains(t, err.Error(), cfg.Name)
}
