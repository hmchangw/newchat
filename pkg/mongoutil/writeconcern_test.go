package mongoutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// A worker that acknowledges its input only after the write lands needs the
// write to survive a primary failover. Left unset, the concern comes from the
// URI or the cluster default, neither of which the service can see.
func TestApplyTuning_SetsWriteConcern(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithWriteConcern(writeconcern.Majority())).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.WriteConcern)
	assert.Equal(t, "majority", clientOpts.WriteConcern.W)
}

// Unset must leave the URI's or the cluster's own concern in place.
func TestApplyTuning_UnsetLeavesWriteConcernUntouched(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig().applyTuning(clientOpts)

	assert.Nil(t, clientOpts.WriteConcern)
}

// A nil concern is a no-op rather than a panic, so a caller can pass one
// through from config without branching.
func TestWithWriteConcern_NilIsANoOp(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithWriteConcern(nil)).applyTuning(clientOpts)

	assert.Nil(t, clientOpts.WriteConcern)
}
