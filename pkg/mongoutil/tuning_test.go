package mongoutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestApplyTuning_SetsMaxPoolSize(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithMaxPoolSize(500)).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxPoolSize)
	assert.Equal(t, uint64(500), *clientOpts.MaxPoolSize)
}

func TestApplyTuning_SetsMinPoolSize(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithMinPoolSize(10)).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MinPoolSize)
	assert.Equal(t, uint64(10), *clientOpts.MinPoolSize)
}

// Unset options must leave the client options untouched so a maxPoolSize/
// minPoolSize supplied via the connection URI (or the driver default) survives.
func TestApplyTuning_UnsetLeavesOptionsUntouched(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig().applyTuning(clientOpts)

	assert.Nil(t, clientOpts.MaxPoolSize, "unset WithMaxPoolSize must not write MaxPoolSize")
	assert.Nil(t, clientOpts.MinPoolSize, "unset WithMinPoolSize must not write MinPoolSize")
}

// Without this the driver never reaps, so a burst's peak is held for the
// life of the process.
func TestApplyTuning_SetsMaxIdleTime(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithMaxIdleTime(5 * time.Minute)).applyTuning(clientOpts)

	require.NotNil(t, clientOpts.MaxConnIdleTime)
	assert.Equal(t, 5*time.Minute, *clientOpts.MaxConnIdleTime)
}

// The driver reads 0 as "never reap", so an unset option must not write it.
func TestApplyTuning_UnsetMaxIdleTimeLeavesDriverDefault(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig().applyTuning(clientOpts)

	assert.Nil(t, clientOpts.MaxConnIdleTime)
}

// A stopped MongoDB does not return errors — it blocks on server selection for
// the driver default of 30s, which is longer than any request budget in this
// system. Every fail-open path downstream depends on the read failing FAST
// enough to leave time to serve a cached answer, so this bound is what makes
// those paths reachable at all.
func TestWithServerSelectionTimeout(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig(WithServerSelectionTimeout(2 * time.Second)).applyTuning(clientOpts)
	require.NotNil(t, clientOpts.ServerSelectionTimeout)
	assert.Equal(t, 2*time.Second, *clientOpts.ServerSelectionTimeout)
}

func TestWithServerSelectionTimeout_UnsetLeavesDriverDefault(t *testing.T) {
	clientOpts := options.Client()
	newConnectConfig().applyTuning(clientOpts)
	assert.Nil(t, clientOpts.ServerSelectionTimeout,
		"unset must not clobber a URI-provided or driver default value")
}
