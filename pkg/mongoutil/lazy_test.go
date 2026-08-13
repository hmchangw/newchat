package mongoutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestWithLazyConnect_SetsFlag(t *testing.T) {
	cfg := newConnectConfig(WithLazyConnect())
	assert.True(t, cfg.lazy, "WithLazyConnect must set the lazy flag")
}

func TestNewConnectConfig_PingIsDefault(t *testing.T) {
	cfg := newConnectConfig()
	assert.False(t, cfg.lazy, "the startup ping must remain the default behaviour")
}

func TestWithLazyConnect_ComposesWithOtherOptions(t *testing.T) {
	cfg := newConnectConfig(WithLazyConnect(), WithMaxPoolSize(7))
	assert.True(t, cfg.lazy)
	require.NotNil(t, cfg.maxPoolSize)
	assert.EqualValues(t, 7, *cfg.maxPoolSize)
}

// TestConnect_DefaultPingsAndFailsWhenUnreachable pins the default contract:
// with MongoDB unreachable, Connect must fail rather than hand back a client.
// This is the behaviour batch/migration/CLI jobs rely on.
func TestConnect_DefaultPingsAndFailsWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := Connect(ctx, testutil.UnreachableMongoURI(t), "", "")
	require.Error(t, err, "default Connect must fail when MongoDB is unreachable")
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "mongo ping")
}

// TestConnect_LazyReturnsUsableClient is the whole point of the option: a
// long-running service must be able to construct its store and reach a serving
// state while MongoDB is down, instead of dying at boot.
func TestConnect_LazyReturnsUsableClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := Connect(ctx, testutil.UnreachableMongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err, "lazy Connect must not fail when MongoDB is unreachable")
	require.NotNil(t, client)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	coll := client.Database("lazy_db").Collection("docs")
	require.NotNil(t, coll)
	assert.Equal(t, "docs", coll.Name())
}

// TestConnect_LazyFirstOperationStillFailsBounded is the sanity check the
// design guidance asks for: booting lazily must not turn a hard startup
// failure into a hang. The first operation still has to come back with an
// error in bounded time.
func TestConnect_LazyFirstOperationStillFailsBounded(t *testing.T) {
	client, err := Connect(context.Background(), testutil.UnreachableMongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	opCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	err = client.Database("lazy_db").Collection("docs").FindOne(opCtx, map[string]any{"_id": "x"}).Err()
	elapsed := time.Since(start)

	require.Error(t, err, "first operation against unreachable MongoDB must return an error")
	assert.Less(t, elapsed, 5*time.Second, "first operation must fail in bounded time, not hang")
}

func TestConnectRead_LazySucceedsWhenUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := ConnectRead(ctx, testutil.UnreachableMongoURI(t), "", "", WithLazyConnect())
	require.NoError(t, err, "lazy ConnectRead must not fail when MongoDB is unreachable")
	require.NotNil(t, client)
	t.Cleanup(func() { Disconnect(context.Background(), client) })
}
