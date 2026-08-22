package natsrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGuardConfig_Validate(t *testing.T) {
	require.NoError(t, GuardConfig{MaxConcurrency: 256, RequestTimeout: 10 * time.Second}.Validate())
	require.NoError(t, GuardConfig{MaxConcurrency: 0, RequestTimeout: 0}.Validate(), "zero disables, not invalid")

	err := GuardConfig{MaxConcurrency: -1}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MAX_CONCURRENCY")

	err = GuardConfig{MaxConcurrency: 1, RequestTimeout: -1}.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "REQUEST_TIMEOUT")
}

func TestGuardConfig_Options_InstallsSemaphoreWhenPositive(t *testing.T) {
	r := &Router{}
	for _, o := range (GuardConfig{MaxConcurrency: 8}).Options() {
		o(r)
	}
	require.NotNil(t, r.sem, "positive MaxConcurrency installs an admission semaphore")
	assert.Equal(t, 8, cap(r.sem))
}

func TestGuardConfig_Options_NoneWhenZero(t *testing.T) {
	r := &Router{}
	for _, o := range (GuardConfig{MaxConcurrency: 0}).Options() {
		o(r)
	}
	assert.Nil(t, r.sem, "zero MaxConcurrency leaves the router unbounded")
}

func TestDefaultGuarded_AppliesCapAndTimeout(t *testing.T) {
	g := GuardConfig{MaxConcurrency: 8, RequestTimeout: 50 * time.Millisecond}
	r := DefaultGuarded(nil, "svc", g)

	require.NotNil(t, r.sem, "admission cap installed from the guard")
	assert.Equal(t, 8, cap(r.sem))
	// Default's 3 middleware (Recovery, RequestID, Logging) + the guard timeout.
	assert.Len(t, r.middleware, 4)
}

func TestDefaultGuarded_NoCapWhenZero(t *testing.T) {
	r := DefaultGuarded(nil, "svc", GuardConfig{MaxConcurrency: 0, RequestTimeout: 0})
	assert.Nil(t, r.sem, "zero concurrency leaves the router unbounded")
	assert.Len(t, r.middleware, 3, "no timeout middleware when RequestTimeout is 0")
}

func TestGuardConfig_TimeoutMiddleware(t *testing.T) {
	assert.Empty(t, (GuardConfig{RequestTimeout: 0}).TimeoutMiddleware(), "zero disables the timeout middleware")

	mws := GuardConfig{RequestTimeout: 50 * time.Millisecond}.TimeoutMiddleware()
	require.Len(t, mws, 1)

	// The middleware must put a deadline on the handler's context.
	c := &Context{ctx: t.Context(), chain: &chainState{index: -1}}
	var ok bool
	c.chain.handlers = []HandlerFunc{mws[0], func(c *Context) { _, ok = c.Deadline() }}
	c.Next()
	assert.True(t, ok, "timeout middleware sets a context deadline")
}
