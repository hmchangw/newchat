package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Valkey is this service's store of record rather than a cache: there is no
// source of truth to fall through to, so a cache-tight ceiling on a Lua EVAL
// under load manufactures failures nothing downstream can absorb. valkeyutil
// defaults to CacheProfile, so StoreProfile applies only if this service asks
// for it — and a dropped option is silent, which is exactly how the wiring went
// missing once already.
//
// Asserted on the dialled client rather than on the option list, because an
// Option is a func and comparing those proves nothing about what was applied.
func TestValkeyDialOptions_SelectStoreProfile(t *testing.T) {
	// Port 1 refuses immediately. The startup probe is non-fatal by design, so
	// ConnectRaw still returns a client whose options can be inspected.
	client, err := valkeyutil.ConnectRaw(context.Background(),
		valkeyutil.Config{Addrs: []string{"127.0.0.1:1"}},
		valkeyDialOptions(nil)...)
	require.NoError(t, err)
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })

	opts := client.Options()
	assert.Equal(t, valkeyutil.StoreProfile.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.WriteTimeout, opts.WriteTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.PoolTimeout, opts.PoolTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.MaxRetries, opts.MaxRetries)
	assert.NotEqual(t, valkeyutil.CacheProfile.ReadTimeout, opts.ReadTimeout,
		"the package default must not silently apply to the store of record")
}

// The instrumentation half of the bundle has to survive beside the profile: an
// uninstrumented client is the one worth least having here, since presence has
// no second signal to diagnose from.
func TestValkeyDialOptions_KeepInstrumentation(t *testing.T) {
	assert.Len(t, valkeyDialOptions(nil), 2,
		"dial options are the instrumentation bundle plus the store profile")
}
