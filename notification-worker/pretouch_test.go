package main

import (
	"testing"

	"github.com/bytedance/sonic"
	"github.com/stretchr/testify/require"
)

// TestPretouchTypesCompile asserts every pretouched type has a sonic-compilable
// codec (both encoder AND decoder). This is the guard that catches a
// marshal-only / sonic-undecodable type (e.g. a struct-keyed map like
// cassandra.Reactions) being added to the hot path.
func TestPretouch_TypesCompile(t *testing.T) {
	for _, ty := range pretouchTypes {
		require.NoErrorf(t, sonic.Pretouch(ty), "sonic cannot compile codec for %s", ty)
	}
}
