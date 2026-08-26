// This file is deliberately untagged. Every other file in this package is
// //go:build integration, so an untagged helper here is importable from ordinary
// unit tests without dragging testcontainers, gocql or minio into their build.
package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// UnsetEnv clears key for the duration of the test and restores its prior
// presence and value on cleanup, so a test asserting a config default cannot be
// perturbed by the host environment or by a sibling test.
//
// t.Setenv is not a substitute: it sets a variable to a value, and "present but
// empty" is a different input to caarlos0/env than "absent" — only the latter
// yields the envDefault a default-value test is asserting.
func UnsetEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if had {
			require.NoError(t, os.Setenv(key, prev))
		}
	})
}
