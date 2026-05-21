//go:build integration

package testutil

import "os"

// Disable testcontainers Ryuk reaper repo-wide; our CI runner can't
// run the sidecar. Cleanup is handled by TerminateAll. Set
// TESTCONTAINERS_RYUK_DISABLED=false to flip back on locally.
func init() {
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}
