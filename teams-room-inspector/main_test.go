package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI/SITE_ID).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("SITE_ID", "")
	require.Error(t, run())
}
