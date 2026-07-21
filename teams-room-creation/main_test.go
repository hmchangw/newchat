package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI/NATS_URL).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("NATS_URL", "")
	require.Error(t, run())
}
