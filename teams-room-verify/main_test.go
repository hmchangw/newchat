package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// run() must fail fast when required config is absent (no MONGO_URI, no registry).
func TestRun_MissingConfigFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "")
	t.Setenv("TEAMS_VERIFY_SITE_URLS", "")
	require.Error(t, run())
}

// A syntactically valid Mongo URI but an unparseable registry must still fail
// before any work begins.
func TestRun_BadSiteRegistryFailsFast(t *testing.T) {
	t.Setenv("MONGO_URI", "mongodb://localhost:27017")
	t.Setenv("TEAMS_VERIFY_SITE_URLS", "not-json")
	require.Error(t, run())
}

// validateConfig must run — and fail fast — before any dependency is wired,
// including before the site registry is even parsed. This is a direct
// regression test for that ordering: an invalid numeric knob must reject the
// run without ever reaching a Mongo dial.
func TestRun_InvalidNumericConfigFailsFast(t *testing.T) {
	tests := []struct {
		name       string
		batchSize  string
		maxWorkers string
	}{
		{name: "batch size zero", batchSize: "0", maxWorkers: "8"},
		{name: "batch size negative", batchSize: "-1", maxWorkers: "8"},
		{name: "max workers zero", batchSize: "200", maxWorkers: "0"},
		{name: "max workers negative", batchSize: "200", maxWorkers: "-1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MONGO_URI", "mongodb://localhost:27017")
			t.Setenv("TEAMS_VERIFY_SITE_URLS", `{"site-a":"http://inspector:8080"}`)
			t.Setenv("VERIFY_BATCH_SIZE", tt.batchSize)
			t.Setenv("MAX_WORKERS", tt.maxWorkers)
			require.Error(t, run())
		})
	}
}
