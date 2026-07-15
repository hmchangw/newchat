package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun_FailsFastOnBadConfig(t *testing.T) {
	t.Run("missing required env", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("TEAMS_TENANT_ID", "")

		err := run()
		require.Error(t, err)
		require.ErrorContains(t, err, "parse config")
	})
	t.Run("page size out of range", func(t *testing.T) {
		setRequiredEnv(t)
		t.Setenv("GRAPH_PAGE_SIZE", "1000")

		err := run()
		require.Error(t, err)
		require.ErrorContains(t, err, "GRAPH_PAGE_SIZE")
	})
}
