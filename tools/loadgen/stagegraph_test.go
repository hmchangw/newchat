package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessagesStageGraph_Shape(t *testing.T) {
	g := messagesStageGraph()
	require.Len(t, g, 3)
	assert.Equal(t, "message-gatekeeper", g[0].Name)
	assert.Equal(t, "message-gatekeeper", g[0].Container)
	assert.Equal(t, "E1", g[0].LatencySeries)
	assert.Empty(t, g[0].Durable)

	assert.Equal(t, "message-worker", g[1].Name)
	assert.Equal(t, "message-worker", g[1].Container)
	assert.Equal(t, "message-worker", g[1].Durable)
	assert.Equal(t, []string{"cassandra"}, g[1].DependsOn)

	assert.Equal(t, "broadcast-worker", g[2].Name)
	assert.Equal(t, "broadcast-worker", g[2].Container)
	assert.Equal(t, "broadcast-worker", g[2].Durable)
	assert.Equal(t, "E2", g[2].LatencySeries)
	assert.Equal(t, []string{"mongodb", "valkey"}, g[2].DependsOn)
}

func TestDependencyDisplayName(t *testing.T) {
	assert.Equal(t, "Cassandra", dependencyDisplayName("cassandra"))
	assert.Equal(t, "MongoDB", dependencyDisplayName("mongodb"))
	assert.Equal(t, "valkey", dependencyDisplayName("valkey")) // unknown -> as-is
}
