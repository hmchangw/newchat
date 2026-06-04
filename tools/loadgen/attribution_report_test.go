package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRenderBottleneck_Determined(t *testing.T) {
	var sb strings.Builder
	v := bottleneckVerdict{
		Component: "message-worker", Resource: "Cassandra", Confidence: "high", Determined: true,
		Reasons: []string{"message-worker consumer backlog grew", "cassandra CPU plateaued"},
	}
	renderBottleneck(&sb, &v)
	out := sb.String()
	assert.Contains(t, out, "BOTTLENECK: message-worker (Cassandra-bound)")
	assert.Contains(t, out, "message-worker consumer backlog grew")
	assert.Contains(t, out, "confidence: high")
}

func TestRenderBottleneck_Undetermined(t *testing.T) {
	var sb strings.Builder
	v := bottleneckVerdict{Reasons: []string{"prometheus unreachable"}}
	renderBottleneck(&sb, &v)
	assert.Contains(t, sb.String(), "BOTTLENECK: undetermined (prometheus unreachable)")
}

func TestRenderBottleneck_UndeterminedNoReasons(t *testing.T) {
	var sb strings.Builder
	v := bottleneckVerdict{} // Determined=false, empty Reasons
	renderBottleneck(&sb, &v)
	assert.Contains(t, sb.String(), "BOTTLENECK: undetermined (no signal)")
}

func TestBottleneckCSVColumns(t *testing.T) {
	det := bottleneckCSVColumns(&bottleneckVerdict{Component: "message-worker", Resource: "Cassandra", Confidence: "high", Determined: true})
	assert.Equal(t, []string{"message-worker", "cassandra", "high"}, det)

	und := bottleneckCSVColumns(&bottleneckVerdict{Determined: false})
	assert.Equal(t, []string{"undetermined", "", ""}, und)
}
