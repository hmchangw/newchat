package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBottleneckConfig_Defaults(t *testing.T) {
	var c bottleneckConfig
	// zero value should be safe; the wiring treats Enabled=false as off.
	assert.False(t, c.Enabled)
	assert.Equal(t, "", c.PromURL)
}
