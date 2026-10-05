package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ValidateDevMode(t *testing.T) {
	assert.NoError(t, config{DevMode: false}.validateDevMode())
	assert.NoError(t, config{DevMode: true, DevModeLocalOnlyAck: true}.validateDevMode())
	err := config{DevMode: true, DevModeLocalOnlyAck: false}.validateDevMode()
	assert.ErrorContains(t, err, "DEV_MODE_LOCAL_ONLY_ACK")
}
