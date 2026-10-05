package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_ValidateDevMode(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config
		wantErr bool
	}{
		{name: "dev mode off, ack off", cfg: config{DevMode: false, DevModeLocalOnlyAck: false}},
		{name: "dev mode off, ack on", cfg: config{DevMode: false, DevModeLocalOnlyAck: true}},
		{name: "dev mode on, ack on", cfg: config{DevMode: true, DevModeLocalOnlyAck: true}},
		{name: "dev mode on, ack off is refused", cfg: config{DevMode: true, DevModeLocalOnlyAck: false}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validateDevMode()
			if tt.wantErr {
				assert.ErrorContains(t, err, "DEV_MODE_LOCAL_ONLY_ACK")
				return
			}
			assert.NoError(t, err)
		})
	}
}
