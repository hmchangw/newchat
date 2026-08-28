package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

func baseConfig() Config {
	return Config{BatchSize: 100, MaxWorkers: 8, Pool: mongoutil.PoolConfig{MaxPoolSize: 500}}
}

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"valid", func(*Config) {}, false},
		{"zero batch size", func(c *Config) { c.BatchSize = 0 }, true},
		{"negative batch size", func(c *Config) { c.BatchSize = -1 }, true},
		{"zero workers", func(c *Config) { c.MaxWorkers = 0 }, true},
		{"negative workers", func(c *Config) { c.MaxWorkers = -3 }, true},
		{"zero max pool size", func(c *Config) { c.Pool.MaxPoolSize = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(&cfg)
			err := validateConfig(cfg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}
