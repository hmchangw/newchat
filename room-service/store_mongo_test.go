package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdminAccountPatterns(t *testing.T) {
	tests := []struct {
		name           string
		prefix         string
		wantAdmin      string
		wantBotOrAdmin string
	}{
		{"default admin prefix", "p_chatadmin_", "^p_chatadmin_", `(\.bot$|^p_chatadmin_)`},
		{"legacy broad prefix", "p_", "^p_", `(\.bot$|^p_)`},
		{"empty disables admin filter", "", "", `\.bot$`},
		{"regex metacharacters are escaped", "p.a(", `^p\.a\(`, `(\.bot$|^p\.a\()`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAdmin, gotBotOrAdmin := adminAccountPatterns(tt.prefix)
			assert.Equal(t, tt.wantAdmin, gotAdmin, "adminRegex")
			assert.Equal(t, tt.wantBotOrAdmin, gotBotOrAdmin, "botOrAdminRegex")
		})
	}
}

func TestDefaultAdminAcctPrefix(t *testing.T) {
	assert.Equal(t, "p_chatadmin_", defaultAdminAcctPrefix)
}
