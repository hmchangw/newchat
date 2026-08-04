package badgecache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey_HashTagShape(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    string
	}{
		{"simple account", "alice", "badge:{alice}"},
		{"empty account", "", "badge:{}"},
		{"account with special chars", "alice@example.com", "badge:{alice@example.com}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Key(tt.account))
		})
	}
}

func TestKey_DifferentAccountsDifferentKeys(t *testing.T) {
	assert.NotEqual(t, Key("alice"), Key("bob"))
}
