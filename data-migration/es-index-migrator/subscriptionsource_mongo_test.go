package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToStringSlice_DropsNonStringAndEmptyValues(t *testing.T) {
	got := toStringSlice([]any{"room1", "", 42, "room2", nil})
	assert.Equal(t, []string{"room1", "room2"}, got)
}

func TestToStringSlice_EmptyInput(t *testing.T) {
	assert.Empty(t, toStringSlice(nil))
}

// TestNewMongoSubscriptionSource_NilDatabasePanics documents that db is a
// required, non-nil dependency (newMongoSubscriptionSource resolves the
// "subscriptions" collection eagerly at construction time) without opening
// any real Mongo connection — the panic fires before any I/O occurs.
func TestNewMongoSubscriptionSource_NilDatabasePanics(t *testing.T) {
	assert.Panics(t, func() { newMongoSubscriptionSource(nil) })
}
