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

// TestNewMongoSubscriptionSource_NilDatabasePanics verifies the explicit nil
// check in newMongoSubscriptionSource — db is a required dependency, and
// this fails fast at construction instead of relying on however the mongo
// driver happens to behave when db.Collection is called on a nil receiver.
func TestNewMongoSubscriptionSource_NilDatabasePanics(t *testing.T) {
	assert.PanicsWithValue(t, "newMongoSubscriptionSource: db must not be nil", func() { newMongoSubscriptionSource(nil) })
}
