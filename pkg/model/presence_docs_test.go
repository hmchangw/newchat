package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// This file anchors acceptance criteria for presence-mode documentation.
// It exists so the typed constants are the single source of truth for the
// wire values described in docs/client-api.md §8.

// AC-5.1: documented manual-only modes have stable typed wire values.
func TestPresenceDocs_WireValues(t *testing.T) {
	assert.Equal(t, PresenceStatus("dnd"), StatusDND)
	assert.Equal(t, PresenceStatus("brb"), StatusBRB)
}
