package session_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/session"
)

func TestCollectionConstant(t *testing.T) {
	// Both admin-service and botplatform-service must target the same
	// collection; codify the name here so a rename can't silently drift.
	assert.Equal(t, "sessions", session.Collection)
}

func TestSession_ZeroValueIsValid(t *testing.T) {
	// Sanity: zero value must be usable (no required constructor).
	var s session.Session
	assert.Empty(t, s.ID)
	assert.Empty(t, s.Roles)
}
