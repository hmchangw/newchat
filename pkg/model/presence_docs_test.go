package model

import "testing"

// This file anchors acceptance criteria for presence-mode documentation.
// It exists so the typed constants are the single source of truth for the
// wire values described in docs/client-api.md §8.

// AC-5.1: documented manual-only modes have stable typed wire values.
func TestPresenceDocs_WireValues(t *testing.T) {
	if StatusDND != "dnd" {
		t.Errorf("StatusDND = %q, want %q", StatusDND, "dnd")
	}
	if StatusBRB != "brb" {
		t.Errorf("StatusBRB = %q, want %q", StatusBRB, "brb")
	}
}
