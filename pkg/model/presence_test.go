package model

import "testing"

func TestStatusInCall_Value(t *testing.T) {
	if StatusInCall != "in-call" {
		t.Fatalf("StatusInCall = %q, want %q", StatusInCall, "in-call")
	}
}
