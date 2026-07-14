package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDeliverCapFor: short cap for non-actionable-delete collections; room_members deletes (real writes) keep the full cap.
func TestDeliverCapFor(t *testing.T) {
	const roomMembersColl = "company_room_members"
	const maxDeliver = 100
	const deleteMaxDeliver = 60

	tests := []struct {
		name       string
		op         string
		collection string
		want       int
	}{
		{"non-delete insert on room_members uses full cap", "insert", roomMembersColl, maxDeliver},
		{"non-delete update on other collection uses full cap", "update", "company_rooms", maxDeliver},
		{"delete on room_members uses full cap (real target write)", "delete", roomMembersColl, maxDeliver},
		{"delete on rooms uses short cap (non-actionable ack-skip)", "delete", "company_rooms", deleteMaxDeliver},
		{"delete on subscriptions uses short cap", "delete", "company_subscriptions", deleteMaxDeliver},
		{"delete on thread_subs uses short cap", "delete", "company_thread_subscriptions", deleteMaxDeliver},
		{"delete on users uses short cap", "delete", "company_users", deleteMaxDeliver},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &handler{roomMembersColl: roomMembersColl}
			got := h.deliverCapFor(tc.op, tc.collection, maxDeliver, deleteMaxDeliver)
			assert.Equal(t, tc.want, got)
		})
	}
}
