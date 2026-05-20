package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDedupedAccounts(t *testing.T) {
	cases := []struct {
		name     string
		sender   string
		mentions []string
		want     []string
	}{
		{name: "no mentions", sender: "alice", want: []string{"alice"}},
		{name: "mentions exclude sender", sender: "alice", mentions: []string{"bob", "carol"}, want: []string{"alice", "bob", "carol"}},
		{name: "sender appears in mentions", sender: "alice", mentions: []string{"alice", "bob"}, want: []string{"alice", "bob"}},
		{name: "duplicate mentions", sender: "alice", mentions: []string{"bob", "bob", "carol"}, want: []string{"alice", "bob", "carol"}},
		{name: "preserves mention order", sender: "alice", mentions: []string{"carol", "bob"}, want: []string{"alice", "carol", "bob"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, dedupedAccounts(tc.sender, tc.mentions))
		})
	}
}
