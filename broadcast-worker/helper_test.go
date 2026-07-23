package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMentionVisible(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	before := base.Add(-time.Hour)
	after := base.Add(time.Hour)
	tests := []struct {
		name    string
		hss     *time.Time
		parent  *time.Time
		visible bool
	}{
		{"nil window is unrestricted", nil, &base, true},
		{"nil window nil parent still unrestricted", nil, nil, true},
		{"parent after window is visible", &before, &base, true},
		{"parent equal to window is visible", &base, &base, true},
		{"parent before window is hidden", &after, &base, false},
		{"set window with nil parent is hidden", &before, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.visible, mentionVisible(tt.hss, tt.parent))
		})
	}
}

func TestIsBot(t *testing.T) {
	cases := []struct {
		account string
		want    bool
	}{
		{"weather.bot", true},
		{"p_tchatadmin_siteA", true}, // platform-admin pseudo-account: no client
		{"p_webhook", false},         // QA test account: an ordinary user with a client
		{"p_qa1", false},
		{"alice", false},
	}
	for _, tc := range cases {
		t.Run(tc.account, func(t *testing.T) {
			assert.Equal(t, tc.want, isBot(tc.account))
		})
	}
}

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
