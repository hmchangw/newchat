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
