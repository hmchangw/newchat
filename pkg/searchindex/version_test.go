package searchindex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripVersion(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantBase    string
		wantVersion int
		wantOK      bool
	}{
		{
			name:        "single digit version",
			input:       "messages-site-a-v1",
			wantBase:    "messages-site-a",
			wantVersion: 1,
			wantOK:      true,
		},
		{
			name:        "multi digit version",
			input:       "spotlight-site-b-v42",
			wantBase:    "spotlight-site-b",
			wantVersion: 42,
			wantOK:      true,
		},
		{
			name:        "no suffix returns input unchanged",
			input:       "user-room-mv-site-a",
			wantBase:    "user-room-mv-site-a",
			wantVersion: 0,
			wantOK:      false,
		},
		{
			name:        "uppercase V is not stripped",
			input:       "messages-site-a-V1",
			wantBase:    "messages-site-a-V1",
			wantVersion: 0,
			wantOK:      false,
		},
		{
			name:        "non-numeric tail is not stripped",
			input:       "messages-site-a-v1a",
			wantBase:    "messages-site-a-v1a",
			wantVersion: 0,
			wantOK:      false,
		},
		{
			name:        "version in middle is not stripped",
			input:       "messages-v1-site-a",
			wantBase:    "messages-v1-site-a",
			wantVersion: 0,
			wantOK:      false,
		},
		{
			name:        "empty input",
			input:       "",
			wantBase:    "",
			wantVersion: 0,
			wantOK:      false,
		},
		{
			name:        "only version suffix",
			input:       "-v1",
			wantBase:    "",
			wantVersion: 1,
			wantOK:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base, version, ok := StripVersion(tc.input)
			assert.Equal(t, tc.wantBase, base)
			assert.Equal(t, tc.wantVersion, version)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestIndexPattern(t *testing.T) {
	tests := []struct{ prefix, want string }{
		{"messages-site1-v1", "messages-site1-*"},
		{"messages-site1", "messages-site1-*"},
		{"msgs-v12", "msgs-*"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			assert.Equal(t, tt.want, IndexPattern(tt.prefix))
		})
	}
}
