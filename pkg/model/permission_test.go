package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

func TestEvaluateGrant(t *testing.T) {
	now := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	from := now.Add(-24 * time.Hour)
	until := now.Add(24 * time.Hour)

	tests := []struct {
		name  string
		grant *model.PermissionGrant
		now   time.Time
		want  bool
	}{
		{
			name:  "nil ledger denies",
			grant: nil,
			now:   now,
			want:  false,
		},
		{
			name: "revoked row denies",
			grant: &model.PermissionGrant{
				Granted:       false,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  now,
			want: false,
		},
		{
			name: "nil effectiveFrom denies without panic",
			grant: &model.PermissionGrant{
				Granted:   true,
				ExpiresAt: &until,
			},
			now:  now,
			want: false,
		},
		{
			name: "nil expiresAt denies without panic",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
			},
			now:  now,
			want: false,
		},
		{
			name: "now equals effectiveFrom grants (closed start)",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  from,
			want: true,
		},
		{
			name: "now equals expiresAt denies (open end)",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  until,
			want: false,
		},
		{
			name: "now inside window grants",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  now,
			want: true,
		},
		{
			name: "now before window denies",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  from.Add(-time.Hour),
			want: false,
		},
		{
			name: "now after window denies",
			grant: &model.PermissionGrant{
				Granted:       true,
				EffectiveFrom: &from,
				ExpiresAt:     &until,
			},
			now:  until.Add(time.Hour),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.EvaluateGrant(tt.grant, tt.now))
		})
	}
}

func TestKnownPermission(t *testing.T) {
	tests := []struct {
		name string
		key  model.PermissionKey
		want bool
	}{
		{"known key is recognized", model.PermissionExternalImageView, true},
		{"unknown key is not recognized", model.PermissionKey("external.video.view"), false},
		{"empty key is not recognized", model.PermissionKey(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, model.KnownPermission(tt.key))
		})
	}
}
