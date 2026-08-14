package model_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/model"
)

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

func TestPermissionState_Evaluate(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	before := now.Add(-time.Hour)
	after := now.Add(time.Hour)
	cases := []struct {
		name  string
		state *model.PermissionState
		want  bool
	}{
		{"nil state", nil, false},
		{"revoked", &model.PermissionState{Granted: false, UpdatedAt: now}, false},
		{"granted but nil bounds fails closed", &model.PermissionState{Granted: true, UpdatedAt: now}, false},
		{"granted but nil expiresAt fails closed", &model.PermissionState{Granted: true, EffectiveFrom: &before, UpdatedAt: now}, false},
		{"granted but nil effectiveFrom fails closed", &model.PermissionState{Granted: true, ExpiresAt: &after, UpdatedAt: now}, false},
		{"inside window", &model.PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &after, UpdatedAt: now}, true},
		{"now == effectiveFrom is inside (closed start)", &model.PermissionState{Granted: true, EffectiveFrom: &now, ExpiresAt: &after, UpdatedAt: now}, true},
		{"now == expiresAt is outside (open end)", &model.PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &now, UpdatedAt: now}, false},
		{"not yet effective", &model.PermissionState{Granted: true, EffectiveFrom: &after, ExpiresAt: &after, UpdatedAt: now}, false},
		{"expired", &model.PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &before, UpdatedAt: now}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.state.Evaluate(now))
		})
	}
}

func TestUserPermissions_Evaluated(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	before, after := now.Add(-time.Hour), now.Add(time.Hour)

	t.Run("nil receiver yields every known key false", func(t *testing.T) {
		var p *model.UserPermissions
		assert.Equal(t, map[model.PermissionKey]bool{model.PermissionExternalImageView: false}, p.Evaluated(now))
	})
	t.Run("active grant evaluates true", func(t *testing.T) {
		p := &model.UserPermissions{ExternalImageView: &model.PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &after, UpdatedAt: now}}
		assert.True(t, p.Evaluated(now)[model.PermissionExternalImageView])
	})
	t.Run("expired grant evaluates false", func(t *testing.T) {
		p := &model.UserPermissions{ExternalImageView: &model.PermissionState{Granted: true, EffectiveFrom: &before, ExpiresAt: &before, UpdatedAt: now}}
		assert.False(t, p.Evaluated(now)[model.PermissionExternalImageView])
	})
}

func TestUserPermissions_State(t *testing.T) {
	st := &model.PermissionState{Granted: true}
	p := &model.UserPermissions{ExternalImageView: st}
	assert.Same(t, st, p.State(model.PermissionExternalImageView))
	assert.Nil(t, p.State(model.PermissionKey("nope")))
	var nilP *model.UserPermissions
	assert.Nil(t, nilP.State(model.PermissionExternalImageView))
}

func TestPermissionFieldName(t *testing.T) {
	f, ok := model.PermissionFieldName(model.PermissionExternalImageView)
	assert.True(t, ok)
	assert.Equal(t, "externalImageView", f)
	_, ok = model.PermissionFieldName(model.PermissionKey("nope"))
	assert.False(t, ok)
}
