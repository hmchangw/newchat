package subject_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/subject"
)

func TestEffectiveRouteMode(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	grace := 30 * time.Minute

	tests := []struct {
		name       string
		configured subject.RoomRouteMode
		lane       subject.Lane
		restoredAt time.Time
		want       subject.RoomRouteMode
	}{
		{
			name:       "failover lane always routes global",
			configured: subject.RouteLocal,
			lane:       subject.LaneFailover,
			want:       subject.RouteGlobal,
		},
		{
			name:       "failover lane routes global even in dual mode",
			configured: subject.RouteDual,
			lane:       subject.LaneFailover,
			want:       subject.RouteGlobal,
		},
		{
			name:       "home lane, never lost, uses configured",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: time.Time{},
			want:       subject.RouteLocal,
		},
		{
			name:       "home lane inside the grace window dual-publishes",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-5 * time.Minute),
			want:       subject.RouteDual,
		},
		{
			name:       "home lane past the grace window reverts to configured",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-31 * time.Minute),
			want:       subject.RouteLocal,
		},
		{
			name:       "grace is pointless when configured is already global",
			configured: subject.RouteGlobal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-5 * time.Minute),
			want:       subject.RouteGlobal,
		},
		{
			name:       "exact window boundary is outside",
			configured: subject.RouteLocal,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-30 * time.Minute),
			want:       subject.RouteLocal,
		},
		{
			name:       "dual configured stays dual inside the window",
			configured: subject.RouteDual,
			lane:       subject.LaneHome,
			restoredAt: now.Add(-5 * time.Minute),
			want:       subject.RouteDual,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subject.EffectiveRouteMode(tt.configured, tt.lane, tt.restoredAt, grace, now)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLaneRouter_Mode(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	failover := subject.NewLaneRouter(subject.RouteLocal, subject.LaneFailover, nil, 30*time.Minute)
	assert.Equal(t, subject.RouteGlobal, failover.Mode(now))

	restored := now.Add(-1 * time.Minute)
	home := subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome,
		func() time.Time { return restored }, 30*time.Minute)
	assert.Equal(t, subject.RouteDual, home.Mode(now))
}

// A nil restoredAt must not panic — the failover router has no tracker, and a
// service that does not watch restores simply never enters the grace window.
func TestLaneRouter_NilRestoredAt(t *testing.T) {
	r := subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil, 30*time.Minute)
	assert.NotPanics(t, func() { r.Mode(time.Now()) })
	assert.Equal(t, subject.RouteLocal, r.Mode(time.Now()))
}

// The zero LaneRouter must be inert rather than panic: it resolves to the zero
// RoomRouteMode, which is RouteGlobal — the fail-safe that reaches every client.
func TestLaneRouter_ZeroValueRoutesGlobal(t *testing.T) {
	var r subject.LaneRouter
	assert.Equal(t, subject.RouteGlobal, r.Mode(time.Now()))
}

// A publish site must not panic on a handler that was assembled without a
// resolver; it degrades to global, which over-delivers rather than losing
// events.
func TestResolveMode_NilResolverRoutesGlobal(t *testing.T) {
	assert.Equal(t, subject.RouteGlobal, subject.ResolveMode(nil, time.Now()))
}

func TestResolveMode_DelegatesToTheResolver(t *testing.T) {
	r := subject.NewLaneRouter(subject.RouteLocal, subject.LaneHome, nil, time.Minute)
	assert.Equal(t, subject.RouteLocal, subject.ResolveMode(r, time.Now()))
}
