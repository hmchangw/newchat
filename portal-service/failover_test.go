package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailoverState_ServingTarget(t *testing.T) {
	tests := []struct {
		status FailoverStatus
		want   ServingTarget
	}{
		{StatusHealthy, ServingHome},
		{StatusFailedOver, ServingBackup},
		{StatusFailingBack, ServingBackup},
		{FailoverStatus("garbage"), ServingHome}, // unknown => fail-safe home
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			s := FailoverState{Status: tc.status}
			assert.Equal(t, tc.want, s.ServingTarget())
		})
	}
}

func TestApplyAction_LegalTransitions(t *testing.T) {
	tests := []struct {
		name   string
		from   FailoverStatus
		action FailoverAction
		want   FailoverStatus
	}{
		{"failover", StatusHealthy, ActionFailover, StatusFailedOver},
		{"failback", StatusFailedOver, ActionFailback, StatusFailingBack},
		{"complete", StatusFailingBack, ActionComplete, StatusHealthy},
		{"resume", StatusFailedOver, ActionResume, StatusHealthy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cur := FailoverState{SiteID: "site-a", Status: tc.from, Version: 3}
			next, err := applyAction(cur, tc.action, "jane", "because", 1700)
			require.NoError(t, err)
			assert.Equal(t, tc.want, next.Status)
			assert.Equal(t, "site-a", next.SiteID)
			assert.Equal(t, int64(4), next.Version, "version increments by 1")
			assert.Equal(t, "jane", next.Operator)
			assert.Equal(t, "because", next.Reason)
			assert.Equal(t, int64(1700), next.Since)
			assert.Equal(t, int64(1700), next.Timestamp)
		})
	}
}

func TestApplyAction_IllegalTransitions(t *testing.T) {
	tests := []struct {
		name   string
		from   FailoverStatus
		action FailoverAction
	}{
		{"double failover", StatusFailedOver, ActionFailover},
		{"failback from healthy", StatusHealthy, ActionFailback},
		{"complete from healthy", StatusHealthy, ActionComplete},
		{"resume from failing_back", StatusFailingBack, ActionResume},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cur := FailoverState{SiteID: "site-a", Status: tc.from, Version: 1}
			_, err := applyAction(cur, tc.action, "jane", "because", 1700)
			assert.ErrorIs(t, err, errIllegalTransition)
		})
	}
}

func TestIsKnownAction(t *testing.T) {
	for _, a := range []FailoverAction{ActionFailover, ActionFailback, ActionComplete, ActionResume} {
		assert.True(t, isKnownAction(a))
	}
	assert.False(t, isKnownAction(FailoverAction("nope")))
	assert.False(t, isKnownAction(FailoverAction("")))
}

func TestSentinelsDistinct(t *testing.T) {
	assert.False(t, errors.Is(errIllegalTransition, errFailoverVersionConflict))
}
