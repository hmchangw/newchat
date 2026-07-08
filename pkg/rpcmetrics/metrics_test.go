package rpcmetrics

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
)

func TestStatusLabel(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil is ok", nil, "ok"},
		{"bad request", errcode.BadRequest("bad"), "bad_request"},
		{"not found", errcode.NotFound("nope"), "not_found"},
		{"forbidden", errcode.Forbidden("no"), "forbidden"},
		{"conflict", errcode.Conflict("dup"), "conflict"},
		{"unauthenticated", errcode.Unauthenticated("who"), "unauthenticated"},
		{"too many requests", errcode.TooManyRequests("slow"), "too_many_requests"},
		{"unavailable", errcode.Unavailable("down"), "unavailable"},
		{"internal", errcode.Internal("boom"), "internal"},
		{"wrapped errcode resolves via errors.As", fmt.Errorf("ctx: %w", errcode.NotFound("nope")), "not_found"},
		{"plain error collapses to internal", errors.New("raw"), "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StatusLabel(tt.err))
		})
	}
}

func TestNormalizeStatus(t *testing.T) {
	tests := []struct {
		name string
		code string
		want string
	}{
		{"ok passes through", "ok", "ok"},
		{"not_found passes through", "not_found", "not_found"},
		{"internal passes through", "internal", "internal"},
		{"non-canonical collapses to internal", "weird_code", "internal"},
		{"empty collapses to internal", "", "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NormalizeStatus(tt.code))
		})
	}
}

func TestObserve(t *testing.T) {
	Observe("svc-test", "chat.route.{id}.get", "ok", 12*time.Millisecond)

	// Counter incremented for the exact label tuple (via the exported seam).
	assert.Equal(t, float64(1), CounterValue("svc-test", "chat.route.{id}.get", "ok"))

	// The duration histogram registered a series for this service/route pair.
	count := testutil.CollectAndCount(requestDuration, "rpc_server_request_duration_seconds")
	assert.GreaterOrEqual(t, count, 1)
}
