package errcode

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not transient", nil, false},
		{"unavailable is transient", Unavailable("history down"), true},
		{"internal is transient", Internal("cassandra read failed"), true},
		{"not found is terminal", NotFound("message not found"), false},
		{"forbidden is terminal", Forbidden("no access"), false},
		{"bad request is terminal", BadRequest("bad id"), false},
		{"conflict is terminal", Conflict("already exists"), false},
		{"plain error is transient", errors.New("unmarshal failed"), true},
		{"wrapped unavailable is transient", fmt.Errorf("fetch: %w", Unavailable("down")), true},
		{"wrapped not found is terminal", fmt.Errorf("fetch: %w", NotFound("gone")), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsTransient(tt.err))
		})
	}
}
