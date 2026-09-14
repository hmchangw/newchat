package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/session"
)

// A "healthy absence" must never open the breaker. The session case is the one
// that bites: bot tokens are attacker-supplied, so counting an unrecognised one
// as a failure would let a run of bad tokens fence a perfectly healthy Mongo —
// and take every legitimate bot down with it.
func TestMongoBreakerFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "no error", err: nil, want: false},
		{name: "unknown session", err: session.ErrNotFound, want: false},
		{name: "wrapped unknown session", err: fmt.Errorf("find session: %w", session.ErrNotFound), want: false},
		{name: "unknown account", err: mongo.ErrNoDocuments, want: false},
		{name: "wrapped unknown account", err: fmt.Errorf("find user: %w", mongo.ErrNoDocuments), want: false},
		{name: "bot not in room", err: model.ErrSubscriptionNotFound, want: false},
		{name: "mongo unreachable", err: errors.New("server selection error: context deadline exceeded"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, mongoBreakerFailure(tc.err))
		})
	}
}
