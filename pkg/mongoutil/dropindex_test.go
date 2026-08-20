package mongoutil

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// TestIsIndexAbsentError pins the only two failures a legacy-index retirement
// may swallow. Everything else — an unreachable MongoDB, a missing privilege —
// has to reach the caller, or startup reports the replacement index is in place
// while the legacy one is still enforcing its constraint.
func TestIsIndexAbsentError(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		absent bool
	}{
		{"nothing to drop: collection was never created", mongo.CommandError{Code: 26, Name: "NamespaceNotFound"}, true},
		{"nothing to drop: index already retired", mongo.CommandError{Code: 27, Name: "IndexNotFound"}, true},
		{"a wrapped absent index is still absent", fmt.Errorf("drop: %w", mongo.CommandError{Code: 27, Name: "IndexNotFound"}), true},
		{"unauthorized must not pass as absent", mongo.CommandError{Code: 13, Name: "Unauthorized"}, false},
		{"interrupted must not pass as absent", mongo.CommandError{Code: 11601, Name: "Interrupted"}, false},
		{"an unreachable MongoDB must not pass as absent", errors.New("server selection error: context deadline exceeded"), false},
		{"no error", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.absent, isIndexAbsentError(tc.err))
		})
	}
}
