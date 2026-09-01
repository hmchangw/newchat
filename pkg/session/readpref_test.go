package session

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// Sessions must never resolve from a lagging secondary: a revoked session that
// has not replicated would still authenticate (CWE-613). Services now default
// their Mongo clients to primaryPreferred, so this pin is what keeps the auth
// lookup authoritative — it is deliberately not configurable, and must not
// become so.
func TestSessionReadPreference_IsPrimaryAndNotConfigurable(t *testing.T) {
	require.Equal(t, readpref.PrimaryMode, sessionReadPreference().Mode(),
		"session lookups must not follow a service's client read preference")
}
