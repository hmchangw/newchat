package mongoutil

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
)

func TestBuildClientOptions(t *testing.T) {
	const uri = "mongodb://localhost:27017"

	tests := []struct {
		name         string
		username     string
		password     string
		expectAuth   bool
		expectedUser string
		expectedPass string
	}{
		{
			name:       "no credentials connects without auth",
			username:   "",
			password:   "",
			expectAuth: false,
		},
		{
			name:       "empty username with password skips auth",
			username:   "",
			password:   "secret",
			expectAuth: false,
		},
		{
			name:       "username with empty password skips auth",
			username:   "user",
			password:   "",
			expectAuth: false,
		},
		{
			name:         "both credentials set populates Auth",
			username:     "user",
			password:     "secret",
			expectAuth:   true,
			expectedUser: "user",
			expectedPass: "secret",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := buildClientOptions(uri, tc.username, tc.password)
			require.NotNil(t, opts)

			if !tc.expectAuth {
				assert.Nil(t, opts.Auth)
				return
			}

			require.NotNil(t, opts.Auth)
			assert.Equal(t, tc.expectedUser, opts.Auth.Username)
			assert.Equal(t, tc.expectedPass, opts.Auth.Password)
		})
	}
}

func TestBuildReadClientOptions_SecondaryPreferred(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "user", "pass")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	require.NotNil(t, opts.Auth)
	assert.Equal(t, "user", opts.Auth.Username)
}

func TestBuildReadClientOptions_NoAuthWhenEmpty(t *testing.T) {
	opts := buildReadClientOptions("mongodb://localhost:27017", "", "")
	require.NotNil(t, opts.ReadPreference)
	assert.Equal(t, readpref.SecondaryPreferredMode, opts.ReadPreference.Mode())
	assert.Nil(t, opts.Auth)
}

func TestSanitizeURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{"credentials stripped", "mongodb://user:secret@host:27017/db", "mongodb://host:27017/db"},
		{"username-only stripped", "mongodb://user@host:27017", "mongodb://host:27017"},
		{"no credentials unchanged", "mongodb://host:27017", "mongodb://host:27017"},
		{"srv scheme", "mongodb+srv://user:secret@cluster.example.net/db", "mongodb+srv://cluster.example.net/db"},
		{"query options stripped", "mongodb://host:27017/db?authMechanismProperties=AWS_SESSION_TOKEN:tok&proxyPassword=hunter2", "mongodb://host:27017/db"},
		{"fragment stripped", "mongodb://host:27017/db#frag", "mongodb://host:27017/db"},
		{"unparseable", "mongodb://user:sec ret@%zz", "invalid-uri"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, sanitizeURI(tc.uri))
		})
	}
}

func TestWithDegradedStart(t *testing.T) {
	t.Run("unset by default", func(t *testing.T) {
		cfg := newConnectConfig()
		assert.False(t, cfg.degradedStart)
	})

	t.Run("sets the flag", func(t *testing.T) {
		cfg := newConnectConfig(WithDegradedStart())
		assert.True(t, cfg.degradedStart)
	})

	t.Run("composes with other options", func(t *testing.T) {
		cfg := newConnectConfig(
			WithDegradedStart(),
			WithReadPreference(readpref.Primary()),
			WithMaxPoolSize(7),
		)
		assert.True(t, cfg.degradedStart)
		require.NotNil(t, cfg.readPref)
		assert.Equal(t, readpref.PrimaryMode, cfg.readPref.Mode())
		require.NotNil(t, cfg.maxPoolSize)
		assert.EqualValues(t, 7, *cfg.maxPoolSize)
	})
}

func TestIsAuthError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"AuthenticationFailed (18)", mongo.CommandError{Code: 18, Name: "AuthenticationFailed"}, true},
		{"Unauthorized (13)", mongo.CommandError{Code: 13, Name: "Unauthorized"}, true},
		{"UserNotFound (11): wrong user or authSource", mongo.CommandError{Code: 11, Name: "UserNotFound"}, true},
		{"wrapped auth error", fmt.Errorf("ping: %w", mongo.CommandError{Code: 18}), true},
		{"Atlas bad auth (8000)", mongo.CommandError{Code: 8000, Name: "AtlasError", Message: "bad auth : authentication failed"}, true},
		{"MechanismUnavailable (334)", mongo.CommandError{Code: 334, Name: "MechanismUnavailable"}, true},
		{"BadValue (2) naming the auth mechanism", mongo.CommandError{Code: 2, Name: "BadValue", Message: "Unsupported mechanism 'SCRAM-SHA-256' on authentication database 'admin'"}, true},
		{"BadValue (2) unrelated to auth", mongo.CommandError{Code: 2, Name: "BadValue", Message: "unknown option"}, false},
		{"raw driver error from a pool clear (18)", fmt.Errorf("pool cleared: %w", driver.Error{Code: 18, Message: "Authentication failed."}), true},
		{"raw driver error, unrelated code", driver.Error{Code: 26}, false},
		{"MaxTimeMSExpired (50): overloaded server is an outage", mongo.CommandError{Code: 50, Name: "MaxTimeMSExpired"}, false},
		{"socket reset mid-ping (0, NetworkError) is an outage", mongo.CommandError{Code: 0, Labels: []string{"NetworkError"}}, false},
		{"plain error", errors.New("server selection error"), false},
		{"context deadline", context.DeadlineExceeded, false},
		// The warm-pool shape: a code-0 CommandError over driver.Error{0} joined with the pool's
		// rejection. A first-match search stops at code 0; the rejection is still in the tree.
		{"non-auth command error joined before the rejection", fmt.Errorf("%w (pool cleared: %w)",
			mongo.CommandError{Code: 0, Wrapped: driver.Error{Code: 0, Wrapped: errors.New("pool cleared")}},
			fmt.Errorf("handshake: %w", driver.Error{Code: 18})), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isAuthError(tt.err))
		})
	}
}

func TestCredentialProbe_RecordsRejectionFromPoolCleared(t *testing.T) {
	authClear := &event.PoolEvent{Type: event.ConnectionPoolCleared, Address: "h:1",
		Error: fmt.Errorf("connection() error: %w", driver.Error{Code: 18, Message: "Authentication failed."})}
	netClear := &event.PoolEvent{Type: event.ConnectionPoolCleared, Address: "h:1",
		Error: errors.New("connection() error: dial tcp: connection refused")}
	created := &event.PoolEvent{Type: event.ConnectionCreated, Address: "h:1"}

	t.Run("nothing recorded before any event", func(t *testing.T) {
		p := newCredentialProbe()
		assert.NoError(t, p.rejection())
	})

	t.Run("auth-caused clear is recorded and classified", func(t *testing.T) {
		p := newCredentialProbe()
		p.monitor().Event(authClear)
		require.Error(t, p.rejection())
		assert.True(t, isAuthError(p.rejection()))
	})

	t.Run("network-caused clear and other events are ignored", func(t *testing.T) {
		p := newCredentialProbe()
		p.monitor().Event(created)
		p.monitor().Event(netClear)
		assert.NoError(t, p.rejection())
	})

	t.Run("first rejection is kept", func(t *testing.T) {
		p := newCredentialProbe()
		p.monitor().Event(authClear)
		later := &event.PoolEvent{Type: event.ConnectionPoolCleared, Error: driver.Error{Code: 13}}
		p.monitor().Event(later)
		var de driver.Error
		require.ErrorAs(t, p.rejection(), &de)
		assert.EqualValues(t, 18, de.Code)
	})
}

func TestStartupPingBound(t *testing.T) {
	tests := []struct {
		name string
		sst  *time.Duration
		want time.Duration
	}{
		{"pooled 2s default keeps the 10s floor", ptr(2 * time.Second), 10 * time.Second},
		{"no pool: driver's 30s selection default plus margin", nil, 35 * time.Second},
		{"operator raised selection above the floor", ptr(15 * time.Second), 20 * time.Second},
		{"zero (driver default) is treated as 30s", ptr(time.Duration(0)), 35 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, startupPingBound(tt.sst))
		})
	}
}

// unreachableMongo: nothing listens there, and the short connect bound keeps these tests quick.
const unreachableMongo = "mongodb://127.0.0.1:1/?connectTimeoutMS=200"

// fastFailPool and warmPool keep the failing-connect tests quick with a short server-selection bound.
func fastFailPool() PoolConfig {
	return PoolConfig{MaxPoolSize: 1, ServerSelectionTimeout: 300 * time.Millisecond}
}

func warmPool() PoolConfig {
	return PoolConfig{MaxPoolSize: 10, MinPoolSize: 5, ServerSelectionTimeout: 300 * time.Millisecond}
}

// requireAuthRejected asserts an auth error and no client (connect never returns both).
func requireAuthRejected(t *testing.T, client *mongo.Client, err error, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	assert.Nil(t, client, msgAndArgs...)
	assert.True(t, isAuthError(err), "expected an auth rejection, got: %v", err)
}

// With a warm floor the pool's background connections race the ping: the first SCRAM failure clears
// the pool and the ping sees only that, never the auth error. Rejection must still be fatal.
func TestConnect_WarmPool_RejectedCredentialsFatalEvenWithDegradedStart(t *testing.T) {
	srv := startFakeMongod(t)
	for i := 0; i < 5; i++ {
		client, err := Connect(context.Background(), srv.uri(), "user", "wrong",
			WithPool(warmPool()), WithDegradedStart())
		requireAuthRejected(t, client, err, "attempt %d: started degraded on rejected credentials", i)
	}
}

func TestConnect_ColdPool_RejectedCredentialsFatalEvenWithDegradedStart(t *testing.T) {
	srv := startFakeMongod(t)
	client, err := Connect(context.Background(), srv.uri(), "user", "wrong",
		WithPool(fastFailPool()), WithDegradedStart())
	requireAuthRejected(t, client, err)
}

// A cancelled or expired caller ctx is the caller asking to stop; degraded start must not return a live client.
func TestConnect_CallerCancelled_FailsEvenWithDegradedStart(t *testing.T) {
	srv := startFakeMongod(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := Connect(ctx, srv.uri(), "", "",
		WithPool(fastFailPool()), WithDegradedStart())
	require.Error(t, err)
	assert.Nil(t, client)
	assert.ErrorIs(t, err, context.Canceled)
}

// A pod that started degraded because MongoDB was unreachable may hold bad credentials too; the
// first operation after recovery is refused, and without this the pod would sit Running with an
// unusable client while its consumer burned redeliveries. The rejection must end the process.
func TestConnect_DegradedStart_ShutsDownWhenCredentialsAreRejectedAfterRecovery(t *testing.T) {
	terminated := make(chan struct{}, 1)
	orig := terminateProcess
	terminateProcess = func() { terminated <- struct{}{} }
	t.Cleanup(func() { terminateProcess = orig })

	srv := startFakeMongodPaused(t)
	client, err := Connect(context.Background(), srv.uri()+"/?connectTimeoutMS=200", "user", "wrong",
		WithPool(fastFailPool()), WithDegradedStart())
	require.NoError(t, err, "an unresponsive server starts degraded")
	require.NotNil(t, client)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	srv.resume()
	require.Eventually(t, func() bool {
		_ = client.Ping(context.Background(), nil) // the first operation after recovery
		select {
		case <-terminated:
			return true
		default:
			return false
		}
	}, 10*time.Second, 200*time.Millisecond, "the post-start credential rejection must shut the process down")
}

// A degraded client that is disconnected before any rejection must not leave the watcher behind.
func TestConnect_DegradedStart_DisconnectStopsTheCredentialWatch(t *testing.T) {
	terminated := make(chan struct{}, 1)
	orig := terminateProcess
	terminateProcess = func() { terminated <- struct{}{} }
	t.Cleanup(func() { terminateProcess = orig })

	client, err := Connect(context.Background(), unreachableMongo, "", "",
		WithPool(fastFailPool()), WithDegradedStart())
	require.NoError(t, err)
	Disconnect(context.Background(), client)
	select {
	case <-terminated:
		t.Fatal("no rejection was observed; nothing may terminate")
	case <-time.After(200 * time.Millisecond):
	}
}
